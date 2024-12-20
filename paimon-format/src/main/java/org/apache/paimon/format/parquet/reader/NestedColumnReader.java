/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.heap.AbstractHeapVector;
import org.apache.paimon.data.columnar.heap.ElementCountable;
import org.apache.paimon.data.columnar.heap.HeapArrayVector;
import org.apache.paimon.data.columnar.heap.HeapMapVector;
import org.apache.paimon.data.columnar.heap.HeapRowVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.format.parquet.position.CollectionPosition;
import org.apache.paimon.format.parquet.position.LevelDelegation;
import org.apache.paimon.format.parquet.type.ParquetField;
import org.apache.paimon.format.parquet.type.ParquetGroupField;
import org.apache.paimon.format.parquet.type.ParquetPrimitiveField;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.Triple;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * This ColumnReader mainly used to read `Group` type in parquet such as `Map`, `Array`, `Row`. The
 * method about how to resolve nested struct mainly refer to : <a
 * href="https://github.com/julienledem/redelm/wiki/The-striping-and-assembly-algorithms-from-the-Dremel-paper">The
 * striping and assembly algorithms from the Dremel paper</a>.
 *
 * <p>Brief explanation of reading repetition and definition levels: Repetition level equal to 0
 * means that this is the beginning of a new row. Other value means that we should add data to the
 * current row.
 *
 * <p>For example, if we have the following data: repetition levels: 0,1,1,0,0,1,[0] (last 0 is
 * implicit, normally will be the end of the page) values: a,b,c,d,e,f will consist of the sets of:
 * (a, b, c), (d), (e, f). <br>
 *
 * <p>Definition levels contains 3 situations: level = maxDefLevel means value exist and is not null
 * level = maxDefLevel - 1 means value is null level < maxDefLevel - 1 means value doesn't exist For
 * non-nullable (REQUIRED) fields the (level = maxDefLevel - 1) condition means non-existing value
 * as well. <br>
 *
 * <p>Quick example (maxDefLevel is 2): Read 3 rows out of: repetition levels: 0,1,0,1,1,0,0,...
 * definition levels: 2,1,0,2,1,2,... values: a,b,c,d,e,f,... Resulting buffer: a,n, ,d,n,f that
 * result is (a,n),(d,n),(f) where n means null
 */
public class NestedColumnReader implements ColumnReader<WritableColumnVector> {

    private final Map<ColumnDescriptor, NestedPrimitiveColumnReader> columnReaders;
    private final boolean isUtcTimestamp;

    private final PageReadStore pages;

    private final ParquetField field;

    public NestedColumnReader(boolean isUtcTimestamp, PageReadStore pages, ParquetField field) {
        this.isUtcTimestamp = isUtcTimestamp;
        this.pages = pages;
        this.field = field;
        this.columnReaders = new HashMap<>();
    }

    @Override
    public void readToVector(int readNumber, WritableColumnVector vector) throws IOException {
        readData(field, readNumber, vector, false, false, false);
    }

    private Triple<LevelDelegation, WritableColumnVector, boolean[]> readData(
            ParquetField field,
            int readNumber,
            ColumnVector vector,
            boolean inside,
            boolean readRowField,
            boolean readMapKey)
            throws IOException {
        if (field.getType() instanceof RowType) {
            return readRow((ParquetGroupField) field, readNumber, vector, inside);
        } else if (field.getType() instanceof MapType || field.getType() instanceof MultisetType) {
            return readMap((ParquetGroupField) field, readNumber, vector, inside, readRowField);
        } else if (field.getType() instanceof ArrayType) {
            return readArray((ParquetGroupField) field, readNumber, vector, inside, readRowField);
        } else {
            return readPrimitive(
                    (ParquetPrimitiveField) field, readNumber, vector, readRowField, readMapKey);
        }
    }

    private Triple<LevelDelegation, WritableColumnVector, boolean[]> readRow(
            ParquetGroupField field, int readNumber, ColumnVector vector, boolean inside)
            throws IOException {
        HeapRowVector heapRowVector = (HeapRowVector) vector;
        LevelDelegation longest = null;
        List<ParquetField> children = field.getChildren();
        WritableColumnVector[] childrenVectors = heapRowVector.getFields();
        WritableColumnVector[] finalChildrenVectors =
                new WritableColumnVector[childrenVectors.length];

        // Length of this row. it should be equal to each child's length
        int len = -1;
        // if all children is null at i, then isNull[i] == true
        boolean[] isNull = null;
        boolean hasNull = false;

        for (int i = 0; i < children.size(); i++) {
            Triple<LevelDelegation, WritableColumnVector, boolean[]> triple =
                    readData(children.get(i), readNumber, childrenVectors[i], true, true, false);
            LevelDelegation current = triple.f0;
            if (longest == null) {
                longest = current;
            } else if (current.getDefinitionLevel().length > longest.getDefinitionLevel().length) {
                longest = current;
            }

            if (longest == null) {
                throw new RuntimeException(
                        String.format("Row field does not have any children: %s.", field));
            }

            WritableColumnVector child = triple.f1;
            finalChildrenVectors[i] = child;

            if (len == -1) {
                // init len, isNull
                len = ((ElementCountable) child).getLen();
                isNull = triple.f2;
            } else {
                checkState(
                        len == ((ElementCountable) child).getLen(),
                        "Row %s field %s has length %s, but expected %s.",
                        field,
                        i,
                        ((ElementCountable) child).getLen(),
                        len);

                for (int b = 0; b < len; b++) {
                    isNull[b] = isNull[b] && triple.f2[b];
                }
            }
        }
        if (longest == null) {
            throw new RuntimeException(
                    String.format("Row field does not have any children: %s.", field));
        }

        for (int i = 0; i < len; i++) {
            if (isNull[i]) {
                hasNull = true;
                break;
            }
        }

        // If row was inside the structure, then we need to renew the vector to reset the
        // capacity.
        if (inside) {
            heapRowVector = new HeapRowVector(len, finalChildrenVectors);
        } else {
            heapRowVector.setFields(finalChildrenVectors);
        }

        if (hasNull) {
            setFieldNullFlag(isNull, heapRowVector);
        }
        return Triple.of(longest, heapRowVector, heapRowVector.isNull());
    }

    private Triple<LevelDelegation, WritableColumnVector, boolean[]> readMap(
            ParquetGroupField field,
            int readNumber,
            ColumnVector vector,
            boolean inside,
            boolean readRowField)
            throws IOException {
        HeapMapVector mapVector = (HeapMapVector) vector;
        mapVector.reset();
        List<ParquetField> children = field.getChildren();
        Preconditions.checkArgument(
                children.size() == 2,
                "Maps must have two type parameters, found %s",
                children.size());
        Triple<LevelDelegation, WritableColumnVector, boolean[]> keyTriple =
                readData(
                        children.get(0),
                        readNumber,
                        mapVector.getKeyColumnVector(),
                        true,
                        false,
                        true);
        Triple<LevelDelegation, WritableColumnVector, boolean[]> valueTriple =
                readData(
                        children.get(1),
                        readNumber,
                        mapVector.getValueColumnVector(),
                        true,
                        false,
                        false);

        LevelDelegation levelDelegation = keyTriple.f0;

        CollectionPosition collectionPosition =
                NestedPositionUtil.calculateCollectionOffsets(
                        field,
                        levelDelegation.getDefinitionLevel(),
                        levelDelegation.getRepetitionLevel(),
                        readRowField);

        // If map was inside the structure, then we need to renew the vector to reset the
        // capacity.
        if (inside) {
            mapVector =
                    new HeapMapVector(
                            collectionPosition.getValueCount(), keyTriple.f1, valueTriple.f1);
        } else {
            mapVector.setKeys(keyTriple.f1);
            mapVector.setValues(valueTriple.f1);
        }

        if (collectionPosition.getIsNull() != null) {
            setFieldNullFlag(collectionPosition.getIsNull(), mapVector);
        }

        mapVector.setLengths(collectionPosition.getLength());
        mapVector.setOffsets(collectionPosition.getOffsets());

        return Triple.of(levelDelegation, mapVector, mapVector.isNull());
    }

    private Triple<LevelDelegation, WritableColumnVector, boolean[]> readArray(
            ParquetGroupField field,
            int readNumber,
            ColumnVector vector,
            boolean inside,
            boolean readRowField)
            throws IOException {
        HeapArrayVector arrayVector = (HeapArrayVector) vector;
        arrayVector.reset();
        List<ParquetField> children = field.getChildren();
        Preconditions.checkArgument(
                children.size() == 1,
                "Arrays must have a single type parameter, found %s",
                children.size());
        Triple<LevelDelegation, WritableColumnVector, boolean[]> tuple =
                readData(children.get(0), readNumber, arrayVector.getChild(), true, false, false);

        LevelDelegation levelDelegation = tuple.f0;
        CollectionPosition collectionPosition =
                NestedPositionUtil.calculateCollectionOffsets(
                        field,
                        levelDelegation.getDefinitionLevel(),
                        levelDelegation.getRepetitionLevel(),
                        readRowField);

        // If array was inside the structure, then we need to renew the vector to reset the
        // capacity.
        if (inside) {
            arrayVector = new HeapArrayVector(collectionPosition.getValueCount(), tuple.f1);
        } else {
            arrayVector.setChild(tuple.f1);
        }

        if (collectionPosition.getIsNull() != null) {
            setFieldNullFlag(collectionPosition.getIsNull(), arrayVector);
        }
        arrayVector.setLengths(collectionPosition.getLength());
        arrayVector.setOffsets(collectionPosition.getOffsets());
        return Triple.of(levelDelegation, arrayVector, arrayVector.isNull());
    }

    private Triple<LevelDelegation, WritableColumnVector, boolean[]> readPrimitive(
            ParquetPrimitiveField field,
            int readNumber,
            ColumnVector vector,
            boolean readRowField,
            boolean readMapKey)
            throws IOException {
        ColumnDescriptor descriptor = field.getDescriptor();
        NestedPrimitiveColumnReader reader = columnReaders.get(descriptor);
        if (reader == null) {
            reader =
                    new NestedPrimitiveColumnReader(
                            descriptor,
                            pages,
                            isUtcTimestamp,
                            descriptor.getPrimitiveType(),
                            field.getType(),
                            readRowField,
                            readMapKey);
            columnReaders.put(descriptor, reader);
        }
        WritableColumnVector writableColumnVector =
                reader.readAndNewVector(readNumber, (WritableColumnVector) vector);
        int len = ((ElementCountable) writableColumnVector).getLen();
        boolean[] isNull = new boolean[len];
        System.arraycopy(reader.outerRowIsNull(), 0, isNull, 0, len);
        return Triple.of(reader.getLevelDelegation(), writableColumnVector, isNull);
    }

    private static void setFieldNullFlag(boolean[] nullFlags, AbstractHeapVector vector) {
        for (int index = 0; index < vector.getLen() && index < nullFlags.length; index++) {
            if (nullFlags[index]) {
                vector.setNullAt(index);
            }
        }
    }
}
