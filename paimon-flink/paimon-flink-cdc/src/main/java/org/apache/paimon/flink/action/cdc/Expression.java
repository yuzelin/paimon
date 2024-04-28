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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeJsonParser;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SerializableSupplier;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Produce a computation result for computed column. */
public interface Expression extends Serializable {

    /** Return name of referenced field. */
    String[] fieldReferences();

    /** Return {@link DataType} of computed value. */
    DataType outputType();

    /** Compute value from given input. Input and output are serialized to string. */
    @Nullable
    String eval(String... input);

    /** Expression function. */
    enum ExpressionFunction {
        YEAR(
                (typeMapping, caseSensitive, args) -> {
                    ReferencedField referencedField =
                            ReferencedField.checkArgument(typeMapping, caseSensitive, args);
                    return TemporalToIntConverter.create(
                            referencedField.field(),
                            referencedField.fieldType(),
                            () -> LocalDateTime::getYear,
                            referencedField.literals());
                }),
        MONTH(
                (typeMapping, caseSensitive, args) -> {
                    ReferencedField referencedField =
                            ReferencedField.checkArgument(typeMapping, caseSensitive, args);
                    return TemporalToIntConverter.create(
                            referencedField.field(),
                            referencedField.fieldType(),
                            () -> LocalDateTime::getMonthValue,
                            referencedField.literals());
                }),
        DAY(
                (typeMapping, caseSensitive, args) -> {
                    ReferencedField referencedField =
                            ReferencedField.checkArgument(typeMapping, caseSensitive, args);
                    return TemporalToIntConverter.create(
                            referencedField.field(),
                            referencedField.fieldType(),
                            () -> LocalDateTime::getDayOfMonth,
                            referencedField.literals());
                }),
        HOUR(
                (typeMapping, caseSensitive, args) -> {
                    ReferencedField referencedField =
                            ReferencedField.checkArgument(typeMapping, caseSensitive, args);
                    return TemporalToIntConverter.create(
                            referencedField.field(),
                            referencedField.fieldType(),
                            () -> LocalDateTime::getHour,
                            referencedField.literals());
                }),
        MINUTE(
                (typeMapping, caseSensitive, args) -> {
                    ReferencedField referencedField =
                            ReferencedField.checkArgument(typeMapping, caseSensitive, args);
                    return TemporalToIntConverter.create(
                            referencedField.field(),
                            referencedField.fieldType(),
                            () -> LocalDateTime::getMinute,
                            referencedField.literals());
                }),
        SECOND(
                (typeMapping, caseSensitive, args) -> {
                    ReferencedField referencedField =
                            ReferencedField.checkArgument(typeMapping, caseSensitive, args);
                    return TemporalToIntConverter.create(
                            referencedField.field(),
                            referencedField.fieldType(),
                            () -> LocalDateTime::getSecond,
                            referencedField.literals());
                }),
        DATE_FORMAT(
                (typeMapping, caseSensitive, args) -> {
                    ReferencedField referencedField =
                            ReferencedField.checkArgument(typeMapping, caseSensitive, args);
                    return DateFormat.create(
                            referencedField.field(),
                            referencedField.fieldType(),
                            referencedField.literals());
                }),
        SUBSTRING(
                (typeMapping, caseSensitive, args) -> {
                    ReferencedField referencedField =
                            ReferencedField.checkArgument(typeMapping, caseSensitive, args);
                    return substring(referencedField.field(), referencedField.literals());
                }),
        TRUNCATE(
                (typeMapping, caseSensitive, args) -> {
                    ReferencedField referencedField =
                            ReferencedField.checkArgument(typeMapping, caseSensitive, args);
                    return truncate(
                            referencedField.field(),
                            referencedField.fieldType(),
                            referencedField.literals());
                }),
        CAST((typeMapping, caseSensitive, args) -> cast(args)),

        /** For xiaopeng. */
        GEN_PARTITION_STMT_DT(
                (typeMapping, caseSensitive, args) -> {
                    checkArgument(
                            args.length == 4,
                            "Arguments for gen_partition_stmt_dt: column_a, column_b, "
                                    + "column_b_precision, excluded_partition.");
                    return new GenPartitionStmtDt(
                            args[0].trim(), args[1].trim(), args[2].trim(), args[3].trim());
                }),

        GEN_PARTITION_STMT_INT(
                (typeMapping, caseSensitive, args) -> {
                    checkArgument(
                            args.length == 5,
                            "Arguments for gen_partition_stmt_int: column_a, column_a_precision, column_b, "
                                    + "column_b_precision, excluded_partition.");
                    return new GenPartitionStmtInt(
                            args[0].trim(),
                            args[1].trim(),
                            args[2].trim(),
                            args[3].trim(),
                            args[4].trim());
                });

        public final ExpressionCreator creator;

        ExpressionFunction(ExpressionCreator creator) {
            this.creator = creator;
        }

        public ExpressionCreator getCreator() {
            return creator;
        }

        private static final Map<String, ExpressionCreator> EXPRESSION_FUNCTIONS =
                Arrays.stream(ExpressionFunction.values())
                        .collect(
                                Collectors.toMap(
                                        value -> value.name().toLowerCase(),
                                        ExpressionFunction::getCreator));

        public static ExpressionCreator creator(String exprName) {
            return EXPRESSION_FUNCTIONS.get(exprName.toLowerCase());
        }
    }

    /** Expression creator. */
    @FunctionalInterface
    interface ExpressionCreator {
        Expression create(Map<String, DataType> typeMapping, boolean caseSensitive, String[] args);
    }

    /** Referenced field in expression input parameters. */
    class ReferencedField {
        private final String field;
        private final DataType fieldType;
        private final String[] literals;

        private ReferencedField(String field, DataType fieldType, String[] literals) {
            this.field = field;
            this.fieldType = fieldType;
            this.literals = literals;
        }

        public static ReferencedField checkArgument(
                Map<String, DataType> typeMapping, boolean caseSensitive, String... args) {
            String referencedField = args[0].trim();
            String[] literals =
                    Arrays.stream(args).skip(1).map(String::trim).toArray(String[]::new);
            String referencedFieldCheckForm =
                    StringUtils.caseSensitiveConversion(referencedField, caseSensitive);

            DataType fieldType =
                    checkNotNull(
                            typeMapping.get(referencedFieldCheckForm),
                            String.format(
                                    "Referenced field '%s' is not in given fields: %s.",
                                    referencedFieldCheckForm, typeMapping.keySet()));
            return new ReferencedField(referencedField, fieldType, literals);
        }

        public String field() {
            return field;
        }

        public DataType fieldType() {
            return fieldType;
        }

        public String[] literals() {
            return literals;
        }
    }

    static Expression create(
            Map<String, DataType> typeMapping,
            boolean caseSensitive,
            String exprName,
            String... args) {

        ExpressionCreator function = ExpressionFunction.creator(exprName.toLowerCase());
        if (function == null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported expression: %s. Supported expressions are: %s",
                            exprName,
                            String.join(",", ExpressionFunction.EXPRESSION_FUNCTIONS.keySet())));
        }
        return function.create(typeMapping, caseSensitive, args);
    }

    static Expression substring(String fieldReference, String... literals) {
        checkArgument(
                literals.length == 1 || literals.length == 2,
                String.format(
                        "'substring' expression supports one or two arguments, but found '%s'.",
                        literals.length));
        int beginInclusive;
        Integer endExclusive;
        try {
            beginInclusive = Integer.parseInt(literals[0]);
            endExclusive = literals.length == 1 ? null : Integer.parseInt(literals[1]);
        } catch (NumberFormatException e) {
            throw new RuntimeException(
                    String.format(
                            "The index arguments '%s' contain non integer value.",
                            Arrays.toString(literals)),
                    e);
        }
        checkArgument(
                beginInclusive >= 0,
                "begin index argument (%s) of 'substring' must be >= 0.",
                beginInclusive);
        checkArgument(
                endExclusive == null || endExclusive > beginInclusive,
                "end index (%s) must be larger than begin index (%s).",
                endExclusive,
                beginInclusive);
        return new Substring(fieldReference, beginInclusive, endExclusive);
    }

    static Expression truncate(String fieldReference, DataType fieldType, String... literals) {
        checkArgument(
                literals.length == 1,
                String.format(
                        "'truncate' expression supports one argument, but found '%s'.",
                        literals.length));
        return new TruncateComputer(fieldReference, fieldType, literals[0]);
    }

    static Expression cast(String... literals) {
        checkArgument(
                literals.length == 1 || literals.length == 2,
                String.format(
                        "'cast' expression supports one or two arguments, but found '%s'.",
                        literals.length));
        DataType dataType = DataTypes.STRING();
        if (literals.length == 2) {
            dataType = DataTypeJsonParser.parseAtomicTypeSQLString(literals[1]);
        }
        return new CastExpression(literals[0], dataType);
    }

    // ======================== Expression Implementations ========================

    /** Expression to handle temporal value. */
    abstract class TemporalExpressionBase<T> implements Expression {

        private static final long serialVersionUID = 1L;

        private static final List<Integer> SUPPORTED_PRECISION = Arrays.asList(0, 3, 6, 9);

        private final String[] fieldReferences;
        @Nullable private final Integer precision;

        private transient Function<LocalDateTime, T> converter;

        private TemporalExpressionBase(
                String fieldReference, DataType fieldType, @Nullable Integer precision) {
            this.fieldReferences = new String[] {fieldReference};

            // when the input is INTEGER_NUMERIC, the precision must be set
            if (fieldType.getTypeRoot().getFamilies().contains(DataTypeFamily.INTEGER_NUMERIC)
                    && precision == null) {
                precision = 0;
            }

            checkArgument(
                    precision == null || SUPPORTED_PRECISION.contains(precision),
                    "Unsupported precision of temporal function: %d. Supported precisions are: "
                            + "0 (for epoch seconds), 3 (for epoch milliseconds), 6 (for epoch microseconds) and 9 (for epoch nanoseconds).",
                    precision);

            this.precision = precision;
        }

        @Override
        public String[] fieldReferences() {
            return fieldReferences;
        }

        /** If not, this must be overridden! */
        @Override
        public DataType outputType() {
            return DataTypes.INT();
        }

        @Override
        @Nullable
        public String eval(String... inputs) {
            if (converter == null) {
                this.converter = createConverter();
            }

            String input = inputs[0];
            if (input == null) {
                return null;
            }

            T result = converter.apply(ExpressionUtils.toLocalDateTime(input, precision));
            return String.valueOf(result);
        }

        protected abstract Function<LocalDateTime, T> createConverter();
    }

    /** Convert the temporal value to an integer. */
    final class TemporalToIntConverter extends TemporalExpressionBase<Integer> {

        private static final long serialVersionUID = 1L;

        private final SerializableSupplier<Function<LocalDateTime, Integer>> converterSupplier;

        private TemporalToIntConverter(
                String fieldReference,
                DataType fieldType,
                @Nullable Integer precision,
                SerializableSupplier<Function<LocalDateTime, Integer>> converterSupplier) {
            super(fieldReference, fieldType, precision);
            this.converterSupplier = converterSupplier;
        }

        @Override
        protected Function<LocalDateTime, Integer> createConverter() {
            return converterSupplier.get();
        }

        private static TemporalToIntConverter create(
                String fieldReference,
                DataType fieldType,
                SerializableSupplier<Function<LocalDateTime, Integer>> converterSupplier,
                String... literals) {
            checkArgument(
                    literals.length == 0 || literals.length == 1,
                    "TemporalToIntConverter supports 0 or 1 argument, but found '%s'.",
                    literals.length);

            return new TemporalToIntConverter(
                    fieldReference,
                    fieldType,
                    literals.length == 0 ? null : Integer.valueOf(literals[0]),
                    converterSupplier);
        }
    }

    /** Convert the temporal value to desired formatted string. */
    final class DateFormat extends TemporalExpressionBase<String> {

        private static final long serialVersionUID = 2L;

        private final String pattern;

        private DateFormat(
                String fieldReference,
                DataType fieldType,
                String pattern,
                @Nullable Integer precision) {
            super(fieldReference, fieldType, precision);
            this.pattern = pattern;
        }

        @Override
        public DataType outputType() {
            return DataTypes.STRING();
        }

        @Override
        protected Function<LocalDateTime, String> createConverter() {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
            return localDateTime -> localDateTime.format(formatter);
        }

        private static DateFormat create(
                String fieldReference, DataType fieldType, String... literals) {
            checkArgument(
                    literals.length == 1 || literals.length == 2,
                    "'date_format' supports 1 or 2 arguments, but found '%s'.",
                    literals.length);

            return new DateFormat(
                    fieldReference,
                    fieldType,
                    literals[0],
                    literals.length == 1 ? null : Integer.valueOf(literals[1]));
        }
    }

    /** Get substring using {@link String#substring}. */
    final class Substring implements Expression {

        private static final long serialVersionUID = 1L;

        private final String[] fieldReferences;
        private final int beginInclusive;
        @Nullable private final Integer endExclusive;

        private Substring(
                String fieldReference, int beginInclusive, @Nullable Integer endExclusive) {
            this.fieldReferences = new String[] {fieldReference};
            this.beginInclusive = beginInclusive;
            this.endExclusive = endExclusive;
        }

        @Override
        public String[] fieldReferences() {
            return fieldReferences;
        }

        @Override
        public DataType outputType() {
            return DataTypes.STRING();
        }

        @Override
        public String eval(String... inputs) {
            String input = inputs[0];
            if (input == null) {
                return null;
            }
            try {
                if (endExclusive == null) {
                    return input.substring(beginInclusive);
                } else {
                    return input.substring(beginInclusive, endExclusive);
                }
            } catch (StringIndexOutOfBoundsException e) {
                throw new RuntimeException(
                        String.format(
                                "Cannot get substring from '%s' because the indexes are out of range. Begin index: %s, end index: %s.",
                                input, beginInclusive, endExclusive));
            }
        }
    }

    /** Truncate numeric/decimal/string value. */
    final class TruncateComputer implements Expression {
        private static final long serialVersionUID = 1L;

        private final String[] fieldReferences;

        private final DataType fieldType;

        private final int width;

        TruncateComputer(String fieldReference, DataType fieldType, String literal) {
            this.fieldReferences = new String[] {fieldReference};
            this.fieldType = fieldType;
            try {
                this.width = Integer.parseInt(literal);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid width value for truncate function: %s, expected integer.",
                                literal));
            }
        }

        @Override
        public String[] fieldReferences() {
            return fieldReferences;
        }

        @Override
        public DataType outputType() {
            return fieldType;
        }

        @Override
        public String eval(String... inputs) {
            String input = inputs[0];
            if (input == null) {
                return null;
            }
            switch (fieldType.getTypeRoot()) {
                case TINYINT:
                case SMALLINT:
                    return String.valueOf(truncateShort(width, Short.parseShort(input)));
                case INTEGER:
                    return String.valueOf(truncateInt(width, Integer.parseInt(input)));
                case BIGINT:
                    return String.valueOf(truncateLong(width, Long.parseLong(input)));
                case DECIMAL:
                    return truncateDecimal(BigInteger.valueOf(width), new BigDecimal(input))
                            .toString();
                case VARCHAR:
                case CHAR:
                    checkArgument(
                            width <= input.length(),
                            "Invalid width value for truncate function: %s, expected less than or equal to %s.",
                            width,
                            input.length());
                    return input.substring(0, width);
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unsupported field type for truncate function: %s.",
                                    fieldType.getTypeRoot().toString()));
            }
        }

        private short truncateShort(int width, short value) {
            return (short) (value - (((value % width) + width) % width));
        }

        private int truncateInt(int width, int value) {
            return value - (((value % width) + width) % width);
        }

        private long truncateLong(int width, long value) {
            return value - (((value % width) + width) % width);
        }

        private BigDecimal truncateDecimal(BigInteger unscaledWidth, BigDecimal value) {
            BigDecimal remainder =
                    new BigDecimal(
                            value.unscaledValue()
                                    .remainder(unscaledWidth)
                                    .add(unscaledWidth)
                                    .remainder(unscaledWidth),
                            value.scale());

            return value.subtract(remainder);
        }
    }

    /** Get constant value. */
    final class CastExpression implements Expression {

        private static final long serialVersionUID = 1L;

        private static final String[] EMPTY_FIELD_REFERENCES = new String[0];

        private final String value;

        private final DataType dataType;

        private CastExpression(String value, DataType dataType) {
            this.value = value;
            this.dataType = dataType;
        }

        @Override
        public String[] fieldReferences() {
            return EMPTY_FIELD_REFERENCES;
        }

        @Override
        public DataType outputType() {
            return dataType;
        }

        @Override
        public String eval(String... inputs) {
            return value;
        }
    }

    /** For xiaopeng. */
    final class GenPartitionStmtDt implements Expression {

        private static final long serialVersionUID = 1L;

        private final String[] fieldReferences;
        private final int bPrecision;
        private final String excludedString;

        private GenPartitionStmtDt(
                String fieldRefA, String fieldRefB, String bPrecision, String excludedString) {
            this.fieldReferences = new String[] {fieldRefA, fieldRefB};

            checkArgument(!bPrecision.isEmpty(), "Precision of column b must be set.");
            this.bPrecision = Integer.parseInt(bPrecision);

            this.excludedString = excludedString;
        }

        @Override
        public String[] fieldReferences() {
            return fieldReferences;
        }

        @Override
        public DataType outputType() {
            return DataTypes.STRING();
        }

        @Override
        public String eval(String... inputs) {
            String columnA = inputs[0];
            String columnB = inputs[1];

            if (columnA != null) {
                LocalDateTime localDateTime = ExpressionUtils.toLocalDateTime(columnA, null);
                String formattedA = localDateTime.format(ExpressionUtils.DEFAULT_FORMATTER);
                if (!excludedString.equals(formattedA) && !"1970-01-01".equals(formattedA)) {
                    return formattedA;
                }
            }

            if (columnB == null || "0".equals(columnB)) {
                return null;
            }

            return ExpressionUtils.fromUnixTime(columnB, bPrecision);
        }
    }

    /** For xiaopeng. */
    final class GenPartitionStmtInt implements Expression {

        private static final long serialVersionUID = 1L;

        private final String[] fieldReferences;
        private final int aPrecision;
        private final int bPrecision;
        private final String excludedString;

        private GenPartitionStmtInt(
                String fieldRefA,
                String aPrecision,
                String fieldRefB,
                String bPrecision,
                String excludedString) {
            this.fieldReferences = new String[] {fieldRefA, fieldRefB};
            checkArgument(!aPrecision.isEmpty(), "Precision of column b must be set.");
            this.aPrecision = Integer.parseInt(aPrecision);

            checkArgument(!bPrecision.isEmpty(), "Precision of column b must be set.");
            this.bPrecision = Integer.parseInt(bPrecision);

            this.excludedString = excludedString;
        }

        @Override
        public String[] fieldReferences() {
            return fieldReferences;
        }

        @Override
        public DataType outputType() {
            return DataTypes.STRING();
        }

        @Override
        public String eval(String... inputs) {
            String columnA = inputs[0];
            String columnB = inputs[1];

            if (columnA != null && !"0".equals(columnA)) {
                LocalDateTime localDateTime = ExpressionUtils.toLocalDateTime(columnA, aPrecision);
                String formattedA = localDateTime.format(ExpressionUtils.DEFAULT_FORMATTER);
                if (!excludedString.equals(formattedA) && !"1970-01-01".equals(formattedA)) {
                    return formattedA;
                }
            }

            if (columnB == null || "0".equals(columnB)) {
                return null;
            }

            return ExpressionUtils.fromUnixTime(columnB, bPrecision);
        }
    }
}
