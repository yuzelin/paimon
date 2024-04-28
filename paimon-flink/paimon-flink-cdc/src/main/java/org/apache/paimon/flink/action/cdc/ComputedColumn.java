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

import javax.annotation.Nullable;

import java.io.Serializable;

/** A Computed column's value is computed from input columns. */
public class ComputedColumn implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String columnName;
    private final Expression expression;

    public ComputedColumn(String columnName, Expression expression) {
        this.columnName = columnName;
        this.expression = expression;
    }

    public String columnName() {
        return columnName;
    }

    public DataType columnType() {
        return expression.outputType();
    }

    public String[] fieldReferences() {
        return expression.fieldReferences();
    }

    /** Compute column's value from given argument. Return null if input is null. */
    @Nullable
    public String eval(String... inputs) {
        return expression.eval(inputs);
    }
}
