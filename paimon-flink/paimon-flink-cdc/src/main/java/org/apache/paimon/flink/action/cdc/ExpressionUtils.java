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

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.utils.DateTimeUtils;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/** Utils for {@link Expression}. */
public class ExpressionUtils {

    public static final DateTimeFormatter DEFAULT_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static LocalDateTime toLocalDateTime(String input, @Nullable Integer precision) {
        if (precision == null) {
            return DateTimeUtils.toLocalDateTime(input, 9);
        } else {
            long numericValue = Long.parseLong(input);
            long milliseconds = 0;
            int nanosOfMillisecond = 0;
            switch (precision) {
                case 0:
                    milliseconds = numericValue * 1000L;
                    break;
                case 3:
                    milliseconds = numericValue;
                    break;
                case 6:
                    milliseconds = numericValue / 1000;
                    nanosOfMillisecond = (int) (numericValue % 1000 * 1000);
                    break;
                case 9:
                    milliseconds = numericValue / 1_000_000;
                    nanosOfMillisecond = (int) (numericValue % 1_000_000);
                    break;
                    // no error case because precision is validated
            }
            return Timestamp.fromEpochMillis(milliseconds, nanosOfMillisecond).toLocalDateTime();
        }
    }

    public static String[] getInputs(String[] fieldReferences, Map<String, String> rowData) {
        String[] inputs = new String[fieldReferences.length];
        for (int i = 0; i < fieldReferences.length; i++) {
            inputs[i] = rowData.get(fieldReferences[i]);
        }
        return inputs;
    }

    public static String fromUnixTime(String column, int precision) {
        long inputValue = Long.parseLong(column);
        long epochMillis;
        switch (precision) {
            case 0:
                epochMillis = inputValue * 1000;
                break;
            case 3:
                epochMillis = inputValue;
                break;
            case 6:
                epochMillis = inputValue / 1000;
                break;
            case 9:
                epochMillis = inputValue / 1_000_000;
                break;
            default:
                throw new RuntimeException("Only support precision 0,3,6,9 but get: " + precision);
        }

        LocalDateTime localDateTime = Timestamp.fromEpochMillis(epochMillis).toLocalDateTime();
        return localDateTime.format(ExpressionUtils.DEFAULT_FORMATTER);
    }
}
