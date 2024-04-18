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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.data.Envelope;

import java.util.Map;

/** Registry of available {@link CdcMetadataConverter}s. */
public class CdcMetadataConverters {

    /** Name of the database that contain the row. */
    static class DatabaseNameConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode node) {
            return node.get(AbstractSourceInfo.DATABASE_NAME_KEY).asText();
        }

        @Override
        public DataType dataType() {
            return DataTypes.STRING().notNull();
        }

        @Override
        public String columnName() {
            return "database_name";
        }
    }

    /** Name of the table that contain the row. */
    static class TableNameConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode node) {
            return node.get(AbstractSourceInfo.TABLE_NAME_KEY).asText();
        }

        @Override
        public DataType dataType() {
            return DataTypes.STRING().notNull();
        }

        @Override
        public String columnName() {
            return "table_name";
        }
    }

    /** Name of the schema that contain the row. */
    static class SchemaNameConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode node) {
            return node.get(AbstractSourceInfo.SCHEMA_NAME_KEY).asText();
        }

        @Override
        public DataType dataType() {
            return DataTypes.STRING().notNull();
        }

        @Override
        public String columnName() {
            return "schema_name";
        }
    }

    /**
     * It indicates the time that the change was made in the database. If the record is read from
     * snapshot of the table instead of the binlog, the value is always 0.
     */
    static class OpTsConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode node) {
            return toLocalZonedTimestamp(node.get(AbstractSourceInfo.TIMESTAMP_KEY).asLong());
        }

        @Override
        public DataType dataType() {
            return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull();
        }

        @Override
        public String columnName() {
            return "op_ts";
        }
    }

    /** The kind of operation on a record. */
    static class OperationConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode root) {
            return payload(root).get(Envelope.FieldName.OPERATION).asText();
        }

        @Override
        public DataType dataType() {
            return DataTypes.STRING().notNull();
        }

        @Override
        public String columnName() {
            return "op";
        }
    }

    /**
     * JSON string describing the schema of the payload. Null if the schema is not included in the
     * Debezium record.
     */
    static class SchemaConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode schema) {
            return JsonSerdeUtil.isNull(schema) ? null : JsonSerdeUtil.toFlatJson(schema);
        }

        @Override
        public DataType dataType() {
            return DataTypes.STRING();
        }

        @Override
        public String columnName() {
            return "schema";
        }
    }

    static class IngestionTimestampConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode root) {
            JsonNode ts = payload(root).get(Envelope.FieldName.TIMESTAMP);
            if (JsonSerdeUtil.isNull(ts)) {
                return null;
            }
            return toLocalZonedTimestamp(ts.asLong());
        }

        @Override
        public DataType dataType() {
            return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3);
        }

        @Override
        public String columnName() {
            return "ingestion-timestamp";
        }
    }

    static class SourceTimestampConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode root) {
            JsonNode ts = source(root).get(AbstractSourceInfo.TIMESTAMP_KEY);
            if (JsonSerdeUtil.isNull(ts)) {
                return null;
            }
            return toLocalZonedTimestamp(ts.asLong());
        }

        @Override
        public DataType dataType() {
            return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3);
        }

        @Override
        public String columnName() {
            return "source.timestamp";
        }
    }

    static class SourceDatabaseConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode root) {
            return source(root).get(AbstractSourceInfo.DATABASE_NAME_KEY).asText();
        }

        @Override
        public DataType dataType() {
            return DataTypes.STRING();
        }

        @Override
        public String columnName() {
            return "source.database";
        }
    }

    static class SourceSchemaConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode root) {
            JsonNode schema = source(root).get(AbstractSourceInfo.SCHEMA_NAME_KEY);
            return JsonSerdeUtil.isNull(schema) ? null : schema.asText();
        }

        @Override
        public DataType dataType() {
            return DataTypes.STRING();
        }

        @Override
        public String columnName() {
            return "source.schema";
        }
    }

    static class SourceTableConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode root) {
            JsonNode table = source(root).get(AbstractSourceInfo.TABLE_NAME_KEY);
            JsonNode collection = source(root).get(AbstractSourceInfo.COLLECTION_NAME_KEY);
            if (JsonSerdeUtil.isNull(table) && JsonSerdeUtil.isNull(collection)) {
                return null;
            }
            return JsonSerdeUtil.isNull(table) ? collection.asText() : table.asText();
        }

        @Override
        public DataType dataType() {
            return DataTypes.STRING();
        }

        @Override
        public String columnName() {
            return "source.table";
        }
    }

    // TODO change data type to MAP<STRING, STRING>
    static class SourcePropertiesConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode root) {
            Map<String, String> sourceProperties =
                    JsonSerdeUtil.convertValue(
                            source(root), new TypeReference<Map<String, String>>() {});

            return sourceProperties.toString();
        }

        @Override
        public DataType dataType() {
            return DataTypes.STRING();
        }

        @Override
        public String columnName() {
            return "source.properties";
        }
    }

    private static JsonNode payload(JsonNode root) {
        return root.get(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME);
    }

    private static JsonNode source(JsonNode root) {
        return payload(root).get(CloudEventsMaker.FieldName.SOURCE);
    }

    private static String toLocalZonedTimestamp(long timestampMillis) {
        return DateTimeUtils.formatTimestamp(
                Timestamp.fromEpochMillis(timestampMillis), DateTimeUtils.LOCAL_TZ, 3);
    }
}
