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

import org.apache.paimon.flink.action.cdc.format.DataFormat;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Enumerates the metadata processing behaviors for CDC related data.
 *
 * <p>This enumeration provides definitions for various CDC metadata keys along with their
 * associated data types and converters. Each enum entry represents a specific type of metadata
 * related to CDC and provides a mechanism to read and process this metadata from a given {@link
 * JsonNode} source.
 *
 * <p>The provided converters, which are of type {@link CdcMetadataConverter}, define how the raw
 * metadata is transformed or processed for each specific metadata key.
 */
public enum CdcMetadataProcessor {
    MYSQL_METADATA_PROCESSOR(
            new CdcMetadataConverters.DatabaseNameConverter(),
            new CdcMetadataConverters.TableNameConverter(),
            new CdcMetadataConverters.OpTsConverter()),
    POSTGRES_METADATA_PROCESSOR(
            new CdcMetadataConverters.DatabaseNameConverter(),
            new CdcMetadataConverters.TableNameConverter(),
            new CdcMetadataConverters.SchemaNameConverter(),
            new CdcMetadataConverters.OpTsConverter()),

    KAFKA_DEBEZIUM_META_PROCESSOR(
            new CdcMetadataConverters.SchemaConverter(),
            new CdcMetadataConverters.IngestionTimestampConverter(),
            new CdcMetadataConverters.SourceTimestampConverter(),
            new CdcMetadataConverters.SourceDatabaseConverter(),
            new CdcMetadataConverters.SourceSchemaConverter(),
            new CdcMetadataConverters.SourceTableConverter(),
            new CdcMetadataConverters.SourcePropertiesConverter(),
            new CdcMetadataConverters.OperationConverter());

    private final CdcMetadataConverter[] cdcMetadataConverters;

    CdcMetadataProcessor(CdcMetadataConverter... cdcMetadataConverters) {
        this.cdcMetadataConverters = cdcMetadataConverters;
    }

    private Map<String, CdcMetadataConverter> converterMapping() {
        return Arrays.stream(cdcMetadataConverters)
                .collect(Collectors.toMap(CdcMetadataConverter::columnName, Function.identity()));
    }

    private static final Map<String, CdcMetadataConverter> MYSQL_METADATA_CONVERTERS =
            MYSQL_METADATA_PROCESSOR.converterMapping();

    private static final Map<String, CdcMetadataConverter> POSTGRES_METADATA_CONVERTERS =
            POSTGRES_METADATA_PROCESSOR.converterMapping();

    private static final Map<String, CdcMetadataConverter> KAFKA_DEBEZIUM_META_CONVERTERS =
            KAFKA_DEBEZIUM_META_PROCESSOR.converterMapping();

    public static CdcMetadataConverter converter(
            SyncJobHandler.SourceType sourceType, @Nullable DataFormat dataFormat, String column) {
        switch (sourceType) {
            case MYSQL:
                return MYSQL_METADATA_CONVERTERS.get(column);
            case POSTGRES:
                return POSTGRES_METADATA_CONVERTERS.get(column);
            case KAFKA:
                switch (checkNotNull(dataFormat)) {
                    case DEBEZIUM_JSON:
                        return KAFKA_DEBEZIUM_META_CONVERTERS.get(column);
                    default:
                        throw new UnsupportedOperationException(
                                String.format(
                                        "No metadata converters for source %s with format %s.",
                                        sourceType, dataFormat));
                }
            default:
                throw new UnsupportedOperationException("No metadata converters for " + sourceType);
        }
    }
}
