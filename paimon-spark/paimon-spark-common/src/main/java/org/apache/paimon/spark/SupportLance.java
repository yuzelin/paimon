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

package org.apache.paimon.spark;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.fs.Path;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.spark.catalog.SparkBaseCatalog;
import org.apache.paimon.table.lance.LanceTable;

import com.lancedb.lance.spark.LanceCatalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.spark.utils.CatalogUtils.toIdentifier;

/** PaimonLanceCatalog. */
public abstract class SupportLance extends SparkBaseCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(SupportLance.class);

    private static final String LANCE_PATH_OPTION = "lance_path";
    private static final String LANCE_PATH_SUFFIX = "-lance";

    protected LanceCatalog lanceCatalog = null;

    protected void createLanceTable(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        try {
            LanceTable lanceTable =
                    (LanceTable) paimonCatalog().getTable(toIdentifier(ident, catalogName));
            if (lanceTable.fileIO() instanceof RESTTokenFileIO) {
                LOG.info("In rest catalog, rest server create lance table.");
            } else {
                String lancePath = lanceTable.location() + LANCE_PATH_SUFFIX;
                try {
                    paimonCatalog()
                            .alterTable(
                                    toIdentifier(ident, catalogName),
                                    SchemaChange.setOption(LANCE_PATH_OPTION, lancePath),
                                    true);
                } catch (Catalog.ColumnAlreadyExistException | Catalog.ColumnNotExistException e) {
                    throw new RuntimeException(e);
                }
                resetLanceCatalog(name(), this.catalogOptions, lanceTable);
                Identifier lanceIdentifier = createLanceIdent(lancePath);
                lanceCatalog.createTable(lanceIdentifier, schema, partitions, properties);
            }
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException("Failed to get the newly created lance table.", e);
        }
    }

    protected void dropLanceTable(LanceTable lanceTable) throws Catalog.TableNotExistException {
        resetLanceCatalog(name(), this.catalogOptions, lanceTable);
        lanceCatalog.dropTable(createLanceIdent(lanceTable));
    }

    protected org.apache.spark.sql.connector.catalog.Table loadLanceTable(LanceTable lanceTable)
            throws NoSuchTableException {
        resetLanceCatalog(name(), this.catalogOptions, lanceTable);
        Identifier lanceIdentifier = createLanceIdent(lanceTable);
        return lanceCatalog.loadTable(lanceIdentifier);
    }

    private Identifier createLanceIdent(LanceTable lanceTable) {
        String lancePath;
        if (lanceTable.options().containsKey(LANCE_PATH_OPTION)) {
            lancePath = lanceTable.options().get(LANCE_PATH_OPTION);
        } else {
            lancePath = lanceTable.location();
        }
        return createLanceIdent(lancePath);
    }

    private Identifier createLanceIdent(String lanceTableLocation) {
        return Identifier.of(new String[0], lanceTableLocation);
    }

    private void resetLanceCatalog(
            String catalogName, CaseInsensitiveStringMap catalogOptions, LanceTable table) {
        if (lanceCatalog != null) {
            return;
        }
        URI locationUri = new Path(table.location()).toUri();
        HashMap<String, String> lanceOptions = new HashMap();
        if (table.fileIO() instanceof RESTTokenFileIO) {
            RESTTokenFileIO fileIO = (RESTTokenFileIO) table.fileIO();
            // Convert the oss options from rest token into ones recognizable by lance catalog.
            for (Map.Entry<String, String> kv : fileIO.validToken().token().entrySet()) {
                if ("oss".equals(locationUri.getScheme())) {
                    if (kv.getKey().equals("fs.oss.endpoint")) {
                        lanceOptions.put("oss_endpoint", kv.getValue());
                        lanceOptions.put(
                                "oss_region",
                                kv.getValue().split("\\.")[0].replace("-internal", ""));
                    } else if (kv.getKey().equals("fs.oss.accessKeyId")) {
                        lanceOptions.put("oss_access_key_id", kv.getValue());
                    } else if (kv.getKey().equals("fs.oss.accessKeySecret")) {
                        lanceOptions.put("oss_secret_access_key", kv.getValue());
                    } else if (kv.getKey().equals("fs.oss.securityToken")) {
                        lanceOptions.put("oss_session_token", kv.getValue());
                    }
                }
            }
        }
        // add the options from catalog to lance options
        for (Map.Entry<String, String> kv : catalogOptions.entrySet()) {
            if (kv.getKey().startsWith("lance.")) {
                lanceOptions.put(kv.getKey().substring("lance.".length()), kv.getValue());
            }
        }
        LOG.info("\n\n\nLance Options:");
        for (Map.Entry<String, String> kv : lanceOptions.entrySet()) {
            LOG.info(kv.getKey() + ": " + kv.getValue());
        }
        LOG.info("\n\n");
        lanceCatalog = new LanceCatalog();
        lanceCatalog.initialize(catalogName, new CaseInsensitiveStringMap(lanceOptions));
    }
}
