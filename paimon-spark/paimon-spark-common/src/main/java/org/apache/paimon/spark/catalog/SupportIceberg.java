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

package org.apache.paimon.spark.catalog;

import org.apache.paimon.spark.SparkIcebergTable;
import org.apache.paimon.spark.SupportLance;
import org.apache.paimon.table.iceberg.IcebergTableImpl;

import org.apache.spark.sql.PaimonSparkSession$;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/** Catalog supports iceberg table. */
public abstract class SupportIceberg extends SupportLance {

    protected boolean isIcebergTable(Map<String, String> properties) {
        boolean isIcebergTable =
                properties.getOrDefault("provider", "").equalsIgnoreCase("iceberg");
        return IcebergCatalogUtils.usingIceberg(isIcebergTable);
    }

    protected boolean isIcebergTable(Table table) {
        boolean isIcebergTable = table instanceof SparkIcebergTable;
        return IcebergCatalogUtils.usingIceberg(isIcebergTable);
    }

    protected boolean isIcebergTable(org.apache.paimon.table.Table table) {
        boolean isIcebergTable = table instanceof IcebergTableImpl;
        return IcebergCatalogUtils.usingIceberg(isIcebergTable);
    }

    private TableCatalog icebergCatalog = null;

    private TableCatalog icebergCatalog() {
        if (icebergCatalog != null) {
            return icebergCatalog;
        }

        String icebergCatalogName = IcebergCatalogUtils.buildIcebergCatalogName(catalogName);
        SparkSession sparkSession = PaimonSparkSession$.MODULE$.active();
        icebergCatalog =
                (TableCatalog)
                        sparkSession.sessionState().catalogManager().catalog(icebergCatalogName);
        return icebergCatalog;
    }

    protected org.apache.spark.sql.connector.catalog.Table loadIcebergTable(Identifier ident)
            throws NoSuchTableException {
        return icebergCatalog().loadTable(ident);
    }

    protected org.apache.spark.sql.connector.catalog.Table alterIcebergTable(
            Identifier ident, TableChange... changes) throws NoSuchTableException {
        return icebergCatalog().alterTable(ident, changes);
    }

    protected org.apache.spark.sql.connector.catalog.Table createIcebergTable(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        return icebergCatalog().createTable(ident, schema, partitions, properties);
    }

    public boolean purgeIcebergTable(Identifier ident) throws UnsupportedOperationException {
        return icebergCatalog.purgeTable(ident);
    }

    public void invalidateIcebergTable(Identifier ident) throws UnsupportedOperationException {
        icebergCatalog.invalidateTable(ident);
    }
}
