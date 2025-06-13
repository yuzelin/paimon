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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/** An {@link AbstractStartingScanner} to return plan. */
public abstract class ReadPlanStartingScanner extends AbstractStartingScanner {

    private static final Logger LOG = LoggerFactory.getLogger(ReadPlanStartingScanner.class);

    ReadPlanStartingScanner(SnapshotManager snapshotManager) {
        super(snapshotManager);
    }

    @Nullable
    protected abstract SnapshotReader configure(SnapshotReader snapshotReader);

    @Override
    public Result scan(SnapshotReader snapshotReader) {
        SnapshotReader configured = configure(snapshotReader);
        if (configured == null) {
            return new NoSnapshot();
        }

        SnapshotReader.Plan plan = configured.read();
        List<DataSplit> dataSplits = plan.dataSplits();
        StringBuilder sb = new StringBuilder();
        for (DataSplit split : dataSplits) {
            sb.append(String.format("split bucket path = %s\n", split.bucketPath()));
            sb.append("fileNames:\n");
            List<DataFileMeta> fileMetas = split.dataFiles();
            for (DataFileMeta fileMeta : fileMetas) {
                sb.append(fileMeta.fileName()).append("\n");
            }
            if (!split.deletionFiles().isPresent()) {
                sb.append("no deletion files.\n");
            } else {
                sb.append("deletionFiles:\n");
                List<DeletionFile> deletionFiles = split.deletionFiles().get();
                for (DeletionFile deletionFile : deletionFiles) {
                    if (deletionFile == null) {
                        sb.append("null deletion file.\n");
                    } else {
                        sb.append(deletionFile.toString()).append("\n");
                    }
                }
            }
        }

        LOG.info(
                "DEBUG MESSAGE.\nsnapshotId={}\nsplits messages:\n{}",
                plan.snapshotId(),
                sb.toString());

        return StartingScanner.fromPlan(plan);
    }

    @Override
    public List<PartitionEntry> scanPartitions(SnapshotReader snapshotReader) {
        SnapshotReader configured = configure(snapshotReader);
        if (configured == null) {
            return Collections.emptyList();
        }
        return configured.partitionEntries();
    }
}
