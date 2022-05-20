/**
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

package org.apache.compactatlas.graphdb.janus.migration;

import org.apache.compactatlas.graphdb.janus.AtlasJanusGraphDatabase;
import org.apache.compactatlas.intg.exception.AtlasBaseException;
import org.apache.compactatlas.intg.model.impexp.MigrationStatus;
import org.apache.compactatlas.intg.model.typedef.AtlasTypesDef;
import org.apache.compactatlas.graphdb.api.GraphDBMigrator;
import org.apache.compactatlas.intg.type.AtlasType;
import org.apache.compactatlas.intg.type.AtlasTypeRegistry;
import org.apache.compactatlas.common.utils.AtlasPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.InputStream;

@Component
public class GraphDBGraphSONMigrator implements GraphDBMigrator {
    private static final Logger LOG      = LoggerFactory.getLogger(GraphDBMigrator.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("GraphDBMigrator");

    private final TypesDefScrubber typesDefStrubberForMigrationImport = new TypesDefScrubber();

    @Override
    public AtlasTypesDef getScrubbedTypesDef(String jsonStr) {
        AtlasTypesDef typesDef = AtlasType.fromJson(jsonStr, AtlasTypesDef.class);

        return typesDefStrubberForMigrationImport.scrub(typesDef);
    }

    @Override
    public void importData(AtlasTypeRegistry typeRegistry, InputStream fs) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            LOG.info("Starting loadLegacyGraphSON...");

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "loadLegacyGraphSON");
            }

            AtlasGraphSONReader legacyGraphSONReader = AtlasGraphSONReader.build().
                    relationshipCache(new ElementProcessors(typeRegistry, typesDefStrubberForMigrationImport)).
                    schemaDB(AtlasJanusGraphDatabase.getGraphInstance()).
                    bulkLoadingDB(AtlasJanusGraphDatabase.getBulkLoadingGraphInstance()).
                    create();

            legacyGraphSONReader.readGraph(fs);
        } catch (Exception ex) {
            LOG.error("Error loading loadLegacyGraphSON2", ex);

            throw new AtlasBaseException(ex);
        } finally {
            AtlasPerfTracer.log(perf);

            LOG.info("Done! loadLegacyGraphSON.");
        }
    }

    @Override
    public MigrationStatus getMigrationStatus() {
        return ReaderStatusManager.get(AtlasJanusGraphDatabase.getGraphInstance());
    }
}
