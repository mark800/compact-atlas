/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.compactatlas.webapp.rest;

import org.apache.compactatlas.common.annotation.Timed;
import org.apache.compactatlas.intg.exception.AtlasBaseException;
import org.apache.compactatlas.intg.model.instance.AtlasRelationship;
import org.apache.compactatlas.intg.model.instance.AtlasRelationship.AtlasRelationshipWithExtInfo;
import org.apache.compactatlas.repository.repository.store.graph.AtlasRelationshipStore;
import org.apache.compactatlas.webapp.util.Servlets;
import org.apache.compactatlas.common.utils.AtlasPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

/**
 * REST interface for entity relationships.
 */
@RequestMapping("api/atlas/v2/relationship")
@RestController
public class RelationshipREST {
    private static final Logger LOG = LoggerFactory.getLogger(LineageREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.RelationshipREST");

    private final AtlasRelationshipStore relationshipStore;

    @Autowired
    public RelationshipREST(AtlasRelationshipStore relationshipStore) {
        this.relationshipStore = relationshipStore;
    }

    /**
     * Create a new relationship between entities.
     */
    @PostMapping("")
    @Timed
    public AtlasRelationship create(@RequestBody AtlasRelationship relationship) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "RelationshipREST.create(" + relationship + ")");
            }

            return relationshipStore.create(relationship);

        } catch (Exception e) {
            LOG.warn(e.getMessage());
            return null;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Update an existing relationship between entities.
     */
    @PutMapping("")
    public AtlasRelationship update(AtlasRelationship relationship) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "RelationshipREST.update(" + relationship + ")");
            }

            return relationshipStore.update(relationship);

        } catch (Exception e) {
            LOG.warn(e.getMessage());
            return null;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Get relationship information between entities using guid.
     */
    @GetMapping("/guid/{guid}")
    @Timed
    public AtlasRelationshipWithExtInfo getById(@PathVariable String guid,
                                                @RequestParam(name = "extendedInfo", defaultValue = "false") boolean extendedInfo)
            throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        AtlasRelationshipWithExtInfo ret;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "RelationshipREST.getById(" + guid + ")");
            }

            if (extendedInfo) {
                ret = relationshipStore.getExtInfoById(guid);
            } else {
                ret = new AtlasRelationshipWithExtInfo(relationshipStore.getById(guid));
            }

            return ret;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Delete a relationship between entities using guid.
     */
    @DeleteMapping("/guid/{guid}")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void deleteById(@PathVariable String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "RelationshipREST.deleteById(" + guid + ")");
            }

            relationshipStore.deleteById(guid);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }
}