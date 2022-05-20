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


import org.apache.compactatlas.intg.AtlasErrorCode;
import org.apache.compactatlas.common.annotation.Timed;
import org.apache.compactatlas.repository.discovery.AtlasLineageService;
import org.apache.compactatlas.intg.exception.AtlasBaseException;
import org.apache.compactatlas.intg.model.TypeCategory;
import org.apache.compactatlas.intg.model.lineage.AtlasLineageInfo;
import org.apache.compactatlas.intg.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.compactatlas.repository.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.compactatlas.intg.type.AtlasEntityType;
import org.apache.compactatlas.intg.type.AtlasTypeRegistry;
import org.apache.compactatlas.webapp.util.Servlets;
import org.apache.compactatlas.common.utils.AtlasPerfTracer;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import java.util.HashMap;
import java.util.Map;

/**
 * REST interface for an entity's lineage information
 */
@RequestMapping("api/atlas/v2/lineage")
@RestController
public class LineageREST {
    private static final Logger LOG = LoggerFactory.getLogger(LineageREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.LineageREST");
    private static final String PREFIX_ATTR = "attr:";

    private final AtlasTypeRegistry typeRegistry;
    private final AtlasLineageService atlasLineageService;
    private static final String DEFAULT_DIRECTION = "BOTH";
    private static final String DEFAULT_DEPTH = "3";

    @Context
    private HttpServletRequest httpServletRequest;

    @Autowired
    public LineageREST(AtlasTypeRegistry typeRegistry, AtlasLineageService atlasLineageService) {
        this.typeRegistry = typeRegistry;
        this.atlasLineageService = atlasLineageService;
    }

    /**
     * Returns lineage info about entity.
     *
     * @param guid      - unique entity id
     * @param direction - input, output or both
     * @param depth     - number of hops for lineage
     * @return AtlasLineageInfo
     * @throws AtlasBaseException
     * @HTTP 200 If Lineage exists for the given entity
     * @HTTP 400 Bad query parameters
     * @HTTP 404 If no lineage is found for the given entity
     */
    @GetMapping("/{guid}")
    @Timed
    public AtlasLineageInfo getLineageGraph(@PathVariable String guid,
                                            @RequestParam(defaultValue = DEFAULT_DIRECTION) LineageDirection direction,
                                            @RequestParam(defaultValue = DEFAULT_DEPTH) int depth) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            return atlasLineageService.getAtlasLineageInfo(guid, direction, depth);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            return null;
        }finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Returns lineage info about entity.
     * <p>
     * In addition to the typeName path parameter, attribute key-value pair(s) can be provided in the following format
     * <p>
     * attr:<attrName>=<attrValue>
     * <p>
     * NOTE: The attrName and attrValue should be unique across entities, eg. qualifiedName
     *
     * @param typeName  - typeName of entity
     * @param direction - input, output or both
     * @param depth     - number of hops for lineage
     * @return AtlasLineageInfo
     * @throws AtlasBaseException
     * @HTTP 200 If Lineage exists for the given entity
     * @HTTP 400 Bad query parameters
     * @HTTP 404 If no lineage is found for the given entity
     */
    @GetMapping("/uniqueAttribute/type/{typeName}")
    public AtlasLineageInfo getLineageByUniqueAttribute(@PathVariable String typeName, @RequestParam(defaultValue = DEFAULT_DIRECTION) LineageDirection direction,
                                                        @RequestParam(defaultValue = DEFAULT_DEPTH) int depth, @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);
        AtlasPerfTracer perf = null;

        try {
            AtlasEntityType entityType = ensureEntityType(typeName);
            Map<String, Object> attributes = getAttributes(servletRequest);
            String guid = AtlasGraphUtilsV2.getGuidByUniqueAttributes(entityType, attributes);

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "LineageREST.getLineageByUniqueAttribute(" + typeName + "," + attributes + "," + direction +
                        "," + depth + ")");
            }

            return atlasLineageService.getAtlasLineageInfo(guid, direction, depth);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            return null;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private Map<String, Object> getAttributes(HttpServletRequest request) {
        Map<String, Object> attributes = new HashMap<>();

        if (MapUtils.isNotEmpty(request.getParameterMap())) {
            for (Map.Entry<String, String[]> e : ((Map<String, String[]>) request.getParameterMap()).entrySet()) {
                String key = e.getKey();

                if (key != null && key.startsWith(PREFIX_ATTR)) {
                    String[] values = e.getValue();
                    String value = values != null && values.length > 0 ? values[0] : null;

                    attributes.put(key.substring(PREFIX_ATTR.length()), value);
                }
            }
        }

        return attributes;
    }

    private AtlasEntityType ensureEntityType(String typeName) throws AtlasBaseException {
        AtlasEntityType ret = typeRegistry.getEntityTypeByName(typeName);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), typeName);
        }

        return ret;
    }
}
