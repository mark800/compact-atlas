/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.compactatlas.webapp.resources;

import org.apache.compactatlas.intg.AtlasConfiguration;
import org.apache.compactatlas.repository.discovery.AtlasDiscoveryService;
import org.apache.compactatlas.intg.exception.AtlasBaseException;
import org.apache.compactatlas.intg.model.discovery.AtlasSearchResult;
import org.apache.compactatlas.intg.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.compactatlas.intg.model.instance.AtlasEntityHeader;
import org.apache.compactatlas.repository.query.QueryParams;
import org.apache.compactatlas.repository.repository.converters.AtlasInstanceConverter;
import org.apache.compactatlas.repository.repository.store.graph.AtlasEntityStore;
import org.apache.compactatlas.intg.utils.AtlasJson;
import org.apache.compactatlas.common.utils.AtlasPerfTracer;
import org.apache.compactatlas.common.utils.ParamChecker;
import org.apache.compactatlas.intg.v1.model.discovery.DSLSearchResult;
import org.apache.compactatlas.intg.v1.model.discovery.FullTextSearchResult;
import org.apache.compactatlas.intg.v1.model.instance.Referenceable;
import org.apache.compactatlas.webapp.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Jersey Resource for metadata operations.
 */
@RequestMapping("discovery")
@RestController
public class MetadataDiscoveryResource {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataDiscoveryResource.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.MetadataDiscoveryResource");
    private static final String QUERY_TYPE_DSL = "dsl";
    private static final String QUERY_TYPE_FULLTEXT = "full-text";
    private static final String LIMIT_OFFSET_DEFAULT = "-1";

    private final AtlasDiscoveryService atlasDiscoveryService;
    private final AtlasInstanceConverter restAdapters;
    private final AtlasEntityStore entitiesStore;

    /**
     * Created by the Guice ServletModule and injected with the
     * configured DiscoveryService.
     *
     * @param atlasDiscoveryService atlasDiscoveryService
     * @param restAdapters          restAdapters
     * @param entitiesStore         entitiesStore
     */
    @Autowired
    public MetadataDiscoveryResource(AtlasDiscoveryService atlasDiscoveryService, AtlasInstanceConverter restAdapters, AtlasEntityStore entitiesStore) {
        this.atlasDiscoveryService = atlasDiscoveryService;
        this.restAdapters = restAdapters;
        this.entitiesStore = entitiesStore;
    }

    /**
     * Search using a given query.
     *
     * @param query  search query in DSL format falling back to full text.
     * @param limit  number of rows to be returned in the result, used for pagination. maxlimit > limit > 0. -1 maps to atlas.search.defaultlimit property value
     * @param offset offset to the results returned, used for pagination. offset >= 0. -1 maps to offset 0
     * @return JSON representing the type and results.
     */
    @GetMapping("search")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response search(@RequestParam("query") String query,
                           @DefaultValue(LIMIT_OFFSET_DEFAULT) @RequestParam("limit") int limit,
                           @DefaultValue(LIMIT_OFFSET_DEFAULT) @RequestParam("offset") int offset) {
        boolean dslQueryFailed = false;
        Response response = null;
        try {
            response = searchUsingQueryDSL(query, limit, offset);
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                dslQueryFailed = true;
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error while running DSL. Switching to fulltext for query {}", query, e);
            }

            dslQueryFailed = true;
        }

        if (dslQueryFailed) {
            response = searchUsingFullText(query, limit, offset);
        }

        return response;
    }

    /**
     * Search using query DSL format.
     *
     * @param dslQuery search query in DSL format.
     * @param limit    number of rows to be returned in the result, used for pagination. maxlimit > limit > 0. -1 maps to atlas.search.defaultlimit property value
     * @param offset   offset to the results returned, used for pagination. offset >= 0. -1 maps to offset 0
     *                 Limit and offset in API are used in conjunction with limit and offset in DSL query
     *                 Final limit = min(API limit, max(query limit - API offset, 0))
     *                 Final offset = API offset + query offset
     * @return JSON representing the type and results.
     */
    @GetMapping("search/dsl")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response searchUsingQueryDSL(@RequestParam("query") String dslQuery,
                                        @DefaultValue(LIMIT_OFFSET_DEFAULT) @RequestParam("limit") int limit,
                                        @DefaultValue(LIMIT_OFFSET_DEFAULT) @RequestParam("offset") int offset) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> MetadataDiscoveryResource.searchUsingQueryDSL({}, {}, {})", dslQuery, limit, offset);
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MetadataDiscoveryResource.searchUsingQueryDSL(" + dslQuery + ", " + limit + ", " + offset + ")");
            }

            dslQuery = ParamChecker.notEmpty(dslQuery, "dslQuery cannot be null");

            QueryParams queryParams = validateQueryParams(limit, offset);
            AtlasSearchResult result = atlasDiscoveryService.searchUsingDslQuery(dslQuery, queryParams.limit(), queryParams.offset());
            DSLSearchResult dslResult = new DSLSearchResult();

            dslResult.setQueryType(QUERY_TYPE_DSL);
            dslResult.setRequestId(Servlets.getRequestId());
            dslResult.setDataType(result.getType());
            dslResult.setQuery(result.getQueryText());
            dslResult.setCount(0);

            if (CollectionUtils.isNotEmpty(result.getEntities())) {
                for (AtlasEntityHeader entityHeader : result.getEntities()) {
                    Referenceable entity = getEntity(entityHeader.getGuid());

                    dslResult.addResult(entity);
                }

                if (dslResult.getResults() != null) {
                    dslResult.setCount(dslResult.getResults().size());
                }
            } else if (result.getAttributes() != null && CollectionUtils.isNotEmpty(result.getAttributes().getName())) {
                List<String> attrNames = result.getAttributes().getName();

                for (List<Object> attrValues : result.getAttributes().getValues()) {
                    if (attrValues == null) {
                        continue;
                    }

                    Referenceable entity = new Referenceable();

                    for (int i = 0; i < attrNames.size(); i++) {
                        String attrName = attrNames.get(i);
                        Object attrValue = attrValues.size() > i ? attrValues.get(i) : null;

                        entity.set(attrName, attrValue);
                    }

                    dslResult.addResult(entity);
                }

                if (dslResult.getResults() != null) {
                    dslResult.setCount(dslResult.getResults().size());
                }
            }

            String response = AtlasJson.toV1SearchJson(dslResult);

            return Response.ok(response).build();
        } catch (IllegalArgumentException e) {
            LOG.error("Unable to get entity list for dslQuery {}", dslQuery, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (WebApplicationException e) {
            LOG.error("Unable to get entity list for dslQuery {}", dslQuery, e);
            throw e;
        } catch (Throwable e) {
            LOG.error("Unable to get entity list for dslQuery {}", dslQuery, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== MetadataDiscoveryResource.searchUsingQueryDSL({}, {}, {})", dslQuery, limit, offset);
            }
        }
    }

    /**
     * Search using full text search.
     *
     * @param query  search query.
     * @param limit  number of rows to be returned in the result, used for pagination. maxlimit > limit > 0. -1 maps to atlas.search.defaultlimit property value
     * @param offset offset to the results returned, used for pagination. offset >= 0. -1 maps to offset 0
     * @return JSON representing the type and results.
     */
    @GetMapping("search/fulltext")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response searchUsingFullText(@RequestParam("query") String query,
                                        @DefaultValue(LIMIT_OFFSET_DEFAULT) @RequestParam("limit") int limit,
                                        @DefaultValue(LIMIT_OFFSET_DEFAULT) @RequestParam("offset") int offset) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> MetadataDiscoveryResource.searchUsingFullText({}, {}, {})", query, limit, offset);
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MetadataDiscoveryResource.searchUsingFullText(" + query + ", " + limit + ", " + offset + ")");
            }

            query = ParamChecker.notEmpty(query, "query cannot be null or empty");

            QueryParams queryParams = validateQueryParams(limit, offset);
            AtlasSearchResult result = atlasDiscoveryService.searchUsingFullTextQuery(query, false, queryParams.limit(), queryParams.offset());
            FullTextSearchResult fullTextResult = new FullTextSearchResult();

            fullTextResult.setQueryType(QUERY_TYPE_FULLTEXT);
            fullTextResult.setRequestId(Servlets.getRequestId());
            fullTextResult.setDataType(result.getType());
            fullTextResult.setQuery(result.getQueryText());
            fullTextResult.setCount(0);

            if (CollectionUtils.isNotEmpty(result.getFullTextResult())) {
                for (AtlasSearchResult.AtlasFullTextResult entity : result.getFullTextResult()) {
                    fullTextResult.addResult(entity);
                }

                fullTextResult.setCount(fullTextResult.getResults().size());
            }

            String response = AtlasJson.toV1SearchJson(fullTextResult);

            return Response.ok(response).build();
        } catch (IllegalArgumentException e) {
            LOG.error("Unable to get entity list for query {}", query, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (WebApplicationException e) {
            LOG.error("Unable to get entity list for query {}", query, e);
            throw e;
        } catch (Throwable e) {
            LOG.error("Unable to get entity list for query {}", query, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== MetadataDiscoveryResource.searchUsingFullText({}, {}, {})", query, limit, offset);
            }
        }
    }

    private QueryParams validateQueryParams(int limitParam, int offsetParam) {
        int maxLimit = AtlasConfiguration.SEARCH_MAX_LIMIT.getInt();
        int defaultLimit = AtlasConfiguration.SEARCH_DEFAULT_LIMIT.getInt();
        int limit = defaultLimit;
        boolean limitSet = (limitParam != Integer.valueOf(LIMIT_OFFSET_DEFAULT));

        if (limitSet) {
            ParamChecker.lessThan(limitParam, maxLimit, "limit");
            ParamChecker.greaterThan(limitParam, 0, "limit");

            limit = limitParam;
        }

        int offset = 0;
        boolean offsetSet = (offsetParam != Integer.valueOf(LIMIT_OFFSET_DEFAULT));

        if (offsetSet) {
            ParamChecker.greaterThan(offsetParam, -1, "offset");
            offset = offsetParam;
        }

        return new QueryParams(limit, offset);
    }

    private Referenceable getEntity(String guid) throws AtlasBaseException {
        AtlasEntityWithExtInfo entity = entitiesStore.getById(guid);
        Referenceable referenceable = restAdapters.getReferenceable(entity);

        return referenceable;
    }
}
