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

import org.apache.compactatlas.client.AtlasClient;
import org.apache.compactatlas.intg.AtlasErrorCode;
import org.apache.compactatlas.intg.SortOrder;
import org.apache.compactatlas.common.annotation.Timed;
import org.apache.compactatlas.repository.discovery.AtlasDiscoveryService;
import org.apache.compactatlas.repository.discovery.EntityDiscoveryService;
import org.apache.compactatlas.intg.exception.AtlasBaseException;
import org.apache.compactatlas.intg.model.discovery.*;
import org.apache.compactatlas.intg.model.discovery.SearchParameters.FilterCriteria;
import org.apache.compactatlas.intg.model.profile.AtlasUserSavedSearch;
import org.apache.compactatlas.common.repository.Constants;
import org.apache.compactatlas.intg.type.AtlasEntityType;
import org.apache.compactatlas.intg.type.AtlasStructType;
import org.apache.compactatlas.intg.type.AtlasTypeRegistry;
import org.apache.compactatlas.webapp.util.AtlasAuthorizationUtils;
import org.apache.compactatlas.webapp.util.Servlets;
import org.apache.compactatlas.common.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * REST interface for data discovery using dsl or full text search
 */
@RequestMapping("api/atlas/v2/search")
@RestController
public class DiscoveryREST {
    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.DiscoveryREST");

    @Context
    private HttpServletRequest httpServletRequest;
    private final int maxFullTextQueryLength;
    private final int maxDslQueryLength;

    private final AtlasTypeRegistry typeRegistry;
    private final AtlasDiscoveryService discoveryService;

    @Autowired
    public DiscoveryREST(AtlasTypeRegistry typeRegistry, AtlasDiscoveryService discoveryService, Configuration configuration) {
        this.typeRegistry = typeRegistry;
        this.discoveryService = discoveryService;
        this.maxFullTextQueryLength = configuration.getInt(Constants.MAX_FULLTEXT_QUERY_STR_LENGTH, 4096);
        this.maxDslQueryLength = configuration.getInt(Constants.MAX_DSL_QUERY_STR_LENGTH, 4096);
    }

    /**
     * Retrieve data for the specified DSL
     *
     * @param query          DSL query
     * @param typeName       limit the result to only entities of specified type or its sub-types
     * @param classification limit the result to only entities tagged with the given classification or or its sub-types
     * @param limit          limit the result set to only include the specified number of entries
     * @param offset         start offset of the result set (useful for pagination)
     * @return Search results
     * @throws AtlasBaseException
     * @HTTP 200 On successful DSL execution with some results, might return an empty list if execution succeeded
     * without any results
     * @HTTP 400 Invalid DSL or query parameters
     */
    @GetMapping("/dsl")
    @Timed
    public AtlasSearchResult searchUsingDSL(@RequestParam("query") String query,
                                            @RequestParam("typeName") String typeName,
                                            @RequestParam("classification") String classification,
                                            @RequestParam("limit") int limit,
                                            @RequestParam("offset") int offset) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);
        Servlets.validateQueryParamLength("classification", classification);

        if (StringUtils.isNotEmpty(query)) {
            if (query.length() > maxDslQueryLength) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_QUERY_LENGTH, Constants.MAX_DSL_QUERY_STR_LENGTH);
            }
            query = Servlets.decodeQueryString(query);
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.searchUsingDSL(" + query + "," + typeName
                        + "," + classification + "," + limit + "," + offset + ")");
            }

            String queryStr = discoveryService.getDslQueryUsingTypeNameClassification(query, typeName, classification);

            return discoveryService.searchUsingDslQuery(queryStr, limit, offset);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }


    /**
     * Retrieve data for the specified fulltext query
     *
     * @param query  Fulltext query
     * @param limit  limit the result set to only include the specified number of entries
     * @param offset start offset of the result set (useful for pagination)
     * @return Search results
     * @throws AtlasBaseException
     * @HTTP 200 On successful FullText lookup with some results, might return an empty list if execution succeeded
     * without any results
     * @HTTP 400 Invalid fulltext or query parameters
     */
    @GetMapping("/fulltext")
    @Timed
    public AtlasSearchResult searchUsingFullText(@RequestParam("query") String query,
                                                 @RequestParam("excludeDeletedEntities") boolean excludeDeletedEntities,
                                                 @RequestParam("limit") int limit,
                                                 @RequestParam("offset") int offset) throws AtlasBaseException {
        // Validate FullText query for max allowed length
        if (StringUtils.isNotEmpty(query) && query.length() > maxFullTextQueryLength) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_QUERY_LENGTH, Constants.MAX_FULLTEXT_QUERY_STR_LENGTH);
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.searchUsingFullText(" + query + "," +
                        limit + "," + offset + ")");
            }

            return discoveryService.searchUsingFullTextQuery(query, excludeDeletedEntities, limit, offset);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Retrieve data for the specified fulltext query
     *
     * @param query          Fulltext query
     * @param typeName       limit the result to only entities of specified type or its sub-types
     * @param classification limit the result to only entities tagged with the given classification or or its sub-types
     * @param limit          limit the result set to only include the specified number of entries
     * @param offset         start offset of the result set (useful for pagination)
     * @return Search results
     * @throws AtlasBaseException
     * @HTTP 200 On successful FullText lookup with some results, might return an empty list if execution succeeded
     * without any results
     * @HTTP 400 Invalid fulltext or query parameters
     */
    @GetMapping("/basic")
    @Timed
    public AtlasSearchResult searchUsingBasic(@RequestParam("query") String query,
                                              @RequestParam("typeName") String typeName,
                                              @RequestParam("classification") String classification,
                                              @RequestParam("sortBy") String sortByAttribute,
                                              @RequestParam("sortOrder") SortOrder sortOrder,
                                              @RequestParam("excludeDeletedEntities") boolean excludeDeletedEntities,
                                              @RequestParam("limit") int limit,
                                              @RequestParam("offset") int offset,
                                              @RequestParam("marker") String marker) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);
        Servlets.validateQueryParamLength("classification", classification);
        Servlets.validateQueryParamLength("sortBy", sortByAttribute);
        if (StringUtils.isNotEmpty(query) && query.length() > maxFullTextQueryLength) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_QUERY_LENGTH, Constants.MAX_FULLTEXT_QUERY_STR_LENGTH);
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.searchUsingBasic(" + query + "," +
                        typeName + "," + classification + "," + limit + "," + offset + ")");
            }

            SearchParameters searchParameters = new SearchParameters();
            searchParameters.setTypeName(typeName);
            searchParameters.setClassification(classification);
            searchParameters.setQuery(query);
            searchParameters.setExcludeDeletedEntities(excludeDeletedEntities);
            searchParameters.setLimit(limit);
            searchParameters.setOffset(offset);
            searchParameters.setMarker(marker);
            searchParameters.setSortBy(sortByAttribute);
            searchParameters.setSortOrder(sortOrder);

            return discoveryService.searchWithParameters(searchParameters);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Retrieve data for the specified attribute search query
     *
     * @param attrName        Attribute name
     * @param attrValuePrefix Attibute value to search on
     * @param typeName        limit the result to only entities of specified type or its sub-types
     * @param limit           limit the result set to only include the specified number of entries
     * @param offset          start offset of the result set (useful for pagination)
     * @return Search results
     * @throws AtlasBaseException
     * @HTTP 200 On successful FullText lookup with some results, might return an empty list if execution succeeded
     * without any results
     * @HTTP 400 Invalid wildcard or query parameters
     */
    @GetMapping("/attribute")
    public AtlasSearchResult searchUsingAttribute(@RequestParam(value = "attrName", required = false) String attrName,
                                                  @RequestParam("attrValuePrefix") String attrValuePrefix,
                                                  @RequestParam("typeName") String typeName,
                                                  @RequestParam("limit") int limit,
                                                  @RequestParam("offset") int offset) throws AtlasBaseException {
        //Servlets.validateQueryParamLength("attrName", attrName);
        Servlets.validateQueryParamLength("attrValuePrefix", attrValuePrefix);
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.searchUsingAttribute(" + attrName + "," +
                        attrValuePrefix + "," + typeName + "," + limit + "," + offset + ")");
            }

            if (StringUtils.isEmpty(attrName) && StringUtils.isEmpty(attrValuePrefix)) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS,
                        String.format("attrName : %s, attrValue: %s for attribute search.", attrName, attrValuePrefix));
            }

            if (StringUtils.isEmpty(attrName)) {
                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

                if (entityType != null) {
                    String[] defaultAttrNames = new String[]{AtlasClient.QUALIFIED_NAME, AtlasClient.NAME};

                    for (String defaultAttrName : defaultAttrNames) {
                        AtlasStructType.AtlasAttribute attribute = entityType.getAttribute(defaultAttrName);

                        if (attribute != null) {
                            attrName = defaultAttrName;

                            break;
                        }
                    }
                }

                if (StringUtils.isEmpty(attrName)) {
                    attrName = AtlasClient.QUALIFIED_NAME;
                }
            }

            SearchParameters searchParams = new SearchParameters();
            FilterCriteria attrFilter = new FilterCriteria();

            attrFilter.setAttributeName(StringUtils.isEmpty(attrName) ? AtlasClient.QUALIFIED_NAME : attrName);
            attrFilter.setOperator(SearchParameters.Operator.STARTS_WITH);
            attrFilter.setAttributeValue(attrValuePrefix);

            searchParams.setTypeName(typeName);
            searchParams.setEntityFilters(attrFilter);
            searchParams.setOffset(offset);
            searchParams.setLimit(limit);

            return searchWithParameters(searchParams);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Attribute based search for entities satisfying the search parameters
     *
     * @param parameters Search parameters
     * @return Atlas search result
     * @throws AtlasBaseException
     * @HTTP 200 On successful search
     * @HTTP 400 Tag/Entity doesn't exist or Tag/entity filter is present without tag/type name
     */
    @PostMapping("/basic")
    public AtlasSearchResult searchWithParameters(@RequestBody SearchParameters parameters) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.searchWithParameters(" + parameters + ")");
            }

            if (parameters.getLimit() < 0 || parameters.getOffset() < 0) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Limit/offset should be non-negative");
            }

            if (StringUtils.isEmpty(parameters.getTypeName()) && !isEmpty(parameters.getEntityFilters())) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "EntityFilters specified without Type name");
            }

            if (StringUtils.isEmpty(parameters.getClassification()) && !isEmpty(parameters.getTagFilters())) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "TagFilters specified without tag name");
            }

            if (StringUtils.isEmpty(parameters.getTypeName()) && StringUtils.isEmpty(parameters.getClassification()) &&
                    StringUtils.isEmpty(parameters.getQuery()) && StringUtils.isEmpty(parameters.getTermName())) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_SEARCH_PARAMS);
            }

            validateSearchParameters(parameters);

            return discoveryService.searchWithParameters(parameters);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Relationship search to search for related entities satisfying the search parameters
     *
     * @param guid            Attribute name
     * @param relation        relationName
     * @param attributes      set of attributes in search result.
     * @param sortByAttribute sort the result using this attribute name, default value is 'name'
     * @param sortOrder       sorting order
     * @param limit           limit the result set to only include the specified number of entries
     * @param offset          start offset of the result set (useful for pagination)
     * @return Atlas search result
     * @throws AtlasBaseException
     * @HTTP 200 On successful search
     * @HTTP 400 guid is not a valid entity type or attributeName is not a valid relationship attribute
     */
    @GetMapping("/relationship")
    public AtlasSearchResult searchRelatedEntities(@RequestParam("guid") String guid,
                                                   @RequestParam("relation") String relation,
                                                   @RequestParam(name = "attributes", required = false) Set<String> attributes,
                                                   @RequestParam(name = "sortBy", required = false) String sortByAttribute,
                                                   @RequestParam(name = "sortOrder", required = false) SortOrder sortOrder,
                                                   @RequestParam("excludeDeletedEntities") boolean excludeDeletedEntities,
                                                   @RequestParam(name = "includeClassificationAttributes", required = false) boolean includeClassificationAttributes,
                                                   @RequestParam(name = "getApproximateCount", required = false) boolean getApproximateCount,
                                                   @RequestParam("limit") int limit,
                                                   @RequestParam("offset") int offset) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);
        Servlets.validateQueryParamLength("relation", relation);
        Servlets.validateQueryParamLength("sortBy", sortByAttribute);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.relatedEntitiesSearch(" + guid +
                        ", " + relation + ", " + sortByAttribute + ", " + sortOrder + ", " + excludeDeletedEntities + ", " + getApproximateCount + ", " + limit + ", " + offset + ")");
            }

            SearchParameters parameters = new SearchParameters();
            parameters.setAttributes(attributes);
            parameters.setSortBy(sortByAttribute);
            parameters.setSortOrder(sortOrder);
            parameters.setExcludeDeletedEntities(excludeDeletedEntities);
            parameters.setLimit(limit);
            parameters.setOffset(offset);
            parameters.setIncludeClassificationAttributes(includeClassificationAttributes);
            return discoveryService.searchRelatedEntities(guid, relation, getApproximateCount, parameters);

        } catch (Exception e) {
            LOG.warn(e.getMessage());
            return new AtlasSearchResult();
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * @param savedSearch
     * @return the saved search-object
     * @throws AtlasBaseException
     * @throws IOException
     */
    @PostMapping("/saved")
    public AtlasUserSavedSearch addSavedSearch(@RequestBody AtlasUserSavedSearch savedSearch) throws AtlasBaseException, IOException {
        validateUserSavedSearch(savedSearch);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.addSavedSearch(userName=" + savedSearch.getOwnerName() + ", name=" + savedSearch.getName() + ", searchType=" + savedSearch.getSearchType() + ")");
            }
            return discoveryService.addSavedSearch(AtlasAuthorizationUtils.getCurrentUserName(), savedSearch);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /***
     *
     * @param savedSearch
     * @return the updated search-object
     * @throws AtlasBaseException
     */
    @PutMapping("/saved")
    @Timed
    public AtlasUserSavedSearch updateSavedSearch(@RequestBody AtlasUserSavedSearch savedSearch) throws AtlasBaseException {
        validateUserSavedSearch(savedSearch);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.updateSavedSearch(userName=" + savedSearch.getOwnerName() + ", name=" + savedSearch.getName() + ", searchType=" + savedSearch.getSearchType() + ")");
            }

            return discoveryService.updateSavedSearch(AtlasAuthorizationUtils.getCurrentUserName(), savedSearch);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * @param searchName Name of the saved search
     * @param userName   User for whom the search is retrieved
     * @return
     * @throws AtlasBaseException
     */
    @GetMapping("/saved/{searchName}")
    @Timed
    public AtlasUserSavedSearch getSavedSearch(@PathVariable String searchName,
                                               @RequestParam("user") String userName) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", searchName);
        Servlets.validateQueryParamLength("user", userName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.getSavedSearch(userName=" + userName + ", name=" + searchName + ")");
            }

            return discoveryService.getSavedSearchByName(AtlasAuthorizationUtils.getCurrentUserName(), userName, searchName);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * @param userName User for whom the search is retrieved
     * @return list of all saved searches for given user
     * @throws AtlasBaseException
     */
    @GetMapping("/saved")
    @Timed
    public List<AtlasUserSavedSearch> getSavedSearches(@RequestParam(value = "user", required = false) String userName) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            List<AtlasUserSavedSearch> ret = discoveryService.getSavedSearches(AtlasAuthorizationUtils.getCurrentUserName(), userName);
            return ret;
        } catch (Exception e) {
            LOG.warn(e.getMessage());
        } finally {
            AtlasPerfTracer.log(perf);
        }
        return null;
    }

    /**
     * @param guid Name of the saved search
     */
    @DeleteMapping("saved/{guid}")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void deleteSavedSearch(@PathVariable String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.deleteSavedSearch(guid=" + guid + ")");
            }

            discoveryService.deleteSavedSearch(AtlasAuthorizationUtils.getCurrentUserName(), guid);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }


    /**
     * Attribute based search for entities satisfying the search parameters
     *
     * @param searchName name of saved-search
     * @param userName   saved-search owner
     * @return Atlas search result
     * @throws AtlasBaseException
     */
    @GetMapping("saved/execute/{searchName}")
    @Timed
    public AtlasSearchResult executeSavedSearchByName(@PathVariable String searchName,
                                                      @RequestParam("user") String userName) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", searchName);
        Servlets.validateQueryParamLength("user", userName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG,
                        "DiscoveryREST.executeSavedSearchByName(userName=" + userName + ", " + "name=" + searchName + ")");
            }

            AtlasUserSavedSearch savedSearch = discoveryService.getSavedSearchByName(AtlasAuthorizationUtils.getCurrentUserName(), userName, searchName);

            return executeSavedSearch(savedSearch);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Attribute based search for entities satisfying the search parameters
     *
     * @param searchGuid Guid identifying saved search
     * @return Atlas search result
     * @throws AtlasBaseException
     */
    @GetMapping("saved/execute/guid/{guid}")
    @Timed
    public AtlasSearchResult executeSavedSearchByGuid(@PathVariable String searchGuid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", searchGuid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.executeSavedSearchByGuid(" + searchGuid + ")");
            }

            AtlasUserSavedSearch savedSearch = discoveryService.getSavedSearchByGuid(AtlasAuthorizationUtils.getCurrentUserName(), searchGuid);

            return executeSavedSearch(savedSearch);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Attribute based search for entities satisfying the search parameters
     *
     * @return Atlas search result
     * @throws AtlasBaseException
     * @HTTP 200 On successful search
     * @HTTP 400 Tag/Entity doesn't exist or Tag/entity filter is present without tag/type name
     */
    @GetMapping("/quick")
    @Timed
    public AtlasQuickSearchResult quickSearch(@RequestParam("query") String query,
                                              @RequestParam(required = false) String typeName,
                                              @RequestParam(required = false) boolean excludeDeletedEntities,
                                              @RequestParam("offset") int offset,
                                              @RequestParam("limit") int limit,
                                              @RequestParam(value = "sortBy", required = false) String sortByAttribute,
                                              @RequestParam(value = "sortOrder", required = false) SortOrder sortOrder) throws AtlasBaseException {


        if (StringUtils.isNotEmpty(query) && query.length() > maxFullTextQueryLength) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_QUERY_LENGTH, Constants.MAX_FULLTEXT_QUERY_STR_LENGTH);
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.quick(" + query + "," +
                        "excludeDeletedEntities:" + excludeDeletedEntities + "," + limit + "," + offset + ")");
            }

            QuickSearchParameters quickSearchParameters = new QuickSearchParameters(query,
                    typeName,
                    null,  // entityFilters
                    false, // includeSubTypes
                    excludeDeletedEntities,
                    offset,
                    limit,
                    null, //attributes,
                    sortByAttribute,
                    sortOrder
            );

            return discoveryService.quickSearch(quickSearchParameters);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Attribute based search for entities satisfying the search parameters
     *
     * @return Atlas search result
     * @throws AtlasBaseException
     * @HTTP 200 On successful search
     * @HTTP 400 Entity/attribute doesn't exist or entity filter is present without type name
     */
    @PostMapping("/quick")
    @Timed
    public AtlasQuickSearchResult quickSearch(QuickSearchParameters quickSearchParameters) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.searchWithParameters(" + quickSearchParameters + ")");
            }

            if (quickSearchParameters.getLimit() < 0 || quickSearchParameters.getOffset() < 0) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Limit/offset should be non-negative");
            }

            if (StringUtils.isEmpty(quickSearchParameters.getTypeName()) &&
                    !isEmpty(quickSearchParameters.getEntityFilters())) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "EntityFilters specified without Type name");
            }

            if (StringUtils.isEmpty(quickSearchParameters.getTypeName()) &&
                    StringUtils.isEmpty(quickSearchParameters.getQuery())) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_SEARCH_PARAMS);
            }

            if (StringUtils.isEmpty(quickSearchParameters.getTypeName()) &&
                    StringUtils.isEmpty(quickSearchParameters.getQuery())) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_SEARCH_PARAMS);
            }

            validateSearchParameters(quickSearchParameters);

            return discoveryService.quickSearch(quickSearchParameters);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @GetMapping("suggestions")
    @Timed
    public AtlasSuggestionsResult getSuggestions(@RequestParam("prefixString") String prefixString, @RequestParam(required = false) String fieldName) {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.getSuggestions(" + prefixString + "," + fieldName + ")");
            }

            return discoveryService.getSuggestions(prefixString, fieldName);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private boolean isEmpty(SearchParameters.FilterCriteria filterCriteria) {
        return filterCriteria == null ||
                (StringUtils.isEmpty(filterCriteria.getAttributeName()) && CollectionUtils.isEmpty(filterCriteria.getCriterion()));
    }

    private AtlasSearchResult executeSavedSearch(AtlasUserSavedSearch savedSearch) throws AtlasBaseException {
        SearchParameters sp = savedSearch.getSearchParameters();

        if (savedSearch.getSearchType() == AtlasUserSavedSearch.SavedSearchType.ADVANCED) {
            String dslQuery = discoveryService.getDslQueryUsingTypeNameClassification(sp.getQuery(), sp.getTypeName(), sp.getClassification());

            return discoveryService.searchUsingDslQuery(dslQuery, sp.getLimit(), sp.getOffset());
        } else {
            return discoveryService.searchWithParameters(sp);
        }
    }

    private void validateUserSavedSearch(AtlasUserSavedSearch savedSearch) throws AtlasBaseException {
        if (savedSearch != null) {
            Servlets.validateQueryParamLength("name", savedSearch.getName());
            Servlets.validateQueryParamLength("ownerName", savedSearch.getOwnerName());
            Servlets.validateQueryParamLength("guid", savedSearch.getGuid());

            validateSearchParameters(savedSearch.getSearchParameters());
        }
    }

    private void validateSearchParameters(SearchParameters parameters) throws AtlasBaseException {
        if (parameters != null) {
            Servlets.validateQueryParamLength("typeName", parameters.getTypeName());
            Servlets.validateQueryParamLength("classification", parameters.getClassification());
            Servlets.validateQueryParamLength("sortBy", parameters.getSortBy());
            if (StringUtils.isNotEmpty(parameters.getQuery()) && parameters.getQuery().length() > maxFullTextQueryLength) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_QUERY_LENGTH, Constants.MAX_FULLTEXT_QUERY_STR_LENGTH);
            }

        }
    }

    private void validateSearchParameters(QuickSearchParameters parameters) throws AtlasBaseException {
        if (parameters != null) {
            validateSearchParameters(EntityDiscoveryService.createSearchParameters(parameters));
        }
    }
}
