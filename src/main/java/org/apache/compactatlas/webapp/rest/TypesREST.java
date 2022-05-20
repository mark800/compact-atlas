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
import org.apache.compactatlas.intg.model.SearchFilter;
import org.apache.compactatlas.intg.model.typedef.*;
import org.apache.compactatlas.repository.repository.store.graph.v2.AtlasTypeDefGraphStoreV2;
import org.apache.compactatlas.repository.repository.util.FilterUtil;
import org.apache.compactatlas.intg.type.AtlasTypeUtil;
import org.apache.compactatlas.webapp.util.Servlets;
import org.apache.compactatlas.common.utils.AtlasPerfTracer;
import org.apache.http.annotation.Experimental;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import java.util.List;
import java.util.Set;

/**
 * REST interface for CRUD operations on type definitions
 */
@RequestMapping("api/atlas/v2/types")
@RestController
public class TypesREST {
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.TypesREST");

    private final AtlasTypeDefGraphStoreV2 typeDefStore;

    @Autowired
    public TypesREST(AtlasTypeDefGraphStoreV2 typeDefStore) {
        this.typeDefStore = typeDefStore;
    }

    /**
     * Get type definition by it's name
     *
     * @param name Type name
     * @return Type definition
     * @throws AtlasBaseException
     * @HTTP 200 Successful lookup by name
     * @HTTP 404 Failed lookup by name
     */
    @GetMapping("/typedef/name/{name}")
    @Timed
    public AtlasBaseTypeDef getTypeDefByName(@PathVariable("name") String name) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", name);

        AtlasBaseTypeDef ret = typeDefStore.getByName(name);

        return ret;
    }

    /**
     * @param guid GUID of the type
     * @return Type definition
     * @throws AtlasBaseException
     * @HTTP 200 Successful lookup
     * @HTTP 404 Failed lookup
     */
    @GetMapping("/typedef/guid/{guid}")
    @Timed
    public AtlasBaseTypeDef getTypeDefByGuid(@PathVariable("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasBaseTypeDef ret = typeDefStore.getByGuid(guid);

        return ret;
    }

    /**
     * Bulk retrieval API for all type definitions returned as a list of minimal information header
     *
     * @return List of AtlasTypeDefHeader {@link AtlasTypeDefHeader}
     * @throws AtlasBaseException
     * @HTTP 200 Returns a list of {@link AtlasTypeDefHeader} matching the search criteria
     * or an empty list if no match.
     */
    @GetMapping("/typedefs/headers")
    @Timed
    public List<AtlasTypeDefHeader> getTypeDefHeaders(@Context HttpServletRequest httpServletRequest) throws AtlasBaseException {
        SearchFilter searchFilter = getSearchFilter(httpServletRequest);

        AtlasTypesDef searchTypesDef = typeDefStore.searchTypesDef(searchFilter);

        return AtlasTypeUtil.toTypeDefHeader(searchTypesDef);
    }

    /**
     * Bulk retrieval API for retrieving all type definitions in Atlas
     *
     * @return A composite wrapper object with lists of all type definitions
     * @throws Exception
     * @HTTP 200 {@link AtlasTypesDef} with type definitions matching the search criteria or else returns empty list of type definitions
     */
    @GetMapping("/typedefs")
    @Timed
    public AtlasTypesDef getAllTypeDefs(@Context HttpServletRequest httpServletRequest) throws AtlasBaseException {
        SearchFilter searchFilter = getSearchFilter(httpServletRequest);

        AtlasTypesDef typesDef = typeDefStore.searchTypesDef(searchFilter);

        return typesDef;
    }

    /**
     * Get the enum definition by it's name (unique)
     *
     * @param name enum name
     * @return enum definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the enum definition by it's name
     * @HTTP 404 On Failed lookup for the given name
     */
    @GetMapping("/enumdef/name/{name}")
    @Timed
    public AtlasEnumDef getEnumDefByName(@PathVariable("name") String name) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", name);

        AtlasEnumDef ret = typeDefStore.getEnumDefByName(name);

        return ret;
    }

    /**
     * Get the enum definition for the given guid
     *
     * @param guid enum guid
     * @return enum definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the enum definition by it's guid
     * @HTTP 404 On Failed lookup for the given guid
     */
    @GetMapping("/enumdef/guid/{guid}")
    @Timed
    public AtlasEnumDef getEnumDefByGuid(@PathVariable("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasEnumDef ret = typeDefStore.getEnumDefByGuid(guid);

        return ret;
    }


    /**
     * Get the struct definition by it's name (unique)
     *
     * @param name struct name
     * @return struct definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the struct definition by it's name
     * @HTTP 404 On Failed lookup for the given name
     */
    @GetMapping("/structdef/name/{name}")
    @Timed
    public AtlasStructDef getStructDefByName(@PathVariable("name") String name) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", name);

        AtlasStructDef ret = typeDefStore.getStructDefByName(name);

        return ret;
    }

    /**
     * Get the struct definition for the given guid
     *
     * @param guid struct guid
     * @return struct definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the struct definition by it's guid
     * @HTTP 404 On Failed lookup for the given guid
     */
    @GetMapping("/structdef/guid/{guid}")
    @Timed
    public AtlasStructDef getStructDefByGuid(@PathVariable("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasStructDef ret = typeDefStore.getStructDefByGuid(guid);

        return ret;
    }

    /**
     * Get the classification definition by it's name (unique)
     *
     * @param name classification name
     * @return classification definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the classification definition by it's name
     * @HTTP 404 On Failed lookup for the given name
     */
    @GetMapping("/classificationdef/name/{name}")
    @Timed
    public AtlasClassificationDef getClassificationDefByName(@PathVariable("name") String name) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", name);

        AtlasClassificationDef ret = typeDefStore.getClassificationDefByName(name);

        return ret;
    }

    /**
     * Get the classification definition for the given guid
     *
     * @param guid classification guid
     * @return classification definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the classification definition by it's guid
     * @HTTP 404 On Failed lookup for the given guid
     */
    @GetMapping("/classificationdef/guid/{guid}")
    @Timed
    public AtlasClassificationDef getClassificationDefByGuid(@PathVariable String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasClassificationDef ret = typeDefStore.getClassificationDefByGuid(guid);

        return ret;
    }

    /**
     * Get the entity definition by it's name (unique)
     *
     * @param name entity name
     * @return Entity definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the entity definition by it's name
     * @HTTP 404 On Failed lookup for the given name
     */
    @GetMapping("/entitydef/name/{name}")
    @Timed
    public AtlasEntityDef getEntityDefByName(@PathVariable("name") String name) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", name);

        AtlasEntityDef ret = typeDefStore.getEntityDefByName(name);

        return ret;
    }

    /**
     * Get the Entity definition for the given guid
     *
     * @param guid entity guid
     * @return Entity definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the entity definition by it's guid
     * @HTTP 404 On Failed lookup for the given guid
     */
    @GetMapping("/entitydef/guid/{guid}")
    @Timed
    public AtlasEntityDef getEntityDefByGuid(@PathVariable String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasEntityDef ret = typeDefStore.getEntityDefByGuid(guid);

        return ret;
    }

    /**
     * Get the relationship definition by it's name (unique)
     *
     * @param name relationship name
     * @return relationship definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the relationship definition by it's name
     * @HTTP 404 On Failed lookup for the given name
     */
    @GetMapping("/relationshipdef/name/{name}")
    @Timed
    public AtlasRelationshipDef getRelationshipDefByName(@PathVariable("name") String name) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", name);

        AtlasRelationshipDef ret = typeDefStore.getRelationshipDefByName(name);

        return ret;
    }

    /**
     * Get the relationship definition for the given guid
     *
     * @param guid relationship guid
     * @return relationship definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the relationship definition by it's guid
     * @HTTP 404 On Failed lookup for the given guid
     */
    @GetMapping("/relationshipdef/guid/{guid}")
    @Timed
    public AtlasRelationshipDef getRelationshipDefByGuid(@PathVariable String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasRelationshipDef ret = typeDefStore.getRelationshipDefByGuid(guid);

        return ret;
    }

    /**
     * Get the businessMetadata definition for the given guid
     *
     * @param guid businessMetadata guid
     * @return businessMetadata definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the businessMetadata definition by it's guid
     * @HTTP 404 On Failed lookup for the given guid
     */
    @GetMapping("/businessmetadatadef/guid/{guid}")
    @Timed
    public AtlasBusinessMetadataDef getBusinessMetadataDefByGuid(@PathVariable String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasBusinessMetadataDef ret = typeDefStore.getBusinessMetadataDefByGuid(guid);

        return ret;
    }

    /**
     * Get the businessMetadata definition by it's name (unique)
     *
     * @param name businessMetadata name
     * @return businessMetadata definition
     * @throws AtlasBaseException
     * @HTTP 200 On successful lookup of the the businessMetadata definition by it's name
     * @HTTP 404 On Failed lookup for the given name
     */
    @GetMapping("/businessmetadatadef/name/{name}")
    @Timed
    public AtlasBusinessMetadataDef getBusinessMetadataDefByName(@PathVariable("name") String name) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", name);

        AtlasBusinessMetadataDef ret = typeDefStore.getBusinessMetadataDefByName(name);

        return ret;
    }

    /* Bulk API operation */

    /**
     * Bulk create APIs for all atlas type definitions, only new definitions will be created.
     * Any changes to the existing definitions will be discarded
     *
     * @param typesDef A composite wrapper object with corresponding lists of the type definition
     * @return A composite wrapper object with lists of type definitions that were successfully
     * created
     * @throws Exception
     * @HTTP 200 On successful update of requested type definitions
     * @HTTP 400 On validation failure for any type definitions
     */
    @PostMapping("/typedefs")
    //@Timed
    public AtlasTypesDef createAtlasTypeDefs(@RequestBody AtlasTypesDef typesDef) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "TypesREST.createAtlasTypeDefs(" +
                        AtlasTypeUtil.toDebugString(typesDef) + ")");
            }

            return typeDefStore.createTypesDef(typesDef);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Bulk update API for all types, changes detected in the type definitions would be persisted
     *
     * @param typesDef A composite object that captures all type definition changes
     * @return A composite object with lists of type definitions that were updated
     * @throws Exception
     * @HTTP 200 On successful update of requested type definitions
     * @HTTP 400 On validation failure for any type definitions
     */
    @PutMapping("/typedefs")
    @Experimental
    @Timed
    public AtlasTypesDef updateAtlasTypeDefs(@RequestBody AtlasTypesDef typesDef) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "TypesREST.updateAtlasTypeDefs(" +
                        AtlasTypeUtil.toDebugString(typesDef) + ")");
            }

            return typeDefStore.updateTypesDef(typesDef);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Bulk delete API for all types
     *
     * @param typesDef A composite object that captures all types to be deleted
     * @throws Exception
     * @HTTP 204 On successful deletion of the requested type definitions
     * @HTTP 400 On validation failure for any type definitions
     */
    @DeleteMapping("/typedefs")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void deleteAtlasTypeDefs(@RequestBody AtlasTypesDef typesDef) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "TypesREST.deleteAtlasTypeDefs(" +
                        AtlasTypeUtil.toDebugString(typesDef) + ")");
            }


            typeDefStore.deleteTypesDef(typesDef);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Delete API for type identified by its name.
     *
     * @param typeName Name of the type to be deleted.
     * @throws AtlasBaseException
     * @HTTP 204 On successful deletion of the requested type definitions
     * @HTTP 400 On validation failure for any type definitions
     */
    @DeleteMapping("/typedef/name/{typeName}")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void deleteAtlasTypeByName(@PathVariable("typeName") String typeName) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "TypesREST.deleteAtlasTypeByName(" + typeName + ")");
            }

            typeDefStore.deleteTypeByName(typeName);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Populate a SearchFilter on the basis of the Query Parameters
     *
     * @return
     */
    private SearchFilter getSearchFilter(HttpServletRequest httpServletRequest) {
        SearchFilter ret = new SearchFilter();
        Set<String> keySet = httpServletRequest.getParameterMap().keySet();

        for (String k : keySet) {
            String key = String.valueOf(k);
            String value = String.valueOf(httpServletRequest.getParameter(k));

            if (key.equalsIgnoreCase("excludeInternalTypesAndReferences") && value.equalsIgnoreCase("true")) {
                FilterUtil.addParamsToHideInternalType(ret);
            } else {
                ret.setParam(key, value);
            }
        }

        return ret;
    }
}
