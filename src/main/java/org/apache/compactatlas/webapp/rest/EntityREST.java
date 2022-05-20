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

import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;
import org.apache.compactatlas.intg.AtlasErrorCode;
import org.apache.compactatlas.serverapi.RequestContext;
import org.apache.compactatlas.common.annotation.Timed;
import org.apache.compactatlas.intg.bulkimport.BulkImportResponse;
import org.apache.compactatlas.intg.exception.AtlasBaseException;
import org.apache.compactatlas.intg.model.TypeCategory;
import org.apache.compactatlas.intg.model.audit.EntityAuditEventV2;
import org.apache.compactatlas.intg.model.instance.*;
import org.apache.compactatlas.intg.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.compactatlas.intg.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.compactatlas.intg.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.compactatlas.repository.repository.converters.AtlasInstanceConverter;
import org.apache.compactatlas.repository.repository.store.graph.AtlasEntityStore;
import org.apache.compactatlas.repository.repository.store.graph.v2.AtlasEntityStream;
import org.apache.compactatlas.repository.repository.store.graph.v2.ClassificationAssociator;
import org.apache.compactatlas.repository.repository.store.graph.v2.EntityStream;
import org.apache.compactatlas.intg.type.AtlasClassificationType;
import org.apache.compactatlas.intg.type.AtlasEntityType;
import org.apache.compactatlas.intg.type.AtlasTypeRegistry;
import org.apache.compactatlas.repository.util.FileUtils;
import org.apache.compactatlas.webapp.util.Servlets;
import org.apache.compactatlas.common.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;


/**
 * REST for a single entity
 */
@RequestMapping("api/atlas/v2/entity")
@RestController
public class EntityREST {
    private static final Logger LOG = LoggerFactory.getLogger(EntityREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.EntityREST");

    public static final String PREFIX_ATTR = "attr:";
    public static final String PREFIX_ATTR_ = "attr_";


    private final AtlasTypeRegistry typeRegistry;
    private final AtlasEntityStore entitiesStore;
    //private final EntityAuditRepository  auditRepository;
    private final AtlasInstanceConverter instanceConverter;

    @Autowired
    public EntityREST(AtlasTypeRegistry typeRegistry, AtlasEntityStore entitiesStore,
                      AtlasInstanceConverter instanceConverter) {
        //EntityAuditRepository auditRepository,
        this.typeRegistry = typeRegistry;
        this.entitiesStore = entitiesStore;
        //this.auditRepository   = auditRepository;
        this.instanceConverter = instanceConverter;
    }

    /**
     * Fetch complete definition of an entity given its GUID.
     *
     * @param guid GUID for the entity
     * @return AtlasEntity
     * @throws AtlasBaseException
     */
    @GetMapping("/guid/{guid}")
    @Timed
    public AtlasEntityWithExtInfo getById(@PathVariable String guid, @RequestParam(defaultValue = "false") boolean minExtInfo, @RequestParam(defaultValue = "false") boolean ignoreRelationships) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getById(" + guid + ", " + minExtInfo + " )");
            }

            return entitiesStore.getById(guid, minExtInfo, ignoreRelationships);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Get entity header given its GUID.
     *
     * @param guid GUID for the entity
     * @return AtlasEntity
     * @throws AtlasBaseException
     */
    @GetMapping("/guid/{guid}/header")
    @Timed
    public AtlasEntityHeader getHeaderById(@PathVariable String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getHeaderById(" + guid + ")");
            }

            return entitiesStore.getHeaderById(guid);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Fetch AtlasEntityHeader given its type and unique attribute.
     * <p>
     * In addition to the typeName path parameter, attribute key-value pair(s) can be provided in the following format
     * <p>
     * attr:<attrName>=<attrValue>
     * <p>
     * NOTE: The attrName and attrValue should be unique across entities, eg. qualifiedName
     * <p>
     * The REST request would look something like this
     * <p>
     * GET /v2/entity/uniqueAttribute/type/aType/header?attr:aTypeAttribute=someValue
     *
     * @param typeName
     * @return AtlasEntityHeader
     * @throws AtlasBaseException
     */
    @GetMapping("/uniqueAttribute/type/{typeName}/header")
    @Timed
    public AtlasEntityHeader getEntityHeaderByUniqueAttributes(@PathVariable String typeName,
                                                               @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            Map<String, Object> attributes = getAttributes(servletRequest);
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getEntityHeaderByUniqueAttributes(" + typeName + "," + attributes + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);

            validateUniqueAttribute(entityType, attributes);

            return entitiesStore.getEntityHeaderByUniqueAttributes(entityType, attributes);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Fetch complete definition of an entity given its type and unique attribute.
     * <p>
     * In addition to the typeName path parameter, attribute key-value pair(s) can be provided in the following format
     * <p>
     * attr:<attrName>=<attrValue>
     * <p>
     * NOTE: The attrName and attrValue should be unique across entities, eg. qualifiedName
     * <p>
     * The REST request would look something like this
     * <p>
     * GET /v2/entity/uniqueAttribute/type/aType?attr:aTypeAttribute=someValue
     *
     * @param typeName
     * @param minExtInfo
     * @param ignoreRelationships
     * @return AtlasEntityWithExtInfo
     * @throws AtlasBaseException
     */
    @GetMapping("/uniqueAttribute/type/{typeName}")
    @Timed
    public AtlasEntityWithExtInfo getByUniqueAttributes(@PathVariable String typeName, @RequestParam(defaultValue = "false") boolean minExtInfo,
                                                        @RequestParam(defaultValue = "false") boolean ignoreRelationships, @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            Map<String, Object> attributes = getAttributes(servletRequest);

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getByUniqueAttributes(" + typeName + "," + attributes + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);

            validateUniqueAttribute(entityType, attributes);

            return entitiesStore.getByUniqueAttributes(entityType, attributes, minExtInfo, ignoreRelationships);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            //e.printStackTrace();
        } finally {
            AtlasPerfTracer.log(perf);
        }
        return new AtlasEntityWithExtInfo();
    }

    /*******
     * Entity Partial Update - Allows a subset of attributes to be updated on
     * an entity which is identified by its type and unique attribute  eg: Referenceable.qualifiedName.
     * Null updates are not possible
     *
     * In addition to the typeName path parameter, attribute key-value pair(s) can be provided in the following format
     *
     * attr:<attrName>=<attrValue>
     *
     * NOTE: The attrName and attrValue should be unique across entities, eg. qualifiedName
     *
     * The REST request would look something like this
     *
     * PUT /v2/entity/uniqueAttribute/type/aType?attr:aTypeAttribute=someValue
     *
     *******/
    @PutMapping("/uniqueAttribute/type/{typeName}")
    @Timed
    public EntityMutationResponse partialUpdateEntityByUniqueAttrs(@PathVariable String typeName,
                                                                   @Context HttpServletRequest servletRequest,
                                                                   @RequestBody AtlasEntityWithExtInfo entityInfo) throws Exception {
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            Map<String, Object> uniqueAttributes = getAttributes(servletRequest);

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.partialUpdateEntityByUniqueAttrs(" + typeName + "," + uniqueAttributes + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);

            validateUniqueAttribute(entityType, uniqueAttributes);

            return entitiesStore.updateByUniqueAttributes(entityType, uniqueAttributes, entityInfo);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Delete an entity identified by its type and unique attributes.
     * <p>
     * In addition to the typeName path parameter, attribute key-value pair(s) can be provided in the following format
     * <p>
     * attr:<attrName>=<attrValue>
     * <p>
     * NOTE: The attrName and attrValue should be unique across entities, eg. qualifiedName
     * <p>
     * The REST request would look something like this
     * <p>
     * DELETE /v2/entity/uniqueAttribute/type/aType?attr:aTypeAttribute=someValue
     *
     * @param typeName       - entity type to be deleted
     * @param servletRequest - request containing unique attributes/values
     * @return EntityMutationResponse
     */
    @DeleteMapping("/uniqueAttribute/type/{typeName}")
    public EntityMutationResponse deleteByUniqueAttribute(@PathVariable String typeName,
                                                          @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            Map<String, Object> attributes = getAttributes(servletRequest);

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.deleteByUniqueAttribute(" + typeName + "," + attributes + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);

            return entitiesStore.deleteByUniqueAttributes(entityType, attributes);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Create new entity or update existing entity in Atlas.
     * Existing entity is matched using its unique guid if supplied or by its unique attributes eg: qualifiedName
     *
     * @param entity
     * @return EntityMutationResponse
     * @throws AtlasBaseException
     */
    @PostMapping("")
    @Timed
    public EntityMutationResponse createOrUpdate(@RequestBody AtlasEntityWithExtInfo entity) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.createOrUpdate()");
            }

            return entitiesStore.createOrUpdate(new AtlasEntityStream(entity), false);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /*******
     * Entity Partial Update - Add/Update entity attribute identified by its GUID.
     * Supports only uprimitive attribute type and entity references.
     * does not support updation of complex types like arrays, maps
     * Null updates are not possible
     *******/
    @PutMapping("/guid/{guid}")
    @Timed
    public EntityMutationResponse partialUpdateEntityAttrByGuid(@PathVariable String guid,
                                                                @RequestParam("name") String attrName,
                                                                @RequestBody Object attrValue) throws Exception {
        Servlets.validateQueryParamLength("guid", guid);
        Servlets.validateQueryParamLength("name", attrName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.partialUpdateEntityAttrByGuid(" + guid + "," + attrName + ")");
            }

            return entitiesStore.updateEntityAttributeByGuid(guid, attrName, attrValue);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Delete an entity identified by its GUID.
     *
     * @param guid GUID for the entity
     * @return EntityMutationResponse
     */
    @DeleteMapping("/guid/{guid}")
    @Timed
    public EntityMutationResponse deleteByGuid(@PathVariable String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.deleteByGuid(" + guid + ")");
            }

            return entitiesStore.deleteById(guid);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Gets the list of classifications for a given entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     * @return classification for the given entity guid
     */
    @GetMapping("/guid/{guid}/classification/{classificationName}")
    @Timed
    public AtlasClassification getClassification(@PathVariable String guid, @RequestParam("classificationName") final String classificationName) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);
        Servlets.validateQueryParamLength("classificationName", classificationName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getClassification(" + guid + "," + classificationName + ")");
            }

            if (StringUtils.isEmpty(guid)) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            ensureClassificationType(classificationName);
            return entitiesStore.getClassification(guid, classificationName);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Gets the list of classifications for a given entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     * @return a list of classifications for the given entity guid
     */
    @GetMapping("/guid/{guid}/classifications")
    @Timed
    public AtlasClassification.AtlasClassifications getClassifications(@PathVariable String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getClassifications(" + guid + ")");
            }

            if (StringUtils.isEmpty(guid)) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            return new AtlasClassification.AtlasClassifications(entitiesStore.getClassifications(guid));
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Adds classification to the entity identified by its type and unique attributes.
     *
     * @param typeName
     */
    @PostMapping("/uniqueAttribute/type/{typeName}/classifications")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void addClassificationsByUniqueAttribute(@PathVariable String typeName, @Context HttpServletRequest servletRequest, @RequestBody List<AtlasClassification> classifications) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addClassificationsByUniqueAttribute(" + typeName + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);
            Map<String, Object> attributes = getAttributes(servletRequest);
            String guid = entitiesStore.getGuidByUniqueAttributes(entityType, attributes);

            if (guid == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, typeName, attributes.toString());
            }

            entitiesStore.addClassifications(guid, classifications);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Adds classifications to an existing entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     */
    @PostMapping("/guid/{guid}/classifications")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void addClassifications(@PathVariable String guid, @RequestBody List<AtlasClassification> classifications) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addClassifications(" + guid + ")");
            }

            if (StringUtils.isEmpty(guid)) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            entitiesStore.addClassifications(guid, classifications);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
        }  finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @PostMapping("/guid/{guid}/classification")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void addClassification(@PathVariable String guid, @RequestBody AtlasClassification classification) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addClassifications(" + guid + ")");
            }

            if (StringUtils.isEmpty(guid)) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }
            List<AtlasClassification> claList = new LinkedList<>();
            claList.add(classification);
            entitiesStore.addClassifications(guid, claList);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Updates classification on an entity identified by its type and unique attributes.
     *
     * @param typeName
     */
    @PutMapping("/uniqueAttribute/type/{typeName}/classifications")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void updateClassificationsByUniqueAttribute(@PathVariable String typeName, @Context HttpServletRequest servletRequest, @RequestBody List<AtlasClassification> classifications) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.updateClassificationsByUniqueAttribute(" + typeName + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);
            Map<String, Object> attributes = getAttributes(servletRequest);
            String guid = entitiesStore.getGuidByUniqueAttributes(entityType, attributes);

            if (guid == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, typeName, attributes.toString());
            }

            entitiesStore.updateClassifications(guid, classifications);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
        }  finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Updates classifications to an existing entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     * @return classification for the given entity guid
     */
    @PutMapping("/guid/{guid}/classifications")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void updateClassifications(@PathVariable String guid, @RequestBody List<AtlasClassification> classifications) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.updateClassifications(" + guid + ")");
            }

            if (StringUtils.isEmpty(guid)) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            entitiesStore.updateClassifications(guid, classifications);

        } catch (Exception e) {
            LOG.warn(e.getMessage());
        }  finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Deletes a given classification from an entity identified by its type and unique attributes.
     *
     * @param typeName
     * @param classificationName name of the classification
     */
    @DeleteMapping("/uniqueAttribute/type/{typeName}/classification/{classificationName}")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void deleteClassificationByUniqueAttribute(@PathVariable String typeName, @Context HttpServletRequest servletRequest, @PathVariable String classificationName) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);
        Servlets.validateQueryParamLength("classificationName", classificationName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.deleteClassificationByUniqueAttribute(" + typeName + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);
            Map<String, Object> attributes = getAttributes(servletRequest);
            String guid = entitiesStore.getGuidByUniqueAttributes(entityType, attributes);

            if (guid == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, typeName, attributes.toString());
            }

            entitiesStore.deleteClassification(guid, classificationName);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
        }  finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Deletes a given classification from an existing entity represented by a guid.
     *
     * @param guid               globally unique identifier for the entity
     * @param classificationName name of the classifcation
     */
    @DeleteMapping("/guid/{guid}/classification/{classificationName}")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void deleteClassification(@PathVariable String guid,
                                     @PathVariable String classificationName,
                                     @RequestParam(required = false) String associatedEntityGuid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);
        Servlets.validateQueryParamLength("classificationName", classificationName);
        //Servlets.validateQueryParamLength("associatedEntityGuid", associatedEntityGuid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.deleteClassification(" + guid + "," + classificationName + "," + associatedEntityGuid + ")");
            }

            if (StringUtils.isEmpty(guid)) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
            }

            ensureClassificationType(classificationName);

            entitiesStore.deleteClassification(guid, classificationName, associatedEntityGuid);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /******************************************************************/
    /** Bulk API operations                                          **/
    /******************************************************************/

    /**
     * Bulk API to retrieve list of entities identified by its unique attributes.
     * <p>
     * In addition to the typeName path parameter, attribute key-value pair(s) can be provided in the following format
     * <p>
     * typeName=<typeName>&attr_1:<attrName>=<attrValue>&attr_2:<attrName>=<attrValue>&attr_3:<attrName>=<attrValue>
     * <p>
     * NOTE: The attrName should be an unique attribute for the given entity-type
     * <p>
     * The REST request would look something like this
     * <p>
     * GET /v2/entity/bulk/uniqueAttribute/type/hive_db?attr_0:qualifiedName=db1@cl1&attr_2:qualifiedName=db2@cl1
     *
     * @param typeName
     * @param minExtInfo
     * @param ignoreRelationships
     * @return AtlasEntitiesWithExtInfo
     * @throws AtlasBaseException
     */
    @GetMapping("/bulk/uniqueAttribute/type/{typeName}")
    public AtlasEntitiesWithExtInfo getEntitiesByUniqueAttributes(@PathVariable String typeName,
                                                                  @RequestParam(defaultValue = "false") boolean minExtInfo,
                                                                  @RequestParam(defaultValue = "false") boolean ignoreRelationships,
                                                                  @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            List<Map<String, Object>> uniqAttributesList = getAttributesList(servletRequest);

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getEntitiesByUniqueAttributes(" + typeName + "," + uniqAttributesList + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);

            for (Map<String, Object> uniqAttributes : uniqAttributesList) {
                validateUniqueAttribute(entityType, uniqAttributes);
            }

            return entitiesStore.getEntitiesByUniqueAttributes(entityType, uniqAttributesList, minExtInfo, ignoreRelationships);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Bulk API to retrieve list of entities identified by its GUIDs.
     */
    @GetMapping("/bulk")
    public AtlasEntitiesWithExtInfo getByGuids(@RequestParam("guid") List<String> guids, @RequestParam(defaultValue = "false") boolean minExtInfo, @RequestParam(defaultValue = "false") boolean ignoreRelationships) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(guids)) {
            for (String guid : guids) {
                Servlets.validateQueryParamLength("guid", guid);
            }
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getByGuids(" + guids + ")");
            }

            if (CollectionUtils.isEmpty(guids)) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guids);
            }

            return entitiesStore.getByIds(guids, minExtInfo, ignoreRelationships);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Bulk API to create new entities or updates existing entities in Atlas.
     * Existing entity is matched using its unique guid if supplied or by its unique attributes eg: qualifiedName
     */
    @PostMapping("/bulk")
    public EntityMutationResponse createOrUpdate(@RequestBody AtlasEntitiesWithExtInfo entities) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.createOrUpdate(entityCount=" +
                        (CollectionUtils.isEmpty(entities.getEntities()) ? 0 : entities.getEntities().size()) + ")");
            }

            EntityStream entityStream = new AtlasEntityStream(entities);

            return entitiesStore.createOrUpdate(entityStream, false);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Bulk API to delete list of entities identified by its GUIDs
     */
    @DeleteMapping("/bulk")
    @Timed
    public EntityMutationResponse deleteByGuids(@RequestParam("guid") final List<String> guids) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(guids)) {
            for (String guid : guids) {
                Servlets.validateQueryParamLength("guid", guid);
            }
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.deleteByGuids(" + guids + ")");
            }

            return entitiesStore.deleteByIds(guids);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Bulk API to associate a tag to multiple entities.
     * Option 1: List of GUIDs to associate a tag
     * Option 2: Typename and list of uniq attributes for entities to associate a tag
     * Option 3: List of GUIDs and Typename with list of uniq attributes for entities to associate a tag
     */
    @PostMapping("/bulk/classification")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void addClassification(@RequestBody ClassificationAssociateRequest request) throws AtlasBaseException {
        try {
            AtlasClassification classification = request == null ? null : request.getClassification();
            List<String> entityGuids = request == null ? null : request.getEntityGuids();
            List<Map<String, Object>> entitiesUniqueAttributes = request == null ? null : request.getEntitiesUniqueAttributes();
            String entityTypeName = request == null ? null : request.getEntityTypeName();

            if (classification == null || StringUtils.isEmpty(classification.getTypeName())) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "no classification");
            }

            if (hasNoGUIDAndTypeNameAttributes(request)) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Need either list of GUIDs or entity type and list of qualified Names");
            }

            if (CollectionUtils.isNotEmpty(entitiesUniqueAttributes) && entityTypeName != null) {
                AtlasEntityType entityType = ensureEntityType(entityTypeName);

                if (CollectionUtils.isEmpty(entityGuids)) {
                    entityGuids = new ArrayList<>();
                }

                for (Map<String, Object> eachEntityAttributes : entitiesUniqueAttributes) {
                    try {
                        String guid = entitiesStore.getGuidByUniqueAttributes(entityType, eachEntityAttributes);

                        if (guid != null) {
                            entityGuids.add(guid);
                        }
                    } catch (AtlasBaseException e) {
                        if (RequestContext.get().isSkipFailedEntities()) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("getByIds(): ignoring failure for entity with unique attributes {} and typeName {}: error code={}, message={}", eachEntityAttributes, entityTypeName, e.getAtlasErrorCode(), e.getMessage());
                            }

                            continue;
                        }

                        throw e;
                    }
                }

                if (CollectionUtils.isEmpty(entityGuids)) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "No guid found for given entity Type Name and list of attributes");
                }
            }
            entitiesStore.addClassification(entityGuids, classification);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
        }
    }

    @GetMapping("{guid}/audit")
    @Timed
    public List<EntityAuditEventV2> getAuditEvents(@PathVariable String guid, @RequestParam(value = "startKey", required = false) String startKey,
                                                   @RequestParam(value = "auditAction", required = false) EntityAuditEventV2.EntityAuditActionV2 auditAction,
                                                   @RequestParam("count") @DefaultValue("100") short count,
                                                   @RequestParam("offset") @DefaultValue("-1") int offset,
                                                   @RequestParam("sortBy") String sortBy,
                                                   @RequestParam("sortOrder") String sortOrder) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getAuditEvents(" + guid + ", " + startKey + ", " + count + ")");
            }

            // Enforces authorization for entity-read
//            try {
//                entitiesStore.getHeaderById(guid);
//            } catch (AtlasBaseException e) {
//                if (e.getAtlasErrorCode() == AtlasErrorCode.INSTANCE_GUID_NOT_FOUND) {
//                    AtlasEntityHeader entityHeader = getEntityHeaderFromPurgedAudit(guid);
//
//                    AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, entityHeader), "read entity audit: guid=", guid);
//                } else {
//                    throw e;
//                }
//            }
//
//            List<EntityAuditEventV2> ret = new ArrayList<>();

//            if (sortBy != null || offset > -1) {
//                ret = auditRepository.listEventsV2(guid, auditAction, sortBy, StringUtils.equalsIgnoreCase(sortOrder, "desc"), offset, count);
//            } else if(auditAction != null) {
//                ret = auditRepository.listEventsV2(guid, auditAction, startKey, count);
//            } else {
//                List events = auditRepository.listEvents(guid, startKey, count);
//
//                for (Object event : events) {
//                    if (event instanceof EntityAuditEventV2) {
//                        ret.add((EntityAuditEventV2) event);
//                    } else if (event instanceof EntityAuditEvent) {
//                        ret.add(instanceConverter.toV2AuditEvent((EntityAuditEvent) event));
//                    } else {
//                        LOG.warn("unknown entity-audit event type {}. Ignored", event != null ? event.getClass().getCanonicalName() : "null");
//                    }
//                }
//            }

            return new ArrayList<EntityAuditEventV2>();
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @GetMapping("/bulk/headers")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @Timed
    public AtlasEntityHeaders getEntityHeaders(@RequestParam("tagUpdateStartTime") long tagUpdateStartTime) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            long tagUpdateEndTime = System.currentTimeMillis();

            if (tagUpdateStartTime > tagUpdateEndTime) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "fromTimestamp should be less than toTimestamp");
            }

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.getEntityHeaders(" + tagUpdateStartTime + ", " + tagUpdateEndTime + ")");
            }
//auditRepository
            ClassificationAssociator.Retriever associator = new ClassificationAssociator.Retriever(typeRegistry, null);
            return associator.get(tagUpdateStartTime, tagUpdateEndTime);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @PostMapping("/bulk/setClassifications")
    //@Produces(Servlets.JSON_MEDIA_TYPE)
    //@Consumes(Servlets.JSON_MEDIA_TYPE)
    //@Timed
    public String setClassifications(@RequestBody AtlasEntityHeaders entityHeaders) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.setClassifications()");
            }

            ClassificationAssociator.Updater associator = new ClassificationAssociator.Updater(typeRegistry, entitiesStore);
            return associator.setClassifications(entityHeaders.getGuidHeaderMap());
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @PostMapping("/guid/{guid}/businessmetadata")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void addOrUpdateBusinessAttributes(@PathVariable String guid, @RequestParam(defaultValue = "false") boolean isOverwrite, @RequestBody Map<String, Map<String, Object>> businessAttributes) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addOrUpdateBusinessAttributes(" + guid + ", isOverwrite=" + isOverwrite + ")");
            }

            entitiesStore.addOrUpdateBusinessAttributes(guid, businessAttributes, isOverwrite);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @DeleteMapping("/guid/{guid}/businessmetadata")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void removeBusinessAttributes(@PathVariable String guid, @RequestBody Map<String, Map<String, Object>> businessAttributes) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.removeBusinessAttributes(" + guid + ")");
            }

            entitiesStore.removeBusinessAttributes(guid, businessAttributes);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @PostMapping("/guid/{guid}/businessmetadata/{bmName}")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void addOrUpdateBusinessAttributes(@PathVariable String guid, @RequestParam("bmName") final String bmName, @RequestBody Map<String, Object> businessAttributes) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addOrUpdateBusinessAttributes(" + guid + ", " + bmName + ")");
            }

            entitiesStore.addOrUpdateBusinessAttributes(guid, Collections.singletonMap(bmName, businessAttributes), false);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @DeleteMapping("/guid/{guid}/businessmetadata/{bmName}")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void removeBusinessAttributes(@PathVariable String guid, @PathVariable String bmName, @RequestBody Map<String, Object> businessAttributes) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.removeBusinessAttributes(" + guid + ", " + bmName + ")");
            }

            entitiesStore.removeBusinessAttributes(guid, Collections.singletonMap(bmName, businessAttributes));
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Set labels to a given entity
     *
     * @param guid   - Unique entity identifier
     * @param labels - set of labels to be set to the entity
     * @throws AtlasBaseException
     */
    @PostMapping("/guid/{guid}/labels")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void setLabels(@PathVariable String guid, @RequestBody Set<String> labels) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.setLabels()");
            }

            entitiesStore.setLabels(guid, labels);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * delete given labels to a given entity
     *
     * @param guid - Unique entity identifier
     * @throws AtlasBaseException
     */
    @DeleteMapping("/guid/{guid}/labels")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void removeLabels(@PathVariable String guid, @RequestBody Set<String> labels) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.deleteLabels()");
            }

            entitiesStore.removeLabels(guid, labels);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * add given labels to a given entity
     *
     * @param guid - Unique entity identifier
     * @throws AtlasBaseException
     */
    @PutMapping("/guid/{guid}/labels")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void addLabels(@PathVariable String guid, @RequestBody Set<String> labels) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addLabels()");
            }

            entitiesStore.addLabels(guid, labels);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @PostMapping("/uniqueAttribute/type/{typeName}/labels")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void setLabels(@PathVariable String typeName, @RequestBody Set<String> labels,
                          @Context HttpServletRequest servletRequest) throws AtlasBaseException {

        Servlets.validateQueryParamLength("typeName", typeName);
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.setLabels(" + typeName + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);
            Map<String, Object> attributes = getAttributes(servletRequest);
            String guid = entitiesStore.getGuidByUniqueAttributes(entityType, attributes);

            if (guid == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, typeName, attributes.toString());
            }

            entitiesStore.setLabels(guid, labels);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @PutMapping("/uniqueAttribute/type/{typeName}/labels")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void addLabels(@PathVariable String typeName, @RequestBody Set<String> labels,
                          @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.addLabels(" + typeName + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);
            Map<String, Object> attributes = getAttributes(servletRequest);
            String guid = entitiesStore.getGuidByUniqueAttributes(entityType, attributes);

            if (guid == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, typeName, attributes.toString());
            }

            entitiesStore.addLabels(guid, labels);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @DeleteMapping("/uniqueAttribute/type/{typeName}/labels")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void removeLabels(@PathVariable String typeName, @RequestBody Set<String> labels,
                             @Context HttpServletRequest servletRequest) throws AtlasBaseException {

        Servlets.validateQueryParamLength("typeName", typeName);
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityREST.removeLabels(" + typeName + ")");
            }

            AtlasEntityType entityType = ensureEntityType(typeName);
            Map<String, Object> attributes = getAttributes(servletRequest);
            String guid = entitiesStore.getGuidByUniqueAttributes(entityType, attributes);

            if (guid == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, typeName, attributes.toString());
            }

            entitiesStore.removeLabels(guid, labels);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private AtlasEntityType ensureEntityType(String typeName) throws AtlasBaseException {
        AtlasEntityType ret = typeRegistry.getEntityTypeByName(typeName);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), typeName);
        }

        return ret;
    }

    private AtlasClassificationType ensureClassificationType(String typeName) throws AtlasBaseException {
        AtlasClassificationType ret = typeRegistry.getClassificationTypeByName(typeName);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.CLASSIFICATION.name(), typeName);
        }

        return ret;
    }

    // attr:qualifiedName=db1@cl1 ==> { qualifiedName:db1@cl1 }
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

    // attr_1:qualifiedName=db1@cl1&attr_2:qualifiedName=db2@cl1 ==> [ { qualifiedName:db1@cl1 }, { qualifiedName:db2@cl1 } ]
    private List<Map<String, Object>> getAttributesList(HttpServletRequest request) {
        Map<String, Map<String, Object>> ret = new HashMap<>();

        if (MapUtils.isNotEmpty(request.getParameterMap())) {
            for (Map.Entry<String, String[]> entry : ((Map<String, String[]>) request.getParameterMap()).entrySet()) {
                String key = entry.getKey();

                if (key == null || !key.startsWith(PREFIX_ATTR_)) {
                    continue;
                }

                int sepPos = key.indexOf(':', PREFIX_ATTR_.length());
                String[] values = entry.getValue();
                String value = values != null && values.length > 0 ? values[0] : null;

                if (sepPos == -1 || value == null) {
                    continue;
                }

                String attrName = key.substring(sepPos + 1);
                String listIdx = key.substring(PREFIX_ATTR_.length(), sepPos);
                Map<String, Object> attributes = ret.get(listIdx);

                if (attributes == null) {
                    attributes = new HashMap<>();

                    ret.put(listIdx, attributes);
                }

                attributes.put(attrName, value);
            }
        }

        return new ArrayList<>(ret.values());
    }

    /**
     * Validate that each attribute given is an unique attribute
     *
     * @param entityType the entity type
     * @param attributes attributes
     */
    private void validateUniqueAttribute(AtlasEntityType entityType, Map<String, Object> attributes) throws AtlasBaseException {
        if (MapUtils.isEmpty(attributes)) {
            throw new AtlasBaseException(AtlasErrorCode.ATTRIBUTE_UNIQUE_INVALID, entityType.getTypeName(), "");
        }

        for (String attributeName : attributes.keySet()) {
            AtlasAttributeDef attribute = entityType.getAttributeDef(attributeName);

            if (attribute == null || !attribute.getIsUnique()) {
                throw new AtlasBaseException(AtlasErrorCode.ATTRIBUTE_UNIQUE_INVALID, entityType.getTypeName(), attributeName);
            }
        }
    }

    /**
     * Get the sample Template for uploading/creating bulk BusinessMetaData
     *
     * @return Template File
     * @throws AtlasBaseException
     * @HTTP 400 If the provided fileType is not supported
     */
    @GetMapping("/businessmetadata/import/template")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response produceTemplate() {
        return Response.ok(new StreamingOutput() {
            @Override
            public void write(OutputStream outputStream) throws IOException, WebApplicationException {
                outputStream.write(FileUtils.getBusinessMetadataHeaders().getBytes());
            }
        }).header("Content-Disposition", "attachment; filename=\"template_business_metadata\"").build();
    }

    /**
     * Upload the file for creating Business Metadata in BULK
     *
     * @param uploadedInputStream InputStream of file
     * @param fileDetail          FormDataContentDisposition metadata of file
     * @return
     * @throws AtlasBaseException
     * @HTTP 200 If Business Metadata creation was successful
     * @HTTP 400 If Business Metadata definition has invalid or missing information
     * @HTTP 409 If Business Metadata already exists (duplicate qualifiedName)
     */
    @PostMapping("/businessmetadata/import")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Timed
    public BulkImportResponse importBMAttributes(@FormDataParam("file") InputStream uploadedInputStream,
                                                 @FormDataParam("file") FormDataContentDisposition fileDetail) throws AtlasBaseException {

        return entitiesStore.bulkCreateOrUpdateBusinessAttributes(uploadedInputStream, fileDetail.getFileName());
    }

//    private AtlasEntityHeader getEntityHeaderFromPurgedAudit(String guid) throws AtlasBaseException {
//        List<EntityAuditEventV2> auditEvents = auditRepository.listEventsV2(guid, EntityAuditActionV2.ENTITY_PURGE, null, (short)1);
//        AtlasEntityHeader        ret         = CollectionUtils.isNotEmpty(auditEvents) ? auditEvents.get(0).getEntityHeader() : null;
//
//        if (ret == null) {
//            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
//        }
//
//        return ret;
//    }

    private boolean hasNoGUIDAndTypeNameAttributes(ClassificationAssociateRequest request) {
        return (request == null || (CollectionUtils.isEmpty(request.getEntityGuids()) &&
                (CollectionUtils.isEmpty(request.getEntitiesUniqueAttributes()) || request.getEntityTypeName() == null)));
    }
}
