///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package org.apache.compactatlas.webapp.service;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.hadoop.metrics2.annotation.Metric;
//import org.apache.hadoop.metrics2.annotation.Metrics;
//import org.apache.hadoop.metrics2.lib.MutableRate;
//import org.aspectj.lang.Signature;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.lang.reflect.Field;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Set;
//
//@Metrics(context = DebugMetricsWrapper.Constants.DEBUG_METRICS_CONTEXT)
//public class AtlasDebugMetricsSource {
//    private static final Logger LOG = LoggerFactory.getLogger(AtlasDebugMetricsSource.class);
//
//    protected @Metric(always = true)  MutableRate entityREST_getById;
//    protected @Metric(always = true)  MutableRate entityREST_createOrUpdate;
//    protected @Metric(always = true)  MutableRate entityREST_partialUpdateEntityAttrByGuid;
//    protected @Metric(always = true)  MutableRate entityREST_deleteByGuid;
//    protected @Metric(always = true)  MutableRate entityREST_getClassification;
//    protected @Metric(always = true)  MutableRate entityREST_getClassifications;
//    protected @Metric(always = true)  MutableRate entityREST_addClassificationsByUniqAttr;
//    protected @Metric(always = true)  MutableRate entityREST_addClassifications;
//    protected @Metric(always = true)  MutableRate entityREST_deleteClassificationByUniqAttr;
//    protected @Metric(always = true)  MutableRate entityREST_deleteClassification;
//    protected @Metric(always = true)  MutableRate entityREST_getHeaderById;
//    protected @Metric(always = true)  MutableRate entityREST_getEntityHeaderByUniqAttr;
//    protected @Metric(always = true)  MutableRate entityREST_getByUniqueAttributes;
//    protected @Metric(always = true)  MutableRate entityREST_partialUpdateEntityByUniqAttr;
//    protected @Metric(always = true)  MutableRate entityREST_deleteByUniqAttr;
//    protected @Metric(always = true)  MutableRate entityREST_updateClassificationsByUniqAttr;
//    protected @Metric(always = true)  MutableRate entityREST_updateClassifications;
//    protected @Metric(always = true)  MutableRate entityREST_getEntitiesByUniqAttr;
//    protected @Metric(always = true)  MutableRate entityREST_getByGuids;
//    protected @Metric(always = true)  MutableRate entityREST_createOrUpdateBulk;
//    protected @Metric(always = true)  MutableRate entityREST_deleteByGuids;
//    protected @Metric(always = true)  MutableRate entityREST_addClassification;
//    protected @Metric(always = true)  MutableRate entityREST_getAuditEvents;
//    protected @Metric(always = true)  MutableRate entityREST_getEntityHeaders;
//    protected @Metric(always = true)  MutableRate entityREST_setClassifications;
//    protected @Metric(always = true)  MutableRate entityREST_addOrUpdateBusinessAttributes;
//    protected @Metric(always = true)  MutableRate entityREST_removeBusinessAttributes;
//    protected @Metric(always = true)  MutableRate entityREST_addOrUpdateBusinessAttributesByName;
//    protected @Metric(always = true)  MutableRate entityREST_removeBusinessAttributesByName;
//    protected @Metric(always = true)  MutableRate entityREST_setLabels;
//    protected @Metric(always = true)  MutableRate entityREST_addLabels;
//    protected @Metric(always = true)  MutableRate entityREST_removeLabels;
//    protected @Metric(always = true)  MutableRate entityREST_removeLabelsByTypeName;
//    protected @Metric(always = true)  MutableRate entityREST_setLabelsByTypeName;
//    protected @Metric(always = true)  MutableRate entityREST_addLabelsByTypeName;
//    protected @Metric(always = true)  MutableRate entityREST_importBMAttributes;
//
//    protected @Metric(always = true)  MutableRate typesREST_getEntityDefByName;
//    protected @Metric(always = true)  MutableRate typesREST_getTypeDefByName;
//    protected @Metric(always = true)  MutableRate typesREST_getTypeDefByGuid;
//    protected @Metric(always = true)  MutableRate typesREST_getTypeDefHeaders;
//    protected @Metric(always = true)  MutableRate typesREST_getAllTypeDefs;
//    protected @Metric(always = true)  MutableRate typesREST_getEnumDefByName;
//    protected @Metric(always = true)  MutableRate typesREST_getEnumDefByGuid;
//    protected @Metric(always = true)  MutableRate typesREST_getStructDefByName;
//    protected @Metric(always = true)  MutableRate typesREST_getStructDefByGuid;
//    protected @Metric(always = true)  MutableRate typesREST_getClassificationDefByName;
//    protected @Metric(always = true)  MutableRate typesREST_getClassificationDefByGuid;
//    protected @Metric(always = true)  MutableRate typesREST_getEntityDefByGuid;
//    protected @Metric(always = true)  MutableRate typesREST_getRelationshipDefByName;
//    protected @Metric(always = true)  MutableRate typesREST_getRelationshipDefByGuid;
//    protected @Metric(always = true)  MutableRate typesREST_getBusinessMetadataDefByGuid;
//    protected @Metric(always = true)  MutableRate typesREST_getBusinessMetadataDefByName;
//    protected @Metric(always = true)  MutableRate typesREST_createAtlasTypeDefs;
//    protected @Metric(always = true)  MutableRate typesREST_updateAtlasTypeDefs;
//    protected @Metric(always = true)  MutableRate typesREST_deleteAtlasTypeDefs;
//    protected @Metric(always = true)  MutableRate typesREST_deleteAtlasTypeByName;
//
//
//    protected @Metric(always = true)  MutableRate glossaryREST_getGlossaries;
//    protected @Metric(always = true)  MutableRate glossaryREST_getGlossary;
//    protected @Metric(always = true)  MutableRate glossaryREST_getDetailedGlossary;
//    protected @Metric(always = true)  MutableRate glossaryREST_getGlossaryTerm;
//    protected @Metric(always = true)  MutableRate glossaryREST_getGlossaryCategory;
//    protected @Metric(always = true)  MutableRate glossaryREST_createGlossary;
//    protected @Metric(always = true)  MutableRate glossaryREST_createGlossaryTerm;
//    protected @Metric(always = true)  MutableRate glossaryREST_createGlossaryTerms;
//    protected @Metric(always = true)  MutableRate glossaryREST_createGlossaryCategory;
//    protected @Metric(always = true)  MutableRate glossaryREST_createGlossaryCategories;
//    protected @Metric(always = true)  MutableRate glossaryREST_updateGlossary;
//    protected @Metric(always = true)  MutableRate glossaryREST_partialUpdateGlossary;
//    protected @Metric(always = true)  MutableRate glossaryREST_updateGlossaryTerm;
//    protected @Metric(always = true)  MutableRate glossaryREST_partialUpdateGlossaryTerm;
//    protected @Metric(always = true)  MutableRate glossaryREST_updateGlossaryCategory;
//    protected @Metric(always = true)  MutableRate glossaryREST_partialUpdateGlossaryCategory;
//    protected @Metric(always = true)  MutableRate glossaryREST_deleteGlossary;
//    protected @Metric(always = true)  MutableRate glossaryREST_deleteGlossaryTerm;
//    protected @Metric(always = true)  MutableRate glossaryREST_deleteGlossaryCategory;
//    protected @Metric(always = true)  MutableRate glossaryREST_getGlossaryTerms;
//    protected @Metric(always = true)  MutableRate glossaryREST_getGlossaryTermHeaders;
//    protected @Metric(always = true)  MutableRate glossaryREST_getGlossaryCategories;
//    protected @Metric(always = true)  MutableRate glossaryREST_getGlossaryCategoriesHeaders;
//    protected @Metric(always = true)  MutableRate glossaryREST_getCategoryTerms;
//    protected @Metric(always = true)  MutableRate glossaryREST_getRelatedTerms;
//    protected @Metric(always = true)  MutableRate glossaryREST_getEntitiesAssignedWithTerm;
//    protected @Metric(always = true)  MutableRate glossaryREST_assignTermToEntities;
//    protected @Metric(always = true)  MutableRate glossaryREST_removeTermAssignmentFromEntities;
//    protected @Metric(always = true)  MutableRate glossaryREST_disassociateTermAssignmentFromEntities;
//    protected @Metric(always = true)  MutableRate glossaryREST_getRelatedCategories;
//    protected @Metric(always = true)  MutableRate glossaryREST_importGlossaryData;
//
//    protected @Metric(always = true)  MutableRate discoveryREST_quickSearchQuickSearchParams;
//    protected @Metric(always = true)  MutableRate discoveryREST_searchUsingFullText;
//    protected @Metric(always = true)  MutableRate discoveryREST_searchUsingAttribute;
//    protected @Metric(always = true)  MutableRate discoveryREST_searchWithParameters;
//    protected @Metric(always = true)  MutableRate discoveryREST_searchRelatedEntities;
//    protected @Metric(always = true)  MutableRate discoveryREST_updateSavedSearch;
//    protected @Metric(always = true)  MutableRate discoveryREST_getSavedSearch;
//    protected @Metric(always = true)  MutableRate discoveryREST_getSavedSearches;
//    protected @Metric(always = true)  MutableRate discoveryREST_deleteSavedSearch;
//    protected @Metric(always = true)  MutableRate discoveryREST_executeSavedSearchByName;
//    protected @Metric(always = true)  MutableRate discoveryREST_executeSavedSearchByGuid;
//    protected @Metric(always = true)  MutableRate discoveryREST_getSuggestions;
//    protected @Metric(always = true)  MutableRate discoveryREST_searchUsingDSL;
//    protected @Metric(always = true)  MutableRate discoveryREST_searchUsingBasic;
//    protected @Metric(always = true)  MutableRate discoveryREST_quickSearch;
//    protected @Metric(always = true)  MutableRate discoveryREST_addSavedSearch;
//
//    protected @Metric(always = true)  MutableRate notificationHookConsumer_doWork;
//
//    protected @Metric(always = true)  MutableRate lineageREST_getLineageByUniqAttr;
//    protected @Metric(always = true)  MutableRate lineageREST_getLineageGraph;
//
//    protected @Metric(always = true)  MutableRate relationshipREST_create;
//    protected @Metric(always = true)  MutableRate relationshipREST_update;
//    protected @Metric(always = true)  MutableRate relationshipREST_getById;
//    protected @Metric(always = true)  MutableRate relationshipREST_deleteById;
//
//
//    public static final Map<String, String> fieldLowerCaseUpperCaseMap = new HashMap<>();
//    public static final Set<String>         debugMetricsAttributes     = new HashSet<>();
//
//    public AtlasDebugMetricsSource() {
//        initAttrList();
//
//        populateFieldList();
//    }
//
//    public void update(Signature name, Long timeConsumed) {
//        String signatureName = name.toString();
//
//        switch (signatureName) {
//            case DebugMetricsWrapper.Constants.EntityREST_createOrUpdateBulk:
//                entityREST_createOrUpdateBulk.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_addOrUpdateBMByName:
//                entityREST_addOrUpdateBusinessAttributesByName.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_removeBMByName:
//                entityREST_removeBusinessAttributesByName.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_removeLabelsByTypeName:
//                entityREST_removeLabelsByTypeName.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_setLabelsByTypeName:
//                entityREST_setLabelsByTypeName.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_addLabelsByTypeName:
//                entityREST_addLabelsByTypeName.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.DiscoveryREST_quickSearchQuickSearchParams:
//                discoveryREST_quickSearchQuickSearchParams.add(timeConsumed);
//                break;
//
//            default:
//                update(name.toShortString(), timeConsumed);
//                break;
//        }
//    }
//
//    private void populateFieldList() {
//        Field fields[] = AtlasDebugMetricsSource.class.getDeclaredFields();
//
//        for (int i = 0; i < fields.length; i++) {
//            if (fields[i].getAnnotation(Metric.class) == null) {
//                continue;
//            }
//
//            String name = fields[i].getName();
//
//            for(String metricName : debugMetricsAttributes) {
//                fieldLowerCaseUpperCaseMap.put(name.toLowerCase() + metricName, StringUtils.capitalize(name));
//            }
//
//        }
//    }
//
//    private void initAttrList() {
//        debugMetricsAttributes.add(DebugMetricsWrapper.Constants.NUM_OPS);
//        debugMetricsAttributes.add(DebugMetricsWrapper.Constants.MIN_TIME);
//        debugMetricsAttributes.add(DebugMetricsWrapper.Constants.STD_DEV_TIME);
//        debugMetricsAttributes.add(DebugMetricsWrapper.Constants.MAX_TIME);
//        debugMetricsAttributes.add(DebugMetricsWrapper.Constants.AVG_TIME);
//    }
//
//    private void update(String name, Long timeConsumed) {
//        switch (name) {
//            case DebugMetricsWrapper.Constants.RelationshipREST_create:
//                relationshipREST_create.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.RelationshipREST_update:
//                relationshipREST_update.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.RelationshipREST_getById:
//                relationshipREST_getById.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.RelationshipREST_deleteById:
//                relationshipREST_deleteById.add(timeConsumed);
//                break;
//
//                //DiscoveryREST apis
//            case DebugMetricsWrapper.Constants.DiscoveryREST_searchUsingFullText:
//                discoveryREST_searchUsingFullText.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.DiscoveryREST_searchUsingAttribute:
//                discoveryREST_searchUsingAttribute.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.DiscoveryREST_searchWithParameters:
//                discoveryREST_searchWithParameters.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.DiscoveryREST_searchRelatedEntities:
//                discoveryREST_searchRelatedEntities.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.DiscoveryREST_updateSavedSearch:
//                discoveryREST_updateSavedSearch.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.DiscoveryREST_getSavedSearch:
//                discoveryREST_getSavedSearch.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.DiscoveryREST_getSavedSearches:
//                discoveryREST_getSavedSearches.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.DiscoveryREST_deleteSavedSearch:
//                discoveryREST_deleteSavedSearch.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.DiscoveryREST_executeSavedSearchByName:
//                discoveryREST_executeSavedSearchByName.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.DiscoveryREST_executeSavedSearchByGuid:
//                discoveryREST_executeSavedSearchByGuid.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.DiscoveryREST_getSuggestions:
//                discoveryREST_getSuggestions.add(timeConsumed);
//                break;
//
//            //GlossaryRest apis
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_getGlossaries:
//                glossaryREST_getGlossaries.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_getGlossary:
//                glossaryREST_getGlossary.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_getDetailedGlossary:
//                glossaryREST_getDetailedGlossary.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_getGlossaryTerm:
//                glossaryREST_getGlossaryTerm.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_getGlossaryCategory:
//                glossaryREST_getGlossaryCategory.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_createGlossary:
//                glossaryREST_createGlossary.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_createGlossaryTerm:
//                glossaryREST_createGlossaryTerm.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_createGlossaryTerms:
//                glossaryREST_createGlossaryTerms.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_createGlossaryCategory:
//                glossaryREST_createGlossaryCategory.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_createGlossaryCategories:
//                glossaryREST_createGlossaryCategories.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_updateGlossary:
//                glossaryREST_updateGlossary.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_partialUpdateGlossary:
//                glossaryREST_partialUpdateGlossary.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_updateGlossaryTerm:
//                glossaryREST_updateGlossaryTerm.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_partialUpdateGlossaryTerm:
//                glossaryREST_partialUpdateGlossaryTerm.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_updateGlossaryCategory:
//                glossaryREST_updateGlossaryCategory.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_partialUpdateGlossaryCategory:
//                glossaryREST_partialUpdateGlossaryCategory.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_deleteGlossary:
//                glossaryREST_deleteGlossary.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_deleteGlossaryTerm:
//                glossaryREST_deleteGlossaryTerm.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_deleteGlossaryCategory:
//                glossaryREST_deleteGlossaryCategory.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_getGlossaryTerms:
//                glossaryREST_getGlossaryTerms.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_getGlossaryTermHeaders:
//                glossaryREST_getGlossaryTermHeaders.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_getGlossaryCategories:
//                glossaryREST_getGlossaryCategories.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_getGlossaryCategoriesHeaders:
//                glossaryREST_getGlossaryCategoriesHeaders.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_getCategoryTerms:
//                glossaryREST_getCategoryTerms.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_getRelatedTerms:
//                glossaryREST_getRelatedTerms.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_getEntitiesAssignedWithTerm:
//                glossaryREST_getEntitiesAssignedWithTerm.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_assignTermToEntities:
//                glossaryREST_assignTermToEntities.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_removeTermAssignmentFromEntities:
//                glossaryREST_removeTermAssignmentFromEntities.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_disassociateTermAssignmentFromEntities:
//                glossaryREST_disassociateTermAssignmentFromEntities.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_getRelatedCategories:
//                glossaryREST_getRelatedCategories.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.GlossaryREST_importGlossaryData:
//                glossaryREST_importGlossaryData.add(timeConsumed);
//                break;
//
//            //TypesREST apis
//
//            case DebugMetricsWrapper.Constants.TypesREST_getTypeDefByName:
//                typesREST_getTypeDefByName.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_getTypeDefByGuid:
//                typesREST_getTypeDefByGuid.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_getTypeDefHeaders:
//                typesREST_getTypeDefHeaders.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_getAllTypeDefs:
//                typesREST_getAllTypeDefs.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_getEnumDefByName:
//                typesREST_getEnumDefByName.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_getEnumDefByGuid:
//                typesREST_getEnumDefByGuid.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_getStructDefByName:
//                typesREST_getStructDefByName.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_getStructDefByGuid:
//                typesREST_getStructDefByGuid.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_getClassificationDefByName:
//                typesREST_getClassificationDefByName.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_getClassificationDefByGuid:
//                typesREST_getClassificationDefByGuid.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_getEntityDefByName:
//                typesREST_getEntityDefByName.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_getEntityDefByGuid:
//                typesREST_getEntityDefByGuid.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_getRelationshipDefByName:
//                typesREST_getRelationshipDefByName.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_getRelationshipDefByGuid:
//                typesREST_getRelationshipDefByGuid.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_getBusinessMetadataDefByGuid:
//                typesREST_getBusinessMetadataDefByGuid.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_getBusinessMetadataDefByName:
//                typesREST_getBusinessMetadataDefByName.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_createAtlasTypeDefs:
//                typesREST_createAtlasTypeDefs.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_updateAtlasTypeDefs:
//                typesREST_updateAtlasTypeDefs.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_deleteAtlasTypeDefs:
//                typesREST_deleteAtlasTypeDefs.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.TypesREST_deleteAtlasTypeByName:
//                typesREST_deleteAtlasTypeByName.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_getHeaderById:
//                entityREST_getHeaderById.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_getEntityHeaderByUA:
//                entityREST_getEntityHeaderByUniqAttr.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_getByUniqueAttributes:
//                entityREST_getByUniqueAttributes.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_partialUpdateEntityByUA:
//                entityREST_partialUpdateEntityByUniqAttr.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_deleteByUA:
//                entityREST_deleteByUniqAttr.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_updateClassificationsByUA:
//                entityREST_updateClassificationsByUniqAttr.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_updateClassifications:
//                entityREST_updateClassifications.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_getEntitiesByUA:
//                entityREST_getEntitiesByUniqAttr.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_getByGuids:
//                entityREST_getByGuids.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_deleteByGuids:
//                entityREST_deleteByGuids.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_addClassification:
//                entityREST_addClassification.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_getAuditEvents:
//                entityREST_getAuditEvents.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_getEntityHeaders:
//                entityREST_getEntityHeaders.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_setClassifications:
//                entityREST_setClassifications.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_addOrUpdateBusinessAttributes:
//                entityREST_addOrUpdateBusinessAttributes.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_removeBusinessAttributes:
//                entityREST_removeBusinessAttributes.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_setLabels:
//                entityREST_setLabels.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_addLabels:
//                entityREST_addLabels.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_removeLabels:
//                entityREST_removeLabels.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_getById:
//                entityREST_getById.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_createOrUpdate:
//                entityREST_createOrUpdate.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_partialUpdateEntityAttrByGuid:
//                entityREST_partialUpdateEntityAttrByGuid.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_deleteByGuid:
//                entityREST_deleteByGuid.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_getClassification:
//                entityREST_getClassification.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_getClassifications:
//                entityREST_getClassifications.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_addClassificationsByUA:
//                entityREST_addClassificationsByUniqAttr.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_addClassifications:
//                entityREST_addClassifications.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_deleteClassificationByUA:
//                entityREST_deleteClassificationByUniqAttr.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_deleteClassification:
//                entityREST_deleteClassification.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.EntityREST_importBMAttributes:
//                entityREST_importBMAttributes.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.NotificationHookConsumer_doWork:
//                notificationHookConsumer_doWork.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.DiscoveryREST_searchUsingDSL:
//                discoveryREST_searchUsingDSL.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.DiscoveryREST_searchUsingBasic:
//                discoveryREST_searchUsingBasic.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.DiscoveryREST_quickSearch:
//                discoveryREST_quickSearch.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.DiscoveryREST_addSavedSearch:
//                discoveryREST_addSavedSearch.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.LineageREST_getLineageByUA:
//                lineageREST_getLineageByUniqAttr.add(timeConsumed);
//                break;
//
//            case DebugMetricsWrapper.Constants.LineageREST_getLineageGraph:
//                lineageREST_getLineageGraph.add(timeConsumed);
//                break;
//
//            default:
//                LOG.error("The signature '{}' is not handled!", name);
//                break;
//        }
//    }
//}