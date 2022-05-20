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

package org.apache.compactatlas.repository.repository.impexp;

import org.apache.compactatlas.intg.exception.AtlasBaseException;
import org.apache.compactatlas.repository.glossary.GlossaryService;
import org.apache.compactatlas.intg.model.TypeCategory;
import org.apache.compactatlas.intg.model.glossary.AtlasGlossaryTerm;
import org.apache.compactatlas.intg.model.instance.AtlasClassification;
import org.apache.compactatlas.intg.model.instance.AtlasEntity;
import org.apache.compactatlas.intg.model.instance.AtlasRelatedObjectId;
import org.apache.compactatlas.intg.model.typedef.AtlasStructDef;
import org.apache.compactatlas.intg.type.AtlasArrayType;
import org.apache.compactatlas.intg.type.AtlasBusinessMetadataType;
import org.apache.compactatlas.intg.type.AtlasClassificationType;
import org.apache.compactatlas.intg.type.AtlasEntityType;
import org.apache.compactatlas.intg.type.AtlasEnumType;
import org.apache.compactatlas.intg.type.AtlasMapType;
import org.apache.compactatlas.intg.type.AtlasRelationshipType;
import org.apache.compactatlas.intg.type.AtlasStructType;
import org.apache.compactatlas.intg.type.AtlasType;
import org.apache.compactatlas.intg.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

class ExportTypeProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ExportTypeProcessor.class);
    private static final String RELATIONSHIP_ATTR_MEANINGS = "meanings";

    private AtlasTypeRegistry typeRegistry;
    private GlossaryService glossaryService;

    ExportTypeProcessor(AtlasTypeRegistry typeRegistry, GlossaryService glossaryService) {
        this.typeRegistry = typeRegistry;
        this.glossaryService = glossaryService;
    }

    public void addTypes(AtlasEntity entity, ExportService.ExportContext context) {
        addEntityType(entity.getTypeName(), context);

        if(CollectionUtils.isNotEmpty(entity.getClassifications())) {
            for (AtlasClassification c : entity.getClassifications()) {
                addClassificationType(c.getTypeName(), context);
            }
        }

        addTerms(entity, context);
    }

    private void addTerms(AtlasEntity entity, ExportService.ExportContext context) {
        Object relAttrMeanings = entity.getRelationshipAttribute(RELATIONSHIP_ATTR_MEANINGS);
        if (relAttrMeanings == null || !(relAttrMeanings instanceof List)) {
            return;
        }

        List list = (List) relAttrMeanings;
        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        for (Object objectId : list) {
            if (objectId instanceof AtlasRelatedObjectId) {
                AtlasRelatedObjectId termObjectId = (AtlasRelatedObjectId) objectId;

                try {
                    AtlasGlossaryTerm term = glossaryService.getTerm(termObjectId.getGuid());
                    context.termsGlossary.put(termObjectId.getGuid(), term.getAnchor().getGlossaryGuid());
                }
                catch (AtlasBaseException e) {
                    LOG.warn("Error fetching term details: {}", termObjectId);
                }
            }
        }
    }

    private void addType(String typeName, ExportService.ExportContext context) {
        AtlasType type = null;

        try {
            type = typeRegistry.getType(typeName);

            addType(type, context);
        } catch (AtlasBaseException excp) {
            LOG.error("unknown type {}", typeName);
        }
    }

    private void addEntityType(String typeName, ExportService.ExportContext context) {
        if (!context.entityTypes.contains(typeName)) {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

            addEntityType(entityType, context);
        }
    }

    private void addClassificationType(String typeName, ExportService.ExportContext context) {
        if (!context.classificationTypes.contains(typeName)) {
            AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(typeName);

            addClassificationType(classificationType, context);
        }
    }

    private void addType(AtlasType type, ExportService.ExportContext context) {
        if (type.getTypeCategory() == TypeCategory.PRIMITIVE) {
            return;
        }

        if (type instanceof AtlasArrayType) {
            AtlasArrayType arrayType = (AtlasArrayType)type;

            addType(arrayType.getElementType(), context);
        } else if (type instanceof AtlasMapType) {
            AtlasMapType mapType = (AtlasMapType)type;

            addType(mapType.getKeyType(), context);
            addType(mapType.getValueType(), context);
        } else if (type instanceof AtlasEntityType) {
            addEntityType((AtlasEntityType)type, context);
        } else if (type instanceof AtlasClassificationType) {
            addClassificationType((AtlasClassificationType)type, context);
        } else if (type instanceof AtlasRelationshipType) {
            addRelationshipType(type.getTypeName(), context);
        } else if (type instanceof AtlasBusinessMetadataType) {
            addBusinessMetadataType((AtlasBusinessMetadataType) type, context);
        } else if (type instanceof AtlasStructType) {
            addStructType((AtlasStructType)type, context);
        } else if (type instanceof AtlasEnumType) {
            addEnumType((AtlasEnumType)type, context);
        }
    }

    private void addEntityType(AtlasEntityType entityType, ExportService.ExportContext context) {
        if (!context.entityTypes.contains(entityType.getTypeName())) {
            context.entityTypes.add(entityType.getTypeName());

            addAttributeTypes(entityType, context);
            addRelationshipTypes(entityType, context);
            addBusinessMetadataType(entityType, context);

            if (CollectionUtils.isNotEmpty(entityType.getAllSuperTypes())) {
                for (String superType : entityType.getAllSuperTypes()) {
                    addEntityType(superType, context);
                }
            }
        }
    }

    private void addClassificationType(AtlasClassificationType classificationType, ExportService.ExportContext context) {
        if (!context.classificationTypes.contains(classificationType.getTypeName())) {
            context.classificationTypes.add(classificationType.getTypeName());

            addAttributeTypes(classificationType, context);

            if (CollectionUtils.isNotEmpty(classificationType.getAllSuperTypes())) {
                for (String superType : classificationType.getAllSuperTypes()) {
                    addClassificationType(superType, context);
                }
            }
        }
    }

    private void addStructType(AtlasStructType structType, ExportService.ExportContext context) {
        if (!context.structTypes.contains(structType.getTypeName())) {
            context.structTypes.add(structType.getTypeName());

            addAttributeTypes(structType, context);
        }
    }

    private void addEnumType(AtlasEnumType enumType, ExportService.ExportContext context) {
        if (!context.enumTypes.contains(enumType.getTypeName())) {
            context.enumTypes.add(enumType.getTypeName());
        }
    }

    private void addRelationshipType(String relationshipTypeName, ExportService.ExportContext context) {
        if (!context.relationshipTypes.contains(relationshipTypeName)) {
            AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(relationshipTypeName);

            if (relationshipType != null) {
                context.relationshipTypes.add(relationshipTypeName);

                addAttributeTypes(relationshipType, context);
                addEntityType(relationshipType.getEnd1Type(), context);
                addEntityType(relationshipType.getEnd2Type(), context);
            }
        }
    }

    private void addBusinessMetadataType(AtlasBusinessMetadataType businessMetadataType, ExportService.ExportContext context) {
        if (!context.businessMetadataTypes.contains(businessMetadataType.getTypeName())) {
            context.businessMetadataTypes.add(businessMetadataType.getTypeName());

            addAttributeTypes(businessMetadataType, context);
        }
    }

    private void addBusinessMetadataType(AtlasEntityType entityType, ExportService.ExportContext context) {
        for (String bmTypeName : entityType.getBusinessAttributes().keySet()) {
            AtlasBusinessMetadataType bmType = typeRegistry.getBusinessMetadataTypeByName(bmTypeName);

            addBusinessMetadataType(bmType, context);
        }
    }

    private void addAttributeTypes(AtlasStructType structType, ExportService.ExportContext context) {
        for (AtlasStructDef.AtlasAttributeDef attributeDef : structType.getStructDef().getAttributeDefs()) {
            addType(attributeDef.getTypeName(), context);
        }
    }

    private void addRelationshipTypes(AtlasEntityType entityType, ExportService.ExportContext context) {
        for (Map.Entry<String, Map<String, AtlasStructType.AtlasAttribute>> entry : entityType.getRelationshipAttributes().entrySet()) {
            for (String relationshipType : entry.getValue().keySet()) {
                addRelationshipType(relationshipType, context);
            }
        }
    }
}
