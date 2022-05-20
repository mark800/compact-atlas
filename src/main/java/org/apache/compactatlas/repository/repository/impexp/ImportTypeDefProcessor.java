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
import org.apache.compactatlas.intg.model.impexp.AtlasImportResult;
import org.apache.compactatlas.intg.model.typedef.AtlasBusinessMetadataDef;
import org.apache.compactatlas.intg.model.typedef.AtlasClassificationDef;
import org.apache.compactatlas.intg.model.typedef.AtlasEntityDef;
import org.apache.compactatlas.intg.model.typedef.AtlasEnumDef;
import org.apache.compactatlas.intg.model.typedef.AtlasRelationshipDef;
import org.apache.compactatlas.intg.model.typedef.AtlasStructDef;
import org.apache.compactatlas.intg.model.typedef.AtlasTypesDef;
import org.apache.compactatlas.repository.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.compactatlas.repository.store.AtlasTypeDefStore;
import org.apache.compactatlas.intg.type.AtlasTypeRegistry;

public class ImportTypeDefProcessor {
    private final AtlasTypeDefStore typeDefStore;
    private final AtlasTypeRegistry typeRegistry;
    private TypeAttributeDifference typeAttributeDifference;

    public ImportTypeDefProcessor(AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) {
        this.typeDefStore = typeDefStore;
        this.typeRegistry = typeRegistry;
        this.typeAttributeDifference = new TypeAttributeDifference(typeDefStore, typeRegistry);
    }

    public void processTypes(AtlasTypesDef typeDefinitionMap, AtlasImportResult result) throws AtlasBaseException {
        setGuidToEmpty(typeDefinitionMap);

        AtlasTypesDef typesToCreate = AtlasTypeDefStoreInitializer.getTypesToCreate(typeDefinitionMap, this.typeRegistry);
        if (!typesToCreate.isEmpty()) {
            typeDefStore.createTypesDef(typesToCreate);
            updateMetricsForTypesDef(typesToCreate, result);
        }

        typeAttributeDifference.updateTypes(typeDefinitionMap, result);
    }

    private void setGuidToEmpty(AtlasTypesDef typesDef) {
        for (AtlasEntityDef def : typesDef.getEntityDefs()) {
            def.setGuid(null);
        }

        for (AtlasClassificationDef def : typesDef.getClassificationDefs()) {
            def.setGuid(null);
        }

        for (AtlasEnumDef def : typesDef.getEnumDefs()) {
            def.setGuid(null);
        }

        for (AtlasStructDef def : typesDef.getStructDefs()) {
            def.setGuid(null);
        }

        for (AtlasRelationshipDef def : typesDef.getRelationshipDefs()) {
            def.setGuid(null);
        }

        for (AtlasBusinessMetadataDef def : typesDef.getBusinessMetadataDefs()) {
            def.setGuid(null);
        }
    }

    private void updateMetricsForTypesDef(AtlasTypesDef typeDefinitionMap, AtlasImportResult result) {
        result.incrementMeticsCounter("typedef:classification", typeDefinitionMap.getClassificationDefs().size());
        result.incrementMeticsCounter("typedef:enum", typeDefinitionMap.getEnumDefs().size());
        result.incrementMeticsCounter("typedef:entitydef", typeDefinitionMap.getEntityDefs().size());
        result.incrementMeticsCounter("typedef:struct", typeDefinitionMap.getStructDefs().size());
        result.incrementMeticsCounter("typedef:relationship", typeDefinitionMap.getRelationshipDefs().size());
        result.incrementMeticsCounter("typedef:businessmetadata", typeDefinitionMap.getBusinessMetadataDefs().size());
    }
}
