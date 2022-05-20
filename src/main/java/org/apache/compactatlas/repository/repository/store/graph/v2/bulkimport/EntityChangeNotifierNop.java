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
package org.apache.compactatlas.repository.repository.store.graph.v2.bulkimport;

import org.apache.compactatlas.intg.exception.AtlasBaseException;
import org.apache.compactatlas.intg.model.glossary.AtlasGlossaryTerm;
import org.apache.compactatlas.intg.model.instance.AtlasClassification;
import org.apache.compactatlas.intg.model.instance.AtlasEntity;
import org.apache.compactatlas.intg.model.instance.AtlasRelatedObjectId;
import org.apache.compactatlas.intg.model.instance.AtlasRelationship;
import org.apache.compactatlas.intg.model.instance.EntityMutationResponse;
import org.apache.compactatlas.intg.model.notification.EntityNotification;
import org.apache.compactatlas.repository.repository.store.graph.v2.IAtlasEntityChangeNotifier;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class EntityChangeNotifierNop implements IAtlasEntityChangeNotifier {
    @Override
    public void onEntitiesMutated(EntityMutationResponse entityMutationResponse, boolean isImport) throws AtlasBaseException {

    }

    @Override
    public void notifyRelationshipMutation(AtlasRelationship relationship, EntityNotification.EntityNotificationV2.OperationType operationType) throws AtlasBaseException {

    }

    @Override
    public void onClassificationAddedToEntity(AtlasEntity entity, List<AtlasClassification> addedClassifications) throws AtlasBaseException {

    }

    @Override
    public void onClassificationsAddedToEntities(List<AtlasEntity> entities, List<AtlasClassification> addedClassifications) throws AtlasBaseException {

    }

    @Override
    public void onClassificationDeletedFromEntity(AtlasEntity entity, List<AtlasClassification> deletedClassifications) throws AtlasBaseException {

    }

    @Override
    public void onClassificationsDeletedFromEntities(List<AtlasEntity> entities, List<AtlasClassification> deletedClassifications) throws AtlasBaseException {

    }

    @Override
    public void onTermAddedToEntities(AtlasGlossaryTerm term, List<AtlasRelatedObjectId> entityIds) throws AtlasBaseException {

    }

    @Override
    public void onTermDeletedFromEntities(AtlasGlossaryTerm term, List<AtlasRelatedObjectId> entityIds) throws AtlasBaseException {

    }

    @Override
    public void onLabelsUpdatedFromEntity(String entityGuid, Set<String> addedLabels, Set<String> deletedLabels) throws AtlasBaseException {

    }

    @Override
    public void notifyPropagatedEntities() throws AtlasBaseException {

    }

    @Override
    public void onClassificationUpdatedToEntity(AtlasEntity entity, List<AtlasClassification> updatedClassifications) throws AtlasBaseException {

    }

    @Override
    public void onBusinessAttributesUpdated(String entityGuid, Map<String, Map<String, Object>> updatedBusinessAttributes) throws AtlasBaseException {

    }
}
