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

import org.apache.compactatlas.intg.model.instance.AtlasEntity;
import org.apache.compactatlas.intg.model.instance.AtlasRelatedObjectId;
import org.apache.compactatlas.intg.model.typedef.AtlasBaseTypeDef;
import org.apache.compactatlas.intg.model.typedef.AtlasEntityDef;
import org.apache.compactatlas.intg.type.AtlasTypeRegistry;
import org.apache.compactatlas.intg.type.AtlasTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RelationshipAttributesExtractor implements ExtractStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(RelationshipAttributesExtractor.class);

    private final AtlasTypeRegistry typeRegistry;

    public RelationshipAttributesExtractor(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Override
    public void fullFetch(AtlasEntity entity, ExportService.ExportContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> fullFetch({}): guidsToProcess {}", AtlasTypeUtil.getAtlasObjectId(entity), context.guidsToProcess.size());
        }

        List<AtlasRelatedObjectId> atlasRelatedObjectIdList = getRelatedObjectIds(entity);

        for (AtlasRelatedObjectId ar : atlasRelatedObjectIdList) {
            boolean isLineage = isLineageType(ar.getTypeName());

            if (context.skipLineage && isLineage) {
                continue;
            }
            context.addToBeProcessed(isLineage, ar.getGuid(), ExportService.TraversalDirection.BOTH);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== fullFetch({}): guidsToProcess {}", entity.getGuid(), context.guidsToProcess.size());
        }
    }

    @Override
    public void connectedFetch(AtlasEntity entity, ExportService.ExportContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> connectedFetch({}): guidsToProcess {}", AtlasTypeUtil.getAtlasObjectId(entity), context.guidsToProcess.size());
        }

        ExportService.TraversalDirection direction = context.guidDirection.get(entity.getGuid());

        if (direction == null || direction == ExportService.TraversalDirection.UNKNOWN) {
            addToBeProcessed(entity, context, ExportService.TraversalDirection.OUTWARD, ExportService.TraversalDirection.INWARD);
        } else {
            if (isLineageType(entity.getTypeName())) {
                direction = ExportService.TraversalDirection.OUTWARD;
            }
            addToBeProcessed(entity, context, direction);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> connectedFetch({}): guidsToProcess {}", AtlasTypeUtil.getAtlasObjectId(entity), context.guidsToProcess.size());
        }
    }

    @Override
    public void close() {
    }

    private void addToBeProcessed(AtlasEntity entity, ExportService.ExportContext context, ExportService.TraversalDirection... directions) {
        if (directions == null || directions.length == 0) {
            return;
        }

        boolean isLineageEntity = isLineageType(entity.getTypeName());
        List<AtlasRelatedObjectId> relatedObjectIds = getRelatedObjectIds(entity);

        for (ExportService.TraversalDirection direction : directions) {
            for (AtlasRelatedObjectId id : relatedObjectIds) {
                String guid = id.getGuid();
                ExportService.TraversalDirection currentDirection = context.guidDirection.get(guid);
                boolean isLineageId = isLineageType(id.getTypeName());
                ExportService.TraversalDirection edgeDirection = getRelationshipEdgeDirection(id, entity.getTypeName());

                if (context.skipLineage && isLineageId) continue;

                if (!isLineageEntity && direction != edgeDirection ||
                        isLineageEntity && direction == edgeDirection)
                    continue;

                if (currentDirection == null) {
                    context.addToBeProcessed(isLineageId, guid, direction);

                } else if (currentDirection == ExportService.TraversalDirection.OUTWARD && direction == ExportService.TraversalDirection.INWARD) {
                    context.guidsProcessed.remove(guid);
                    context.addToBeProcessed(isLineageId, guid, direction);
                }
            }
        }
    }

    private ExportService.TraversalDirection getRelationshipEdgeDirection(AtlasRelatedObjectId relatedObjectId, String entityTypeName) {
        boolean isOutEdge = typeRegistry.getRelationshipDefByName(relatedObjectId.getRelationshipType()).getEndDef1().getType().equals(entityTypeName);
        return isOutEdge ? ExportService.TraversalDirection.OUTWARD : ExportService.TraversalDirection.INWARD;
    }

    private boolean isLineageType(String typeName) {
        AtlasEntityDef entityDef = typeRegistry.getEntityDefByName(typeName);
        return entityDef.getSuperTypes().contains(AtlasBaseTypeDef.ATLAS_TYPE_PROCESS);
    }

    private List<AtlasRelatedObjectId> getRelatedObjectIds(AtlasEntity entity) {
        List<AtlasRelatedObjectId> relatedObjectIds = new ArrayList<>();

        for (Object o : entity.getRelationshipAttributes().values()) {
            if (o instanceof AtlasRelatedObjectId) {
                relatedObjectIds.add((AtlasRelatedObjectId) o);
            } else if (o instanceof Collection) {
                relatedObjectIds.addAll((List) o);
            }
        }
        return relatedObjectIds;
    }
}
