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
package org.apache.compactatlas.repository.repository.store.graph.v2;

import org.apache.compactatlas.intg.AtlasErrorCode;
import org.apache.compactatlas.serverapi.RequestContext;
import org.apache.compactatlas.intg.exception.AtlasBaseException;
import org.apache.compactatlas.intg.model.TypeCategory;
import org.apache.compactatlas.intg.model.instance.AtlasEntity;
import org.apache.compactatlas.graphdb.api.AtlasGraph;
import org.apache.compactatlas.graphdb.api.AtlasVertex;
import org.apache.compactatlas.repository.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.compactatlas.repository.repository.store.graph.EntityResolver;
import org.apache.compactatlas.intg.type.AtlasEntityType;
import org.apache.compactatlas.intg.type.AtlasTypeRegistry;
import org.apache.compactatlas.intg.type.AtlasTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IDBasedEntityResolver implements EntityResolver {
    private static final Logger LOG = LoggerFactory.getLogger(IDBasedEntityResolver.class);

    private final AtlasGraph        graph;
    private final AtlasTypeRegistry typeRegistry;

    public IDBasedEntityResolver(AtlasGraph graph, AtlasTypeRegistry typeRegistry) {
        this.graph             = graph;
        this.typeRegistry      = typeRegistry;
    }

    public EntityGraphDiscoveryContext resolveEntityReferences(EntityGraphDiscoveryContext context) throws AtlasBaseException {
        if (context == null) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "IDBasedEntityResolver.resolveEntityReferences(): context is null");
        }

        EntityStream entityStream = context.getEntityStream();

        for (String guid : context.getReferencedGuids()) {
            boolean isAssignedGuid = AtlasTypeUtil.isAssignedGuid(guid);
            AtlasVertex vertex = isAssignedGuid ? AtlasGraphUtilsV2.findByGuid(this.graph, guid) : null;

            if (vertex == null && !RequestContext.get().isImportInProgress()) { // if not found in the store, look if the entity is present in the stream
                AtlasEntity entity = entityStream.getByGuid(guid);

                if (entity != null) { // look for the entity in the store using unique-attributes
                    AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

                    if (entityType == null) {
                        throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), entity.getTypeName());
                    }

                    vertex = AtlasGraphUtilsV2.findByUniqueAttributes(this.graph, entityType, entity.getAttributes());
                } else if (!isAssignedGuid) { // for local-guids, entity must be in the stream
                    throw new AtlasBaseException(AtlasErrorCode.REFERENCED_ENTITY_NOT_FOUND, guid);
                }
            }

            if (vertex != null) {
                context.addResolvedGuid(guid, vertex);
            } else {
                if (isAssignedGuid && !RequestContext.get().isImportInProgress()) {
                    throw new AtlasBaseException(AtlasErrorCode.REFERENCED_ENTITY_NOT_FOUND, guid);
                } else {
                    context.addLocalGuidReference(guid);
                }
            }
        }

        return context;
    }
}