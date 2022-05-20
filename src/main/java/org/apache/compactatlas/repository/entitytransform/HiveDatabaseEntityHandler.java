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
package org.apache.compactatlas.repository.entitytransform;

import org.apache.compactatlas.intg.model.instance.AtlasEntity;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;

public class HiveDatabaseEntityHandler extends BaseEntityHandler {
    static final List<String> CUSTOM_TRANSFORM_ATTRIBUTES = Arrays.asList(TransformationConstants.HIVE_DB_NAME_ATTRIBUTE, TransformationConstants.HIVE_DB_CLUSTER_NAME_ATTRIBUTE);

    public HiveDatabaseEntityHandler(List<AtlasEntityTransformer> transformers) {
        super(transformers);
    }

    @Override
    public AtlasTransformableEntity getTransformableEntity(AtlasEntity entity) {
        return isHiveDatabaseEntity(entity) ? new HiveDatabaseEntity(entity) : null;
    }


    private boolean isHiveDatabaseEntity(AtlasEntity entity) {
        return StringUtils.equals(entity.getTypeName(), TransformationConstants.HIVE_DATABASE);
    }

    private static class HiveDatabaseEntity extends AtlasTransformableEntity {
        private String  databaseName;
        private String  clusterName;
        private boolean isCustomAttributeUpdated = false;

        public HiveDatabaseEntity(AtlasEntity entity) {
            super(entity);

            this.databaseName = (String) entity.getAttribute(TransformationConstants.NAME_ATTRIBUTE);

            String qualifiedName = (String) entity.getAttribute(TransformationConstants.QUALIFIED_NAME_ATTRIBUTE);

            if (qualifiedName != null) {
                int clusterSeparatorIdx = qualifiedName.lastIndexOf(TransformationConstants.CLUSTER_DELIMITER);

                this.clusterName = clusterSeparatorIdx != -1 ? qualifiedName.substring(clusterSeparatorIdx + 1) : "";
            } else {
                this.clusterName = "";
            }
        }

        @Override
        public Object getAttribute(EntityAttribute attribute) {
            switch (attribute.getAttributeKey()) {
                case TransformationConstants.HIVE_DB_NAME_ATTRIBUTE:
                    return databaseName;

                case TransformationConstants.HIVE_DB_CLUSTER_NAME_ATTRIBUTE:
                    return clusterName;
            }

            return super.getAttribute(attribute);
        }

        @Override
        public void setAttribute(EntityAttribute attribute, String attributeValue) {
            switch (attribute.getAttributeKey()) {
                case TransformationConstants.HIVE_DB_NAME_ATTRIBUTE:
                    databaseName = attributeValue;

                    isCustomAttributeUpdated = true;
                break;

                case TransformationConstants.HIVE_DB_CLUSTER_NAME_ATTRIBUTE:
                    clusterName = attributeValue;

                    isCustomAttributeUpdated = true;
                break;

                default:
                    super.setAttribute(attribute, attributeValue);
                break;
            }
        }

        @Override
        public void transformComplete() {
            if (isCustomAttributeUpdated) {
                entity.setAttribute(TransformationConstants.NAME_ATTRIBUTE, databaseName);
                entity.setAttribute(TransformationConstants.CLUSTER_NAME_ATTRIBUTE, clusterName);
                entity.setAttribute(TransformationConstants.QUALIFIED_NAME_ATTRIBUTE, toQualifiedName());
            }
        }

        private String toQualifiedName() {
            return String.format("%s@%s", databaseName, clusterName);
        }
    }
}