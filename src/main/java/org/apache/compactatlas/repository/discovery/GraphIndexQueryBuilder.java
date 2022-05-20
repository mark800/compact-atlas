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
package org.apache.compactatlas.repository.discovery;

import static org.apache.compactatlas.common.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.compactatlas.common.repository.Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY;
import static org.apache.compactatlas.common.repository.Constants.STATE_PROPERTY_KEY;

import org.apache.compactatlas.common.repository.Constants;
import org.apache.compactatlas.intg.type.AtlasStructType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class GraphIndexQueryBuilder {
    SearchContext context;

    GraphIndexQueryBuilder(SearchContext context) {
        this.context = context;
    }

    void addClassificationTypeFilter(StringBuilder indexQuery) {
        if (indexQuery != null && CollectionUtils.isNotEmpty(context.getClassificationNames())) {
            String classificationNames = AtlasStructType.AtlasAttribute.escapeIndexQueryValue(context.getClassificationNames(), true);
            if (indexQuery.length() != 0) {
                indexQuery.append(" AND ");
            }

            indexQuery.append("(").append(SearchProcessor.INDEX_SEARCH_PREFIX).append('\"').append(CLASSIFICATION_NAMES_KEY).append('\"').append(':').append(classificationNames)
                .append(" OR ").append(SearchProcessor.INDEX_SEARCH_PREFIX).append('\"').append(PROPAGATED_CLASSIFICATION_NAMES_KEY)
                .append('\"').append(':').append(classificationNames).append(")");
        }
    }

    void addClassificationAndSubTypesQueryFilter(StringBuilder indexQuery) {
        if (indexQuery != null && CollectionUtils.isNotEmpty(context.getClassificationTypes())) {
            String classificationTypesQryStr = context.getClassificationTypesQryStr();

            if (indexQuery.length() != 0) {
                indexQuery.append(" AND ");
            }

            indexQuery.append("(").append(SearchProcessor.INDEX_SEARCH_PREFIX).append("\"").append(CLASSIFICATION_NAMES_KEY)
                .append("\"").append(":" + classificationTypesQryStr).append(" OR ").append(SearchProcessor.INDEX_SEARCH_PREFIX)
                .append("\"").append(PROPAGATED_CLASSIFICATION_NAMES_KEY).append("\"").append(":" + classificationTypesQryStr).append(")");
        }
    }

    void addClassificationFilterForBuiltInTypes(StringBuilder indexQuery) {
        if (indexQuery != null && CollectionUtils.isNotEmpty(context.getClassificationTypes())) {
            if (context.getClassificationTypes().iterator().next() == SearchContext.MATCH_ALL_NOT_CLASSIFIED) {
                if (indexQuery.length() != 0) {
                    indexQuery.append(" AND ");
                }
                indexQuery.append("( *:* ").append("-").append(SearchProcessor.INDEX_SEARCH_PREFIX).append("\"").append(CLASSIFICATION_NAMES_KEY)
                    .append("\"").append(":" + "[* TO *]").append(" AND ").append("-")
                    .append(SearchProcessor.INDEX_SEARCH_PREFIX).append("\"").append(PROPAGATED_CLASSIFICATION_NAMES_KEY)
                    .append("\"").append(":" + "[* TO *]").append(")");
            }
        }
    }

    void addActiveStateQueryFilter(StringBuilder indexQuery){
        if (context.getSearchParameters().getExcludeDeletedEntities() && indexQuery != null) {
            if (indexQuery.length() != 0) {
                indexQuery.append(" AND ");
            }
            indexQuery.append("(").append(SearchProcessor.INDEX_SEARCH_PREFIX).append("\"").append(STATE_PROPERTY_KEY)
                      .append("\"").append(":" + "ACTIVE").append(")");
        }
    }

    void addTypeAndSubTypesQueryFilter(StringBuilder indexQuery, String typeAndAllSubTypesQryStr) {
        if (indexQuery != null && StringUtils.isNotEmpty(typeAndAllSubTypesQryStr)) {
            if (indexQuery.length() > 0) {
                indexQuery.append(" AND ");
            }

            indexQuery.append("(").append(SearchProcessor.INDEX_SEARCH_PREFIX + "\"").append(Constants.TYPE_NAME_PROPERTY_KEY)
                .append("\":").append(typeAndAllSubTypesQryStr).append(")");
        }
    }
}