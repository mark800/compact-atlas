/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.compactatlas.repository.repository.store.graph.v2;


import org.apache.compactatlas.repository.entitytransform.BaseEntityHandler;
import org.apache.compactatlas.intg.exception.AtlasBaseException;
import org.apache.compactatlas.intg.model.impexp.AtlasExportResult;
import org.apache.compactatlas.intg.model.instance.AtlasEntity;
import org.apache.compactatlas.intg.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.compactatlas.intg.model.typedef.AtlasTypesDef;
import org.apache.compactatlas.repository.repository.impexp.ImportTransforms;

import java.util.List;

public interface EntityImportStream extends EntityStream {

    int size();

    void setPosition(int position);

    int getPosition();

    void setPositionUsingEntityGuid(String guid);

    AtlasEntityWithExtInfo getNextEntityWithExtInfo();

    AtlasEntity.AtlasEntityWithExtInfo getEntityWithExtInfo(String guid) throws AtlasBaseException;

    void onImportComplete(String guid);

    void setImportTransform(ImportTransforms importTransform);

    public ImportTransforms getImportTransform();

    void setEntityHandlers(List<BaseEntityHandler> entityHandlers);

    List<BaseEntityHandler> getEntityHandlers();

    AtlasTypesDef getTypesDef() throws AtlasBaseException;

    AtlasExportResult getExportResult() throws AtlasBaseException;

    List<String> getCreationOrder();

    void close();
}
