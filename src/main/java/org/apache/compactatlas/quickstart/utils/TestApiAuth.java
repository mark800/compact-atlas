package org.apache.compactatlas.quickstart.utils;

import org.apache.compactatlas.client.AtlasClientV2;
import org.apache.compactatlas.intg.model.instance.AtlasEntity;
import org.apache.compactatlas.quickstart.model.HiveDataTypes;

import static org.apache.compactatlas.quickstart.utils.AtlasEntityUtils.findEntity;

public class TestApiAuth {
    private static AtlasClientV2 atlasClientV2;

    private static AtlasClientV2 getApiClient() {
        if (atlasClientV2 == null) {
            String[] atlasUrl = {"http://localhost:9090"};
            try {
                atlasClientV2 = new AtlasClientV2(atlasUrl);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return atlasClientV2;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo findDatabase(String metadataNamespace, String databaseName) throws Exception {


        String typeName = HiveDataTypes.HIVE_DB.getName();
        AtlasEntity.AtlasEntityWithExtInfo ret =
                findEntity(typeName, AtlasEntityUtils.getDBQualifiedName(metadataNamespace, databaseName), true, true, getApiClient());
        if (ret == null || ret.getEntity() == null) {
            return null;
        } else {
            return ret;
        }
    }
}
