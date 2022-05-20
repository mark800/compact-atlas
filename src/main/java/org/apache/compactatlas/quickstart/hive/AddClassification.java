package org.apache.compactatlas.quickstart.hive;

import org.apache.compactatlas.quickstart.model.HiveDataTypes;
import org.apache.compactatlas.quickstart.model.ProductClassification;
import org.apache.compactatlas.quickstart.utils.AtlasEntityUtils;
import org.apache.compactatlas.quickstart.utils.ClassificationUtils;
import org.apache.compactatlas.client.AtlasClientV2;
import org.apache.compactatlas.client.common.AtlasServiceException;
import org.apache.compactatlas.intg.model.instance.AtlasClassification;
import org.apache.compactatlas.intg.model.instance.AtlasEntity;
import org.apache.compactatlas.intg.model.instance.AtlasEntityHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.apache.compactatlas.quickstart.utils.AtlasEntityUtils.findEntity;

@Component
public class AddClassification {
    private static final Logger LOG = LoggerFactory.getLogger(ImportHiveTable.class);

    //lazy init, do not use this directly
    private AtlasClientV2 atlasClientV2 = null;

    @Value("${server.port}")
    private String server_port;

    private AtlasClientV2 getClient() {
        if (atlasClientV2 == null) {
            String[] atlasUrl = {"http://localhost:" + server_port};
            try {
                atlasClientV2 = new AtlasClientV2(atlasUrl);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return atlasClientV2;
    }

    public void addClassificationForHiveTables() {
        String database = "poke";
        String ns = "maens";
        LOG.info("add classification for Db:" + database + ", in namespace:" + ns);
        try {
            String typeName = HiveDataTypes.HIVE_DB.getName();
            AtlasEntity.AtlasEntityWithExtInfo ret =
                    findEntity(typeName, AtlasEntityUtils.getDBQualifiedName(ns, database), true, true, getClient());
            if (ret == null) {
                LOG.info("can not find the database in atlas:" + database);
                return;
            }
            //add classification def first
            ClassificationUtils.addClassificationTypes(getClient());
            setDataWareHouseLayer(ret);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setDataWareHouseLayer(AtlasEntity.AtlasEntityWithExtInfo db) throws AtlasServiceException {
        List<AtlasEntityHeader> allTables = AtlasEntityUtils.getAllTablesInDb(db.getEntity().getGuid(), getClient());
        for (AtlasEntityHeader table : allTables) {
            String guid = table.getGuid();
            AtlasEntity.AtlasEntityWithExtInfo tableEntityExt = getClient().getEntityByGuid(guid);

            if (tableEntityExt == null) {
                LOG.warn("can not find the entity in database, entity guid:" + guid);
            }
            AtlasClassification classification = new AtlasClassification(
                    ProductClassification.toProductLayer(
                            AtlasEntityUtils.getEntityTableName(tableEntityExt.getEntity())
                    )
            );
            getClient().updateClassification(guid, classification);
        }
    }


}
