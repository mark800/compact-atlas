package org.apache.compactatlas.quickstart.hive;

import org.apache.compactatlas.quickstart.model.HiveDataTypes;
import org.apache.compactatlas.quickstart.utils.AtlasEntityUtils;
import org.apache.compactatlas.quickstart.utils.LineageUtils;
import org.apache.compactatlas.quickstart.utils.ParseAirflowDependency;
import org.apache.compactatlas.client.AtlasClientV2;
import org.apache.compactatlas.client.common.AtlasServiceException;
import org.apache.compactatlas.intg.model.instance.AtlasEntity;
import org.apache.compactatlas.intg.model.instance.AtlasEntityHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.*;

import static org.apache.compactatlas.quickstart.utils.AtlasEntityUtils.findEntity;

@Component
public class SetLineage {
    private static final Logger LOG = LoggerFactory.getLogger(SetLineage.class);
    public static final String DepInfoPath = "./initModelsData/quickstart_files/airflow_dependency/dag_config.txt";

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


    public void setLineageForHiveTables() {
        String database = "poke";
        String ns = "maens";
        if (database == null || ns == null) {
            LOG.error("can not find db's name or namespace");
        }
        LOG.info("add Lineage for Db:" + database + ", in namespace:" + ns);

        try {
            String typeName = HiveDataTypes.HIVE_DB.getName();
            AtlasEntity.AtlasEntityWithExtInfo ret =
                    findEntity(typeName, AtlasEntityUtils.getDBQualifiedName(ns, database), true, true, getClient());
            if (ret == null) {
                LOG.info("can not find the database in atlas:" + database);
                return;
            }
            setLineageFromAirflowDep(DepInfoPath, ns, database);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Async
    public void setLineageFromAirflowDep(String fileName, String ns, String dbName) throws Exception {
        Map<String, Set<String>> depInfo = ParseAirflowDependency.getDepinfoFromAirflowConfigFile(fileName);
        for (String output : depInfo.keySet()) {
            AtlasEntity.AtlasEntityWithExtInfo outputEntityExt = AtlasEntityUtils.findTableEntity(ns, dbName, output, getClient());
            if (outputEntityExt == null || outputEntityExt.getEntity() == null) {
                LOG.warn("can not find the output table entity:" + output);
                continue;
            }
            Set<String> inputs = depInfo.get(output);
            List<AtlasEntity> inputEntites = new LinkedList<>();
            for (String table : inputs) {
                AtlasEntity.AtlasEntityWithExtInfo entityExt = AtlasEntityUtils.findTableEntity(ns, dbName, table, getClient());
                if (entityExt == null || entityExt.getEntity() == null) {
                    LOG.warn("can not find the output table entity:" + output);
                } else {
                    inputEntites.add(entityExt.getEntity());
                }
            }
            if (inputEntites.size() > 0 && outputEntityExt != null) {
                AtlasEntity lineageProcess = LineageUtils.getHiveProcessEntity(inputEntites, Collections.singletonList(outputEntityExt.getEntity()), "select b from a");
                getClient().createEntity(new AtlasEntity.AtlasEntityWithExtInfo(lineageProcess));
            }
        }
    }

    private void setTestLineage(AtlasEntity.AtlasEntityWithExtInfo db) throws AtlasServiceException {
        List<AtlasEntityHeader> allTables = AtlasEntityUtils.getAllTablesInDb(db.getEntity().getGuid(), getClient());

        List<AtlasEntity.AtlasEntityWithExtInfo> allTableEntityExt = new LinkedList<>();
        for (AtlasEntityHeader table : allTables) {
            String guid = table.getGuid();
            AtlasEntity.AtlasEntityWithExtInfo tableEntityExt = getClient().getEntityByGuid(guid);
            allTableEntityExt.add(tableEntityExt);
        }
        AtlasEntity from = allTableEntityExt.get(0).getEntity();
        AtlasEntity to = allTableEntityExt.get(1).getEntity();
        try {
            AtlasEntity lineageProcess = LineageUtils.getHiveProcessEntity(Collections.singletonList(from), Collections.singletonList(to), "select b from a");
            getClient().createEntity(new AtlasEntity.AtlasEntityWithExtInfo(lineageProcess));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
