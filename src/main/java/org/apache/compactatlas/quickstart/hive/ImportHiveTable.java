package org.apache.compactatlas.quickstart.hive;

import org.apache.compactatlas.quickstart.utils.AtlasEntityUtils;
import org.apache.compactatlas.client.AtlasClientV2;
import org.apache.compactatlas.quickstart.model.HiveDataTypes;
import org.apache.compactatlas.intg.model.instance.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;

import static org.apache.compactatlas.quickstart.hook.events.BaseHiveEvent.ATTRIBUTE_QUALIFIED_NAME;
import static org.apache.compactatlas.quickstart.utils.AtlasEntityUtils.findEntity;

@Component
public class ImportHiveTable {
    private static final Logger LOG = LoggerFactory.getLogger(ImportHiveTable.class);

    public static final String TEMP_TABLE_PREFIX = "_temp-";
    public static final String SEP = ":".intern();
    public static final String HIVE_TABLE_DB_EDGE_LABEL = "__hive_table.db";
    public static final String TableInfoPath = "./initModelsData/quickstart_files/table_definition_files";

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

    public void importAHiveDbToAtlas() {
        String databaseToImport = "poke";
        String ns = "maens";
        LOG.info("Importing Hive Db:" + databaseToImport + ", in namespace:" + ns);

        AtlasEntity.AtlasEntityWithExtInfo dbEntity;
        try {
            dbEntity = registerDatabase(databaseToImport, ns, false);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        if (dbEntity != null) {
            importTables(dbEntity.getEntity(), databaseToImport, ns, TableInfoPath);
        }
    }

    /**
     * Imports all tables for the given db
     */
    public void importTables(AtlasEntity dbEntity, String dbName, String nameSpace, String path) {
        List<String> tableNames = HiveTableUtils.getAllTables(dbName, path);
        LOG.info("get " + tableNames.size() + " tables from Data base:" + dbName);

        for (String tableName : tableNames) {
            HiveTableUtils.TableInfo hiveTableInfo = HiveTableUtils.getHiveTableInfo(dbName, tableName, path);
            try {
                AtlasEntity retEntity = importTable(dbEntity, hiveTableInfo, nameSpace);
            } catch (Exception e) {
                LOG.error("import failed: " + tableName);
                e.printStackTrace();
                return;
            }
            LOG.info("imported table:" + tableName);
        }
    }

    public AtlasEntity importTable(AtlasEntity dbEntity, HiveTableUtils.TableInfo table, String nameSpace) throws Exception {
        try {
            AtlasEntity.AtlasEntityWithExtInfo tableEntity = registerTable(dbEntity, table, nameSpace, false);

            return tableEntity.getEntity();
        } catch (Exception e) {
            LOG.error("Import failed for hive_table {}", table.getTableName(), e);
        }
        return null;
    }

    /**
     * Checks if db is already registered, else creates and registers db entity
     *
     * @param databaseName
     * @return
     * @throws Exception
     */
    private AtlasEntity.AtlasEntityWithExtInfo registerDatabase(String databaseName, String atlasNameSpace, boolean update) throws Exception {
        AtlasEntity.AtlasEntityWithExtInfo ret = null;

        if (databaseName != null) {
            if (!update) {
                ret = registerInstance(new AtlasEntity.AtlasEntityWithExtInfo(AtlasEntityUtils.toDbEntity(databaseName, null, atlasNameSpace)));
            } else {
                ret = findDatabase(atlasNameSpace, databaseName);
                LOG.info("Database {} is already registered - id={}. Updating it.", databaseName, ret.getEntity().getGuid());
                ret.setEntity(AtlasEntityUtils.toDbEntity(databaseName, ret.getEntity(), atlasNameSpace));
                AtlasEntityUtils.updateInstance(ret, getClient());
            }
        }
        return ret;
    }

    private AtlasEntity.AtlasEntityWithExtInfo registerTable(AtlasEntity dbEntity, HiveTableUtils.TableInfo table, String nameSpace, boolean update) {
        try {
            AtlasEntity.AtlasEntityWithExtInfo ret;

            if (!update) {
                AtlasEntity.AtlasEntityWithExtInfo tableEntity = AtlasEntityUtils.toTableEntity(dbEntity, table, nameSpace);

                ret = registerInstance(tableEntity);
            } else {
                AtlasEntity.AtlasEntityWithExtInfo tableEntity = AtlasEntityUtils.findTableEntity(nameSpace, table.getDbName(), table.getTableName(), getClient());
                LOG.info("Table {}.{} is already registered with id {}. Updating entity.", table.getDbName(), table.getTableName(), tableEntity.getEntity().getGuid());
                ret = AtlasEntityUtils.toTableEntity(dbEntity, table, tableEntity, nameSpace);
                AtlasEntityUtils.updateInstance(ret, getClient());
            }
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Registers an entity in atlas
     */
    private AtlasEntity.AtlasEntityWithExtInfo registerInstance(AtlasEntity.AtlasEntityWithExtInfo entity) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("creating {} entity: {}", entity.getEntity().getTypeName(), entity);
        }

        AtlasEntity.AtlasEntityWithExtInfo ret = null;
        EntityMutationResponse response = getClient().createEntity(entity);
        List<AtlasEntityHeader> createdEntities = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE);

        if (CollectionUtils.isNotEmpty(createdEntities)) {
            for (AtlasEntityHeader createdEntity : createdEntities) {
                if (ret == null) {
                    ret = getClient().getEntityByGuid(createdEntity.getGuid());

                    LOG.info("Created {} entity: name={}, guid={}", ret.getEntity().getTypeName(), ret.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), ret.getEntity().getGuid());
                } else if (ret.getEntity(createdEntity.getGuid()) == null) {
                    AtlasEntity.AtlasEntityWithExtInfo newEntity = getClient().getEntityByGuid(createdEntity.getGuid());

                    ret.addReferredEntity(newEntity.getEntity());

                    if (MapUtils.isNotEmpty(newEntity.getReferredEntities())) {
                        for (Map.Entry<String, AtlasEntity> entry : newEntity.getReferredEntities().entrySet()) {
                            ret.addReferredEntity(entry.getKey(), entry.getValue());
                        }
                    }

                    LOG.info("Created {} entity: name={}, guid={}", newEntity.getEntity().getTypeName(), newEntity.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), newEntity.getEntity().getGuid());
                }
            }
        }
        AtlasEntityUtils.clearRelationshipAttributes(ret);
        return ret;
    }


    /**
     * Gets the atlas entity for the database
     */
    public AtlasEntity.AtlasEntityWithExtInfo findDatabase(String metadataNamespace, String databaseName) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Searching Atlas for database {}", databaseName);
        }

        String typeName = HiveDataTypes.HIVE_DB.getName();
        AtlasEntity.AtlasEntityWithExtInfo ret =
                findEntity(typeName, AtlasEntityUtils.getDBQualifiedName(metadataNamespace, databaseName), true, true, getClient());
        if (ret == null || ret.getEntity() == null) {
            return null;
        } else {
            return ret;
        }
    }

    private AtlasEntity.AtlasEntityWithExtInfo findProcessEntity(String qualifiedName) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Searching Atlas for process {}", qualifiedName);
        }

        String typeName = HiveDataTypes.HIVE_PROCESS.getName();

        return findEntity(typeName, qualifiedName, true, true, getClient());
    }


}

