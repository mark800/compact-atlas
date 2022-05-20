package org.apache.compactatlas.quickstart.utils;

import org.apache.compactatlas.quickstart.hive.ImportHiveTable;
import org.apache.compactatlas.quickstart.hive.HiveTableUtils;
import org.apache.compactatlas.quickstart.model.HiveDataTypes;
import org.apache.compactatlas.client.AtlasClientV2;
import org.apache.compactatlas.client.common.AtlasServiceException;
import org.apache.compactatlas.intg.model.discovery.AtlasSearchResult;
import org.apache.compactatlas.intg.model.instance.*;
import org.apache.compactatlas.intg.type.AtlasTypeUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.compactatlas.quickstart.hive.ImportHiveTable.HIVE_TABLE_DB_EDGE_LABEL;
import static org.apache.compactatlas.quickstart.hook.events.BaseHiveEvent.*;
import static org.apache.compactatlas.quickstart.hook.events.BaseHiveEvent.ATTRIBUTE_QUALIFIED_NAME;

public class AtlasEntityUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityUtils.class);

    /**
     * Create a Hive Database entity
     */
    public static AtlasEntity toDbEntity(String hiveDbName, AtlasEntity dbEntity, String atlasNameSpace) {
        if (dbEntity == null) {
            dbEntity = new AtlasEntity(HiveDataTypes.HIVE_DB.getName());
        }

        String dbName = hiveDbName;

        dbEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, getDBQualifiedName(atlasNameSpace, dbName));
        dbEntity.setAttribute(ATTRIBUTE_NAME, dbName);
        dbEntity.setAttribute(ATTRIBUTE_DESCRIPTION, "");
        dbEntity.setAttribute(ATTRIBUTE_OWNER, "");

        dbEntity.setAttribute(ATTRIBUTE_CLUSTER_NAME, atlasNameSpace);
        //dbEntity.setAttribute(ATTRIBUTE_LOCATION, HdfsNameServiceResolver.getPathWithNameServiceID(""));
        //dbEntity.setAttribute(ATTRIBUTE_PARAMETERS, hiveDB.getParameters());

//        if (hiveDB.getOwnerType() != null) {
//            dbEntity.setAttribute(ATTRIBUTE_OWNER_TYPE, OWNER_TYPE_TO_ENUM_VALUE.get(hiveDB.getOwnerType().getValue()));
//        }
        return dbEntity;
    }

    public static void updateInstance(AtlasEntity.AtlasEntityWithExtInfo entity, AtlasClientV2 client) throws AtlasServiceException {
        client.updateEntity(entity);
        LOG.info("Updated {} entity: name={}, guid={}", entity.getEntity().getTypeName(), entity.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), entity.getEntity().getGuid());
    }

    /**
     * Create a new table instance in Atlas
     */
    public static AtlasEntity.AtlasEntityWithExtInfo toTableEntity(AtlasEntity database, HiveTableUtils.TableInfo hiveTable, String atlasNameSpace) {
        return toTableEntity(database, hiveTable, null, atlasNameSpace);
    }

    public static AtlasEntity.AtlasEntityWithExtInfo toTableEntity(AtlasEntity database, HiveTableUtils.TableInfo hiveTable,
                                                                   AtlasEntity.AtlasEntityWithExtInfo table, String atlasNameSpace) {
        if (table == null) {
            table = new AtlasEntity.AtlasEntityWithExtInfo(new AtlasEntity(HiveDataTypes.HIVE_TABLE.getName()));
        }

        AtlasEntity tableEntity = table.getEntity();
        String tableQualifiedName = getTableQualifiedName(atlasNameSpace, hiveTable);
        long createTime = hiveTable.getCreateTime();
        long lastAccessTime = hiveTable.getLastAccessTime() > 0 ? hiveTable.getLastAccessTime() : createTime;

        tableEntity.setRelationshipAttribute(ATTRIBUTE_DB, AtlasTypeUtil.getAtlasRelatedObjectId(database, RELATIONSHIP_HIVE_TABLE_DB));
        tableEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, tableQualifiedName);
        tableEntity.setAttribute(ATTRIBUTE_NAME, hiveTable.getTableName().toLowerCase());
        tableEntity.setAttribute(ATTRIBUTE_OWNER, hiveTable.getOwner());

        tableEntity.setAttribute(ATTRIBUTE_CREATE_TIME, createTime);
        tableEntity.setAttribute(ATTRIBUTE_LAST_ACCESS_TIME, lastAccessTime);
        tableEntity.setAttribute(ATTRIBUTE_RETENTION, hiveTable.getRetention());
        //tableEntity.setAttribute(ATTRIBUTE_PARAMETERS, hiveTable.getParameters());
        tableEntity.setAttribute(ATTRIBUTE_COMMENT, hiveTable.getComment());
        tableEntity.setAttribute(ATTRIBUTE_TABLE_TYPE, hiveTable.getType());
        tableEntity.setAttribute(ATTRIBUTE_TEMPORARY, hiveTable.isTemporary());

        if (hiveTable.getViewOriginalText() != null) {
            tableEntity.setAttribute(ATTRIBUTE_VIEW_ORIGINAL_TEXT, hiveTable.getViewOriginalText());
        }

        if (hiveTable.getViewExpandedText() != null) {
            tableEntity.setAttribute(ATTRIBUTE_VIEW_EXPANDED_TEXT, hiveTable.getViewExpandedText());
        }

        //AtlasEntity sdEntity = toStorageDescEntity(hiveTable.getSd(), tableQualifiedName, getStorageDescQFName(tableQualifiedName), AtlasTypeUtil.getObjectId(tableEntity));
        List<AtlasEntity> partKeys = toColumns(hiveTable.getPartitionKeys(), tableEntity, RELATIONSHIP_HIVE_TABLE_PART_KEYS);
        List<AtlasEntity> columns = toColumns(hiveTable.getColumns(), tableEntity, RELATIONSHIP_HIVE_TABLE_COLUMNS);

        //tableEntity.setRelationshipAttribute(ATTRIBUTE_STORAGEDESC, AtlasTypeUtil.getAtlasRelatedObjectId(sdEntity, RELATIONSHIP_HIVE_TABLE_STORAGE_DESC));
        tableEntity.setRelationshipAttribute(ATTRIBUTE_PARTITION_KEYS, AtlasTypeUtil.getAtlasRelatedObjectIds(partKeys, RELATIONSHIP_HIVE_TABLE_PART_KEYS));
        tableEntity.setRelationshipAttribute(ATTRIBUTE_COLUMNS, AtlasTypeUtil.getAtlasRelatedObjectIds(columns, RELATIONSHIP_HIVE_TABLE_COLUMNS));


        table.addReferredEntity(database);
        //table.addReferredEntity(sdEntity);

        if (partKeys != null) {
            for (AtlasEntity partKey : partKeys) {
                table.addReferredEntity(partKey);
            }
        }

        if (columns != null) {
            for (AtlasEntity column : columns) {
                table.addReferredEntity(column);
            }
        }

        table.setEntity(tableEntity);

        return table;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo findEntity(final String typeName, final String qualifiedName,
                                                                boolean minExtInfo, boolean ignoreRelationship, AtlasClientV2 client) {
        AtlasEntity.AtlasEntityWithExtInfo ret = null;

        try {
            ret = client.getEntityByAttribute(typeName, Collections.singletonMap(ATTRIBUTE_QUALIFIED_NAME, qualifiedName), minExtInfo, ignoreRelationship);
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
        return ret;
    }

    /**
     * Gets Atlas Entity for the table
     */
    public static AtlasEntity.AtlasEntityWithExtInfo findTableEntity(String ns, String db, String table, AtlasClientV2 client) throws Exception {

        String typeName = HiveDataTypes.HIVE_TABLE.getName();
        String tblQualifiedName = AtlasEntityUtils.getTableQualifiedName(ns, db, table);

        return findEntity(typeName, tblQualifiedName, true, true, client);
    }
    /**
     * Registers an entity in atlas
     */
    private AtlasEntity.AtlasEntitiesWithExtInfo registerInstances(
            AtlasEntity.AtlasEntitiesWithExtInfo entities, AtlasClientV2 client) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("creating {} entities: {}", entities.getEntities().size(), entities);
        }

        AtlasEntity.AtlasEntitiesWithExtInfo ret = null;
        EntityMutationResponse response = client.createEntities(entities);
        List<AtlasEntityHeader> createdEntities = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE);

        if (CollectionUtils.isNotEmpty(createdEntities)) {
            ret = new AtlasEntity.AtlasEntitiesWithExtInfo();

            for (AtlasEntityHeader createdEntity : createdEntities) {
                AtlasEntity.AtlasEntityWithExtInfo entity = client.getEntityByGuid(createdEntity.getGuid());

                ret.addEntity(entity.getEntity());

                if (MapUtils.isNotEmpty(entity.getReferredEntities())) {
                    for (Map.Entry<String, AtlasEntity> entry : entity.getReferredEntities().entrySet()) {
                        ret.addReferredEntity(entry.getKey(), entry.getValue());
                    }
                }

                LOG.info("Created {} entity: name={}, guid={}", entity.getEntity().getTypeName(), entity.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), entity.getEntity().getGuid());
            }
        }
        AtlasEntityUtils.clearRelationshipAttributes(ret);
        return ret;
    }

    public static String getEntityTableName(AtlasEntity entity) {
        return (String) entity.getAttribute(ATTRIBUTE_NAME);
    }

    public static List<AtlasEntityHeader> getAllTablesInDb(String databaseGuid, AtlasClientV2 atlasClient) throws AtlasServiceException {

        List<AtlasEntityHeader> entities = new ArrayList<>();
        final int pageSize = 10000;

        for (int i = 0; ; i++) {
            int offset = pageSize * i;
            LOG.info("Retrieving tables: offset={}, pageSize={}", offset, pageSize);

            AtlasSearchResult searchResult = atlasClient.relationshipSearch(databaseGuid, HIVE_TABLE_DB_EDGE_LABEL, null, null, true, pageSize, offset);

            List<AtlasEntityHeader> entityHeaders = searchResult == null ? null : searchResult.getEntities();
            int tableCount = entityHeaders == null ? 0 : entityHeaders.size();
            LOG.info("Retrieved {} tables of {} database", tableCount, databaseGuid);

            if (tableCount > 0) {
                entities.addAll(entityHeaders);
            }
            if (tableCount < pageSize) { // last page
                break;
            }
        }
        return entities;
    }

    public static List<AtlasEntity> toColumns(List<HiveTableUtils.ColumnInfo> schemaList, AtlasEntity table, String relationshipType) {
        List<AtlasEntity> ret = new ArrayList<>();

        int columnPosition = 0;
        for (HiveTableUtils.ColumnInfo fs : schemaList) {
            LOG.debug("Processing field {}", fs);

            AtlasEntity column = new AtlasEntity(HiveDataTypes.HIVE_COLUMN.getName());

            column.setRelationshipAttribute(ATTRIBUTE_TABLE, AtlasTypeUtil.getAtlasRelatedObjectId(table, relationshipType));
            column.setAttribute(ATTRIBUTE_QUALIFIED_NAME, getColumnQualifiedName((String) table.getAttribute(ATTRIBUTE_QUALIFIED_NAME), fs.getColumnName()));
            column.setAttribute(ATTRIBUTE_NAME, fs.getColumnName());
            column.setAttribute(ATTRIBUTE_OWNER, table.getAttribute(ATTRIBUTE_OWNER));
            column.setAttribute(ATTRIBUTE_COL_TYPE, fs.getType());
            column.setAttribute(ATTRIBUTE_COL_POSITION, columnPosition++);
            column.setAttribute(ATTRIBUTE_COMMENT, fs.getComment());

            ret.add(column);
        }
        return ret;
    }

    /**
     * Construct the qualified name used to uniquely identify a Table instance in Atlas.
     */
    public static String getTableQualifiedName(String metadataNamespace, HiveTableUtils.TableInfo table) {
        return getTableQualifiedName(metadataNamespace, table.getDbName(), table.getTableName(), table.isTemporary());
    }

    public static String getHdfsPathQualifiedName(String hdfsPath, String atlasNameSpace) {
        return String.format("%s@%s", hdfsPath, atlasNameSpace);
    }

    /**
     * Construct the qualified name used to uniquely identify a Database instance in Atlas.
     */
    public static String getDBQualifiedName(String metadataNamespace, String dbName) {
        return String.format("%s@%s", dbName.toLowerCase(), metadataNamespace);
    }

    /**
     * Construct the qualified name used to uniquely identify a Table instance in Atlas.
     *
     * @param metadataNamespace Name of the cluster to which the Hive component belongs
     * @param dbName            Name of the Hive database to which the Table belongs
     * @param tableName         Name of the Hive table
     * @param isTemporaryTable  is this a temporary table
     * @return Unique qualified name to identify the Table instance in Atlas.
     */
    public static String getTableQualifiedName(String metadataNamespace, String dbName, String tableName, boolean isTemporaryTable) {
        String tableTempName = tableName;

        if (isTemporaryTable) {
//            if (SessionState.get() != null && SessionState.get().getSessionId() != null) {
//                tableTempName = tableName + TEMP_TABLE_PREFIX + SessionState.get().getSessionId();
//            } else {
            tableTempName = tableName + ImportHiveTable.TEMP_TABLE_PREFIX + RandomStringUtils.random(10);
//            }
        }
        return String.format("%s.%s@%s", dbName.toLowerCase(), tableTempName.toLowerCase(), metadataNamespace);
    }

    public static String getTableProcessQualifiedName(String metadataNamespace, HiveTableUtils.TableInfo table) {
        String tableQualifiedName = getTableQualifiedName(metadataNamespace, table);
        long createdTime = table.getCreateTime();

        return tableQualifiedName + ImportHiveTable.SEP + createdTime;
    }


    /**
     * Construct the qualified name used to uniquely identify a Table instance in Atlas.
     *
     * @param metadataNamespace Metadata namespace of the cluster to which the Hive component belongs
     * @param dbName            Name of the Hive database to which the Table belongs
     * @param tableName         Name of the Hive table
     * @return Unique qualified name to identify the Table instance in Atlas.
     */
    public static String getTableQualifiedName(String metadataNamespace, String dbName, String tableName) {
        return getTableQualifiedName(metadataNamespace, dbName, tableName, false);
    }

    public static String getStorageDescQFName(String tableQualifiedName) {
        return tableQualifiedName + "_storage";
    }

    public static String getColumnQualifiedName(final String tableQualifiedName, final String colName) {
        final String[] parts = tableQualifiedName.split("@");
        final String tableName = parts[0];
        final String metadataNamespace = parts[1];

        return String.format("%s.%s@%s", tableName, colName.toLowerCase(), metadataNamespace);
    }

    public static void clearRelationshipAttributes(AtlasEntity.AtlasEntitiesWithExtInfo entities) {
        if (entities != null) {
            if (entities.getEntities() != null) {
                for (AtlasEntity entity : entities.getEntities()) {
                    clearRelationshipAttributes(entity);
                    ;
                }
            }

            if (entities.getReferredEntities() != null) {
                clearRelationshipAttributes(entities.getReferredEntities().values());
            }
        }
    }


    public static void clearRelationshipAttributes(AtlasEntity.AtlasEntityWithExtInfo entity) {
        if (entity != null) {
            clearRelationshipAttributes(entity.getEntity());

            if (entity.getReferredEntities() != null) {
                clearRelationshipAttributes(entity.getReferredEntities().values());
            }
        }
    }

    public static void clearRelationshipAttributes(Collection<AtlasEntity> entities) {
        if (entities != null) {
            for (AtlasEntity entity : entities) {
                clearRelationshipAttributes(entity);
            }
        }
    }

    public static void clearRelationshipAttributes(AtlasEntity entity) {
        if (entity != null && entity.getRelationshipAttributes() != null) {
            entity.getRelationshipAttributes().clear();
        }
    }

}
