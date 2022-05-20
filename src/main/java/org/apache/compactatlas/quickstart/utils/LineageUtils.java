package org.apache.compactatlas.quickstart.utils;

import org.apache.compactatlas.intg.model.instance.AtlasEntity;
import org.apache.compactatlas.intg.type.AtlasTypeUtil;

import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.compactatlas.quickstart.hook.events.BaseHiveEvent.*;

public class LineageUtils {
    public static final char QNAME_SEP_METADATA_NAMESPACE = '@';
    public static final char QNAME_SEP_ENTITY_NAME = '.';
    public static final char QNAME_SEP_PROCESS = ':';
    public static final String TEMP_TABLE_PREFIX = "_temp-";
    public static final String CREATE_OPERATION = "CREATE";
    public static final String ALTER_OPERATION = "ALTER";

    public static final String CREATETABLE = "CREATETABLE";
    public static final String CREATETABLE_AS_SELECT = "CREATETABLE_AS_SELECT";
    public static final String CREATEVIEW = "CREATEVIEW";
    public static final String ALTERVIEW_AS = "ALTERVIEW_AS";
    public static final String ALTERTABLE_LOCATION = "ALTERTABLE_LOCATION";

    public static final SimpleDateFormat dtFormat = new SimpleDateFormat("yyyy-MM-dd");

    public static AtlasEntity getHiveProcessEntity(List<AtlasEntity> inputs, List<AtlasEntity> outputs, String queryStr) throws Exception {
        AtlasEntity ret = new AtlasEntity(HIVE_TYPE_PROCESS);
        //getQueryString();
        String qualifiedName = getQualifiedName(inputs, outputs);


        ret.setAttribute(ATTRIBUTE_OPERATION_TYPE, CREATETABLE);

        ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, qualifiedName);
        ret.setAttribute(ATTRIBUTE_NAME, qualifiedName);
        ret.setRelationshipAttribute(ATTRIBUTE_INPUTS, AtlasTypeUtil.getAtlasRelatedObjectIds(inputs, RELATIONSHIP_DATASET_PROCESS_INPUTS));
        ret.setRelationshipAttribute(ATTRIBUTE_OUTPUTS, AtlasTypeUtil.getAtlasRelatedObjectIds(outputs, RELATIONSHIP_PROCESS_DATASET_OUTPUTS));

        // We are setting an empty value to these attributes, since now we have a new entity type called hive process
        // execution which captures these values. We have to set empty values here because these attributes are
        // mandatory attributes for hive process entity type.
        ret.setAttribute(ATTRIBUTE_START_TIME, System.currentTimeMillis());
        ret.setAttribute(ATTRIBUTE_END_TIME, System.currentTimeMillis());

        ret.setAttribute(ATTRIBUTE_USER_NAME, EMPTY_ATTRIBUTE_VALUE);
        ret.setAttribute(ATTRIBUTE_QUERY_TEXT, EMPTY_ATTRIBUTE_VALUE);
        ret.setAttribute(ATTRIBUTE_QUERY_ID, EMPTY_ATTRIBUTE_VALUE);

        ret.setAttribute(ATTRIBUTE_QUERY_PLAN, "Not Supported");
        ret.setAttribute(ATTRIBUTE_RECENT_QUERIES, Collections.singletonList(queryStr));
        //ret.setAttribute(ATTRIBUTE_CLUSTER_NAME, getMetadataNamespace());
        return ret;
    }

    public static String getQualifiedName(List<AtlasEntity> inputs, List<AtlasEntity> outputs) throws Exception {
        if (outputs != null && outputs.size() > 0) {
            String tableName = AtlasEntityUtils.getEntityTableName(outputs.get(0));
            //return tableName + QNAME_SEP_PROCESS + dtFormat.format(new Date());
            return "generate" + QNAME_SEP_PROCESS + tableName;
        }
        return null;
    }
}
