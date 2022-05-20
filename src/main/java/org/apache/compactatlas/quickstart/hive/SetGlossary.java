package org.apache.compactatlas.quickstart.hive;

import org.apache.compactatlas.quickstart.model.GlossaryBusinessType;
import org.apache.compactatlas.quickstart.model.HiveDataTypes;
import org.apache.compactatlas.quickstart.utils.AtlasEntityUtils;
import org.apache.compactatlas.quickstart.utils.GlossaryUtils;
import org.apache.compactatlas.client.AtlasClientV2;
import org.apache.compactatlas.client.common.AtlasServiceException;
import org.apache.compactatlas.intg.model.instance.AtlasEntity;
import org.apache.compactatlas.intg.model.instance.AtlasEntityHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.apache.compactatlas.quickstart.hook.events.BaseHiveEvent.ATTRIBUTE_NAME;
import static org.apache.compactatlas.quickstart.utils.AtlasEntityUtils.findEntity;

@Component
public class SetGlossary {
    private static final Logger LOG = LoggerFactory.getLogger(SetGlossary.class);

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

    public void setGlossaryForHiveTables() {
        String database = "poke";
        String ns = "maens";
        if (database == null || ns == null) {
            LOG.error("can not find db's name or namespace");
        }
        LOG.info("add Glossary for Db:" + database + ", in namespace:" + ns);

        try {
            String typeName = HiveDataTypes.HIVE_DB.getName();
            AtlasEntity.AtlasEntityWithExtInfo ret =
                    findEntity(typeName, AtlasEntityUtils.getDBQualifiedName(ns, database), true, true, getClient());
            if (ret == null) {
                LOG.info("can not find the database in atlas:" + database);
                return;
            }
            setGlossaryTermToEntity(ret.getEntity());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setGlossaryTermToEntity(AtlasEntity dbEntity) throws AtlasServiceException {
        List<AtlasEntityHeader> allTables = AtlasEntityUtils.getAllTablesInDb(dbEntity.getGuid(), getClient());
        for (AtlasEntityHeader table : allTables) {
            String guid = table.getGuid();
            AtlasEntity.AtlasEntityWithExtInfo tableEntityExt = getClient().getEntityByGuid(guid);

            if (tableEntityExt == null) {
                LOG.warn("can not find the entity in database, entity guid:" + guid);
            }
            String tableName = (String) tableEntityExt.getEntity().getAttribute(ATTRIBUTE_NAME);
            //add glossary term
            boolean haveTerm = false;
            for (String term : GlossaryBusinessType.GloTerms) {
                if (tableName.contains(term)) {
                    GlossaryUtils.addTermToEntity(GlossaryBusinessType.GloName, term, tableEntityExt.getEntity(), atlasClientV2);
                    haveTerm = true;
                }
            }
            if (!haveTerm) {
                GlossaryUtils.addTermToEntity(GlossaryBusinessType.GloName, GlossaryBusinessType.TermOther, tableEntityExt.getEntity(), atlasClientV2);
            }
        }
    }
}
