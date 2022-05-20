package org.apache.compactatlas.quickstart.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.compactatlas.client.AtlasClientV2;
import org.apache.compactatlas.intg.model.glossary.AtlasGlossary;
import org.apache.compactatlas.intg.model.glossary.AtlasGlossaryCategory;
import org.apache.compactatlas.intg.model.glossary.AtlasGlossaryTerm;
import org.apache.compactatlas.intg.model.glossary.relations.AtlasGlossaryHeader;
import org.apache.compactatlas.intg.model.glossary.relations.AtlasRelatedTermHeader;
import org.apache.compactatlas.intg.model.instance.AtlasEntity;
import org.apache.compactatlas.intg.model.instance.AtlasRelatedObjectId;
import org.apache.compactatlas.intg.type.AtlasTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class GlossaryUtils {
    private static final Logger LOG = LoggerFactory.getLogger(GlossaryUtils.class);


    public static AtlasGlossary createGlossary(String name, String description, AtlasClientV2 atlasClientV2) {
        AtlasGlossary glossary = new AtlasGlossary();
        glossary.setName(name);
        glossary.setLanguage("English");
        glossary.setShortDescription(description);

        AtlasGlossary ret = null;
        try {
            ret = atlasClientV2.createGlossary(glossary);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    public static AtlasGlossary.AtlasGlossaryExtInfo getGlossaryDetail(AtlasGlossary glossary, AtlasClientV2 atlasClientV2) {
        AtlasGlossary.AtlasGlossaryExtInfo extInfo = null;
        try {
            extInfo = atlasClientV2.getGlossaryExtInfo(glossary.getGuid());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return extInfo;
    }

    public static AtlasGlossaryTerm createGlossaryTerm(AtlasGlossary glossary, String name, AtlasClientV2 atlasClientV2) {
        AtlasGlossaryTerm term = new AtlasGlossaryTerm();
        AtlasGlossaryHeader glossaryHeader = new AtlasGlossaryHeader();

        glossaryHeader.setGlossaryGuid(glossary.getGuid());
        glossaryHeader.setDisplayText(glossary.getName());
        term.setAnchor(glossaryHeader);
        term.setName(name);

        AtlasGlossaryTerm ret = null;
        try {
            ret = atlasClientV2.createGlossaryTerm(term);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    public static void termToEntities(String guid, List<AtlasRelatedObjectId> entities, AtlasClientV2 atlasClientV2) {
        try {
            atlasClientV2.assignTermToEntities(guid, entities);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void addTermToEntity(String glossaryName, String termName, AtlasEntity atlasEntity, AtlasClientV2 atlasClientV2) {
        AtlasGlossary atlasGlossary = queryGlossary(glossaryName, atlasClientV2);
        AtlasGlossaryTerm term = null;

        if (atlasGlossary == null) {
            //create g and term
            atlasGlossary = createGlossary(glossaryName, glossaryName + " description", atlasClientV2);
        } else {
            // query term
            Set<AtlasRelatedTermHeader> atlasRelatedTermHeaders = atlasGlossary.getTerms();
            if (atlasRelatedTermHeaders != null) {
                List<String> guids = new LinkedList<>();
                for (AtlasRelatedTermHeader header : atlasRelatedTermHeaders) {
                    guids.add(header.getTermGuid());
                }
                for (String aguid : guids) {
                    AtlasGlossaryTerm aterm = queryGlossaryTerm(aguid, atlasClientV2);
                    if (aterm.getName().equals(termName)) {
                        term = aterm;
                        break;
                    }
                }
            }
        }
        if (term == null) {
            //create term
            term = GlossaryUtils.createGlossaryTerm(atlasGlossary, termName, atlasClientV2);
        }

        List<AtlasRelatedObjectId> assignedEntities = new LinkedList<>();
        assignedEntities.add(AtlasTypeUtil.toAtlasRelatedObjectId(atlasEntity));
        termToEntities(term.getGuid(), assignedEntities, atlasClientV2);
    }


    public static AtlasGlossaryTerm queryGlossaryTerm(String guid, AtlasClientV2 atlasClientV2) {
        AtlasGlossaryTerm ret = null;
        try {
            ret = atlasClientV2.getGlossaryTerm(guid);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    public static AtlasGlossary queryGlossary(String glossaryName, AtlasClientV2 atlasClientV2) {
        try {
            List<AtlasGlossary> rets = atlasClientV2.getAllGlossaries("ASC", -1, 0);

            if (rets != null && rets.size() > 0) {
                for (int i = 0; i < rets.size(); i++) {
                    ObjectMapper mapper = new ObjectMapper();
                    String jsonObj = mapper.writeValueAsString(rets.get(i));
                    AtlasGlossary atlasGlossary = mapper.readValue(jsonObj, AtlasGlossary.class);
                    if (atlasGlossary.getName().equals(glossaryName)) {
                        return atlasGlossary;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static AtlasGlossaryCategory createGlossaryCategory(AtlasGlossary glossary, String name, AtlasClientV2 atlasClientV2) {
        AtlasGlossaryHeader glossaryHeader = new AtlasGlossaryHeader();
        AtlasGlossaryCategory category = new AtlasGlossaryCategory();

        glossaryHeader.setGlossaryGuid(glossary.getGuid());
        glossaryHeader.setDisplayText(glossary.getName());

        category.setAnchor(glossaryHeader);
        category.setName(name);

        AtlasGlossaryCategory ret = null;
        try {
            ret = atlasClientV2.createGlossaryCategory(category);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    public static void deleteGlossary(String guid, AtlasClientV2 atlasClientV2) {
        if (guid != null) {
            try {
                atlasClientV2.deleteGlossaryByGuid(guid);
                LOG.info("glossary " + guid + " is deleted!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}

