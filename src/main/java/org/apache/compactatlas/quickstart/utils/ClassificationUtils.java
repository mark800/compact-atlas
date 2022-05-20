package org.apache.compactatlas.quickstart.utils;

import org.apache.compactatlas.client.AtlasClientV2;
import org.apache.compactatlas.client.common.AtlasServiceException;
import org.apache.compactatlas.intg.model.instance.AtlasClassification;
import org.apache.commons.collections.CollectionUtils;
import org.apache.compactatlas.quickstart.model.ProductClassification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ClassificationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ClassificationUtils.class);


    public static void addClassificationTypes(AtlasClientV2 atlasClientV2) {
        try {
            atlasClientV2.createAtlasTypeDefs(ProductClassification.getProductDefinitions());
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
    }

    public static List<AtlasClassification> toProductClassifications(List<String> classificationNames) {
        List<AtlasClassification> ret = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(classificationNames)) {
            for (String classificationName : classificationNames) {
                ret.add(new AtlasClassification(classificationName));
            }
        }
        return ret;
    }


}
