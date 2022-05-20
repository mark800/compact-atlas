package org.apache.compactatlas.quickstart.model;

import org.apache.compactatlas.intg.model.instance.AtlasClassification;
import org.apache.compactatlas.intg.model.typedef.AtlasClassificationDef;
import org.apache.compactatlas.intg.model.typedef.AtlasTypesDef;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.compactatlas.intg.type.AtlasTypeUtil.createTraitTypeDef;

public class ProductClassification {
    public static final String CLASSIFICATION_POKE = "product_poke";
    public static final String CLASSIFICATION_OTHER = "product_other";
    public static final String VERSION_1 = "1.0";

    public static AtlasTypesDef getProductDefinitions() {
        // Classification-Definitions
        AtlasClassificationDef pokeClassifDef = createTraitTypeDef(CLASSIFICATION_POKE, "POKE产品", VERSION_1, Collections.emptySet());
        AtlasClassificationDef otherDef = createTraitTypeDef(CLASSIFICATION_OTHER, "其它产品", VERSION_1, Collections.emptySet());

        List<AtlasClassificationDef> classificationDefs = asList(pokeClassifDef, otherDef);
        return new AtlasTypesDef(Collections.emptyList(), Collections.emptyList(), classificationDefs, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }


    public static AtlasClassification toProductLayer(String tableName) {
        if (tableName != null && tableName.length() > 0) {
            if (tableName.toLowerCase().contains("poke")) {
                return (new AtlasClassification(ProductClassification.CLASSIFICATION_POKE));
            } else {
                return (new AtlasClassification(ProductClassification.CLASSIFICATION_OTHER));
            }
        }
        return null;
    }
}
