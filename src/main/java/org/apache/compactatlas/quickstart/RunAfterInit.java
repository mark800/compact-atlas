package org.apache.compactatlas.quickstart;

import org.apache.compactatlas.quickstart.hive.AddClassification;
import org.apache.compactatlas.quickstart.hive.ImportHiveTable;
import org.apache.compactatlas.quickstart.hive.SetGlossary;
import org.apache.compactatlas.quickstart.hive.SetLineage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class RunAfterInit {
    private static final Logger LOG = LoggerFactory.getLogger(RunAfterInit.class);
    @Autowired
    ImportHiveTable importHiveTable;
    @Autowired
    AddClassification addClassification;
    @Autowired
    SetGlossary setGlossary;
    @Autowired
    SetLineage setLineage;

    @Value("${add.demo.data.after.init}")
    private boolean runInit;

    @EventListener(ApplicationReadyEvent.class)
    public void doAfterStartup() {
        if (runInit) {
            importHiveTable.importAHiveDbToAtlas();
            addClassification.addClassificationForHiveTables();
            setGlossary.setGlossaryForHiveTables();
            setLineage.setLineageForHiveTables();
            LOG.info("add demo data finished.");
        }
    }
}
