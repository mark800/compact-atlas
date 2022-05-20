/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.compactatlas.webapp.service;

import com.google.common.base.Preconditions;
import org.apache.compactatlas.intg.ApplicationProperties;
import org.apache.compactatlas.intg.AtlasException;
import org.apache.compactatlas.common.ha.HAConfiguration;
import org.apache.compactatlas.repository.repository.audit.AtlasAuditService;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.inject.Singleton;

import static org.apache.compactatlas.common.AtlasConstants.ATLAS_MIGRATION_MODE_FILENAME;

/**
 * A class that maintains the state of this instance.
 *
 * The states are maintained at a granular level, including in-transition states. The transitions are
 * directed by {link ActiveInstanceElectorService}.
 */
@Component
public class ServiceState {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceState.class);

    @Autowired
    AtlasAuditService auditService;

    public enum ServiceStateValue {
        ACTIVE,
        PASSIVE,
        BECOMING_ACTIVE,
        BECOMING_PASSIVE,
        MIGRATING
    }

    private Configuration configuration;
    private volatile ServiceStateValue state;

    public ServiceState() throws AtlasException {
        this(ApplicationProperties.get());
    }

    public ServiceState(Configuration configuration) {
        this.configuration = configuration;

        state = !HAConfiguration.isHAEnabled(configuration) ? ServiceStateValue.ACTIVE : ServiceStateValue.PASSIVE;

        if(!StringUtils.isEmpty(configuration.getString(ATLAS_MIGRATION_MODE_FILENAME, ""))) {
            state = ServiceStateValue.MIGRATING;
        }
    }

    public ServiceStateValue getState() {
        return state;
    }

    public void becomingActive() {
        LOG.warn("Instance becoming active from {}", state);
        setState(ServiceStateValue.BECOMING_ACTIVE);
    }

    private void setState(ServiceStateValue newState) {
        Preconditions.checkState(HAConfiguration.isHAEnabled(configuration), "Cannot change state as requested, as HA is not enabled for this instance.");

        state = newState;

        //auditServerStatus();
    }

//    private void auditServerStatus() {
//
//        if (state == ServiceStateValue.ACTIVE) {
//            Date   date        = new Date();
//            try {
//                auditService.add(AtlasAuditEntry.AuditOperation.SERVER_START, EmbeddedServer.SERVER_START_TIME, date, null, null, 0);
//                auditService.add(AtlasAuditEntry.AuditOperation.SERVER_STATE_ACTIVE, date, date, null, null, 0);
//            } catch (AtlasBaseException e) {
//                LOG.error("Exception occurred during audit", e);
//            }
//        }
//    }

    public void setActive() {
        LOG.warn("Instance is active from {}", state);
        setState(ServiceStateValue.ACTIVE);
    }

    public void becomingPassive() {
        LOG.warn("Instance becoming passive from {}", state);
        setState(ServiceStateValue.BECOMING_PASSIVE);
    }

    public void setPassive() {
        LOG.warn("Instance is passive from {}", state);
        setState(ServiceStateValue.PASSIVE);
    }

    public boolean isInstanceInTransition() {
        ServiceStateValue state = getState();
        return state == ServiceStateValue.BECOMING_ACTIVE
                || state == ServiceStateValue.BECOMING_PASSIVE;
    }

    public void setMigration() {
        LOG.warn("Instance in {}", state);
        setState(ServiceStateValue.MIGRATING);
    }

    public boolean isInstanceInMigration() {
        return getState() == ServiceStateValue.MIGRATING;
    }
}
