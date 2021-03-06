/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.compactatlas.common;

import org.apache.compactatlas.intg.ApplicationProperties;
import org.apache.compactatlas.intg.AtlasException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@Configuration
@EnableAspectJAutoProxy
public class CommonConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommonConfiguration.class);

    @Bean
    public org.apache.commons.configuration.Configuration getAtlasConfig() throws AtlasException {
        try {
            return ApplicationProperties.get();
        } catch (AtlasException e) {
            LOGGER.warn("AtlasConfig init failed", e);
            throw e;
        }
    }
}
