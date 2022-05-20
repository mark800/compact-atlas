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
package org.apache.compactatlas.webapp.security;

import org.apache.compactatlas.webapp.filters.StaleTransactionCleanupFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.session.SessionRegistryImpl;
import org.springframework.security.web.authentication.session.RegisterSessionAuthenticationStrategy;
import org.springframework.security.web.authentication.session.SessionAuthenticationStrategy;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.servletapi.SecurityContextHolderAwareRequestFilter;

import java.util.*;


@EnableWebSecurity
//@EnableGlobalMethodSecurity(prePostEnabled = true)
//@KeycloakConfiguration
@Configuration
public class AtlasSecurityConfig extends WebSecurityConfigurerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasSecurityConfig.class);

    @Autowired
    private AtlasAuthenticationProvider authenticationProvider;
    @Autowired
    private AtlasAuthenticationSuccessHandler successHandler;
    @Autowired
    private AtlasAuthenticationFailureHandler failureHandler;

    @Override
    protected void configure(AuthenticationManagerBuilder authenticationManagerBuilder) {
        authenticationManagerBuilder.authenticationProvider(authenticationProvider);
    }

    @Override
    public void configure(WebSecurity web) throws Exception {

        List<String> matchers = new ArrayList<>(
                Arrays.asList("/css/**", "/n/css/**",
                        "/img/**",
                        "/n/img/**",
                        "/libs/**",
                        "/n/libs/**",
                        "/js/**",
                        "/n/js/**",
                        "/ieerror.html",
                        "/migration-status.html",
                        "/api/atlas/admin/status"));

//        if (!keycloakEnabled) {
//            matchers.add("/login.jsp");
//        }

        web.ignoring()
                .antMatchers(matchers.toArray(new String[matchers.size()]));
    }

    protected void configure(HttpSecurity httpSecurity) throws Exception {
        httpSecurity.csrf().disable();

        // The pages does not require login
        httpSecurity.authorizeRequests().antMatchers("/", "/login", "/logout", "/logout.html", "/api/**").permitAll();

        httpSecurity
                .authorizeRequests().anyRequest().authenticated()
                .and()
                .formLogin()
                .loginPage("/login")
                .loginProcessingUrl("/security_check_url")
                .defaultSuccessUrl("/index.html", true)
                .failureUrl("/login?error=true")
                //.loginProcessingUrl("/j_spring_security_check")
                //.successHandler(successHandler)
                //.failureHandler(failureHandler)
                .usernameParameter("username")
                .passwordParameter("password")
                .and()
                .logout().logoutUrl("/logout")
                .logoutSuccessUrl("/logoutSuccessful");

        httpSecurity
                .addFilterAfter(new StaleTransactionCleanupFilter(), BasicAuthenticationFilter.class);
        //.addFilterAfter(new AtlasAuthenticationFilter(), SecurityContextHolderAwareRequestFilter.class);
        //.addFilterBefore(ssoAuthenticationFilter, BasicAuthenticationFilter.class)
        //.addFilterAfter(csrfPreventionFilter, AtlasAuthenticationFilter.class);

//        if (keycloakEnabled) {
//            httpSecurity
//                    .logout().addLogoutHandler(keycloakLogoutHandler()).and()
//                    .addFilterBefore(keycloakAuthenticationProcessingFilter(), BasicAuthenticationFilter.class)
//                    .addFilterBefore(keycloakPreAuthActionsFilter(), LogoutFilter.class)
//                    .addFilterAfter(keycloakSecurityContextRequestFilter(), SecurityContextHolderAwareRequestFilter.class)
//                    .addFilterAfter(keycloakAuthenticatedActionsRequestFilter(), KeycloakSecurityContextRequestFilter.class);
//        }
    }


    @Bean
    protected SessionAuthenticationStrategy sessionAuthenticationStrategy() {
        return new RegisterSessionAuthenticationStrategy(new SessionRegistryImpl());
    }

//    @Bean
//    protected AdapterDeploymentContext adapterDeploymentContext() throws Exception {
//        AdapterDeploymentContextFactoryBean factoryBean = null;
//        String fileName = configuration.getString("atlas.authentication.method.keycloak.file");
//        if (fileName != null && !fileName.isEmpty()) {
//            //keycloakConfigFileResource = new FileSystemResource(fileName);
//            //factoryBean = new AdapterDeploymentContextFactoryBean(keycloakConfigFileResource);
//        } else {
//            Configuration conf = configuration.subset("atlas.authentication.method.keycloak");
//            AdapterConfig cfg = new AdapterConfig();
//            cfg.setRealm(conf.getString("realm", "atlas.com"));
//            cfg.setAuthServerUrl(conf.getString("auth-server-url", "https://localhost/auth"));
//            cfg.setResource(conf.getString("resource", "none"));
//
//            Map<String, Object> credentials = new HashMap<>();
//            credentials.put("secret", conf.getString("credentials-secret", "nosecret"));
//            cfg.setCredentials(credentials);
//            KeycloakDeployment dep = KeycloakDeploymentBuilder.build(cfg);
//            factoryBean = new AdapterDeploymentContextFactoryBean(new KeycloakConfigResolver() {
//                @Override
//                public KeycloakDeployment resolve(HttpFacade.Request request) {
//                    return dep;
//                }
//            });
//        }
//
//        factoryBean.afterPropertiesSet();
//        return factoryBean.getObject();
//    }
//
//    @Bean
//    protected KeycloakPreAuthActionsFilter keycloakPreAuthActionsFilter() {
//        return new KeycloakPreAuthActionsFilter(httpSessionManager());
//    }
//
//    @Bean
//    protected HttpSessionManager httpSessionManager() {
//        return new HttpSessionManager();
//    }
//
//    protected KeycloakLogoutHandler keycloakLogoutHandler() throws Exception {
//        return new KeycloakLogoutHandler(adapterDeploymentContext());
//    }
//
//    @Bean
//    protected KeycloakSecurityContextRequestFilter keycloakSecurityContextRequestFilter() {
//        return new KeycloakSecurityContextRequestFilter();
//    }
//
//    @Bean
//    protected KeycloakAuthenticatedActionsFilter keycloakAuthenticatedActionsRequestFilter() {
//        return new KeycloakAuthenticatedActionsFilter();
//    }
//
//    @Bean
//    protected KeycloakAuthenticationProcessingFilter keycloakAuthenticationProcessingFilter() throws Exception {
//        KeycloakAuthenticationProcessingFilter filter = new KeycloakAuthenticationProcessingFilter(authenticationManagerBean(), KEYCLOAK_REQUEST_MATCHER);
//        filter.setSessionAuthenticationStrategy(sessionAuthenticationStrategy());
//        return filter;
//    }
}
