package hu.blackbelt.judo.runtime.core.jetty.guice;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */


import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.*;

public class JudoJettyModule extends AbstractModule {

    @Getter
    JudoJettyModuleConfiguration configuration;

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class JudoJettyModuleConfiguration {
        public static final JudoJettyModuleConfiguration DEFAULT = JudoJettyModuleConfiguration.builder().build();
        @Builder.Default Integer jettyServerPort = 8181;
        @Builder.Default String jettyContextPath = "/";
        @Builder.Default Integer maxThreads = 100;
        @Builder.Default Integer minThreads = 10;
        @Builder.Default Integer idleTimeout = 120;
    }

    public static class JudoJettyModuleBuilder {
        JudoJettyModuleConfiguration configuration = null;
        Integer jettyServerPort = JudoJettyModuleConfiguration.DEFAULT.getJettyServerPort();
        String jettyContextPath = JudoJettyModuleConfiguration.DEFAULT.getJettyContextPath();
        Integer maxThreads = JudoJettyModuleConfiguration.DEFAULT.getMaxThreads();
        Integer minThreads = JudoJettyModuleConfiguration.DEFAULT.getMinThreads();
        Integer idleTimeout = JudoJettyModuleConfiguration.DEFAULT.getIdleTimeout();
    }

    @Inject
    @Getter
    JettyContainer jettyContainer;

    @Builder
    private JudoJettyModule(JudoJettyModuleConfiguration configuration,
                            Integer jettyServerPort,
                            String jettyContextPath,
                            Integer maxThreads,
                            Integer minThreads,
                            Integer idleTimeout
                             ) {
        if (configuration != null) {
            this.configuration = configuration;
        } else {
            this.configuration = JudoJettyModuleConfiguration.builder()
                    .jettyServerPort(jettyServerPort)
                    .jettyContextPath(jettyContextPath)
                    .maxThreads(maxThreads)
                    .minThreads(minThreads)
                    .idleTimeout(idleTimeout)
                    .build();
        }
    }

    protected void configure() {
        configureServer();
        configureOptions();
    }

    protected void configureOptions() {
        bind(Integer.class).annotatedWith(JettyConfigurations.JettyServerPort.class).toInstance(configuration.getJettyServerPort());
        bind(String.class).annotatedWith(JettyConfigurations.JettyServerContextPath.class).toInstance(configuration.getJettyContextPath());
        bind(Integer.class).annotatedWith(JettyConfigurations.JettyServerMaxThreads.class).toInstance(configuration.getMaxThreads());
        bind(Integer.class).annotatedWith(JettyConfigurations.JettyServerMinThreads.class).toInstance(configuration.getMinThreads());
        bind(Integer.class).annotatedWith(JettyConfigurations.JettyServerIdleTimeout.class).toInstance(configuration.getIdleTimeout());
    }

    protected void configureServer() {
        bind(JettyContainer.class).toProvider(JettyContainerProvider.class).asEagerSingleton();
    }

}
