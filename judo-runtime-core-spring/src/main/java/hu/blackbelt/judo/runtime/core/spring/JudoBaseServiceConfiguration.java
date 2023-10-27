package hu.blackbelt.judo.runtime.core.spring;

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

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.UUIDIdentifierProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.*;
import hu.blackbelt.judo.runtime.core.dispatcher.context.ThreadContext;
import hu.blackbelt.judo.runtime.core.security.*;
import hu.blackbelt.judo.runtime.core.export.JxlsExport;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import hu.blackbelt.mapper.impl.DefaultCoercer;
import org.eclipse.emf.common.util.BasicEMap;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EOperation;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;


@Configuration
public class JudoBaseServiceConfiguration {

    @Bean
    public Coercer getExtendableCoercer() {
        return new DefaultCoercer();
    }

    @Bean
    public DataTypeManager getDataTypeManager(ExtendableCoercer coercer) {
        return new DataTypeManager(coercer);
    }

    @Bean
    public IdentifierProvider<UUID> getIdenfiableProvider() {
        return new UUIDIdentifierProvider();
    }

    @Bean
    @SuppressWarnings("unchecked")
    public MetricsCollector getMetricsCollector(
            Context context
    ) {

        // TODO: Parameters
        Consumer metricsConsumer = (m) -> {};
        Boolean enabled = false;
        Boolean verbose = false;

        return DefaultMetricsCollector.builder()
                .context(context)
                .metricsConsumer(metricsConsumer)
                .enabled(enabled)
                .verbose(verbose)
                .build();
    }

    @Bean
    public DispatcherFunctionProvider getDispatcherFunctionProvider() {
        final EMap<EOperation, Function<Payload, Payload>> scripts = new BasicEMap<>();
        DispatcherFunctionProvider dispatcherFunctionProvider = new DispatcherFunctionProvider() {
            @Override
            public EMap<EOperation, Function<Payload, Payload>> getSdkFunctions() {
                return ECollections.emptyEMap();
            }

            @Override
            public EMap<EOperation, Function<Payload, Payload>> getScriptFunctions() {
                return scripts;
            }
        };
        return dispatcherFunctionProvider;
    }

    @Bean
    public OperationCallInterceptorProvider getOperationCallInterceptorProvider() {
        return new OperationCallInterceptorProvider() {};
    }

    @Bean
    public Context getContext(DataTypeManager dataTypeManager) {
        // TODO: Parameters
        Boolean debugThreadFork = false;
        Boolean inheritableContext = true;

        return new ThreadContext(debugThreadFork, inheritableContext, dataTypeManager);
    }

    @Bean
    public PasswordPolicy getPasswordPolicy() {
        return new NoPasswordPolicy();
    }

    @Bean
    public Export getExport() {
        return new JxlsExport();
    }

}


