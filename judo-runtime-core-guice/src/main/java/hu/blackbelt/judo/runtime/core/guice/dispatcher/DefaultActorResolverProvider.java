package hu.blackbelt.judo.runtime.core.guice.dispatcher;

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

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AuthenticationInterceptorProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.runtime.core.guice.JudoConfigurationQualifiers;
import hu.blackbelt.judo.runtime.core.security.PrincipalLocaleConfig;

import javax.annotation.Nullable;

public class DefaultActorResolverProvider implements Provider<ActorResolver> {

    @Inject
    AsmModel asmModel;

    @SuppressWarnings("rawtypes")
    @Inject
    DAO dao;

    @Inject
    DataTypeManager dataTypeManager;

    @Inject
    AuthenticationInterceptorProvider authenticationInterceptorProvider;

    @Inject(optional = true)
    @JudoConfigurationQualifiers.ActorResolverCheckMappedActors
    @Nullable
    Boolean checkMappedActors = false;

    @Inject(optional = true)
    @Nullable
    IdentifierProvider identifierProvider;

    /**
     * JNG-6415 locale settings grouped by the {@code consolidate-locale-config-object} change.
     * Optional — absent binding means the feature is unconfigured, matching pre-refactor
     * "all four scalars null" behaviour.
     */
    @Inject(optional = true)
    @Nullable
    PrincipalLocaleConfig localeConfig;

    @SuppressWarnings("unchecked")
    @Override
    public ActorResolver get() {
        return DefaultActorResolver.builder()
                .dataTypeManager(dataTypeManager)
                .dao(dao)
                .asmModel(asmModel)
                .checkMappedActors(checkMappedActors)
                .authenticationInterceptorProvider(authenticationInterceptorProvider)
                .identifierProvider(identifierProvider)
                .localeConfig(localeConfig)
                .build();
    }
}
