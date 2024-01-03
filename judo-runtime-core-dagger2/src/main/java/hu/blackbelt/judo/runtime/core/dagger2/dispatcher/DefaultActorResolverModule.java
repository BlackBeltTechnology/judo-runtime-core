package hu.blackbelt.judo.runtime.core.dagger2.dispatcher;

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

import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;

import javax.annotation.Nullable;

import javax.inject.Inject;
import javax.inject.Named;

import static java.util.Objects.requireNonNullElse;

@Module
public class DefaultActorResolverModule {

    public static final String ACTOR_RESOLVER_CHECK_MAPPED_ACTORS = "actorResolverCheckMappedActors";

    @SuppressWarnings("unchecked")
    @JudoApplicationScope
    @Provides
    public ActorResolver providesActorResolver(
            AsmModel asmModel,
            DAO dao,
            DataTypeManager dataTypeManager,
            @Named(ACTOR_RESOLVER_CHECK_MAPPED_ACTORS) @Nullable Boolean checkMappedActors
    ) {
        return DefaultActorResolver.builder()
                .dataTypeManager(dataTypeManager)
                .dao(dao)
                .asmModel(asmModel)
                .checkMappedActors(requireNonNullElse(checkMappedActors, false))
                .build();
    }
}
