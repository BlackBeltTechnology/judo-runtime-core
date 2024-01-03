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
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.dispatcher.context.ThreadContext;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import static java.util.Objects.requireNonNullElse;

@Module
public class ThreadContextModule {
    public static final String THREAD_CONTEXT_DEBUG_THREAD_FORK = "threadContextDebugThreadFork";
    public static final String THREAD_CONTEXT_INHERITABLE_CONTEXT = "threadContextInheritableContext";

    @JudoApplicationScope
    @Provides
    public Context providesContext(
            DataTypeManager dataTypeManager,
            @Named(THREAD_CONTEXT_DEBUG_THREAD_FORK) @Nullable Boolean debugThreadFork,
            @Named(THREAD_CONTEXT_INHERITABLE_CONTEXT) @Nullable Boolean inheritableContext
    ) {
        return new ThreadContext(requireNonNullElse(debugThreadFork, false), requireNonNullElse(inheritableContext, true), dataTypeManager);
    }
}
