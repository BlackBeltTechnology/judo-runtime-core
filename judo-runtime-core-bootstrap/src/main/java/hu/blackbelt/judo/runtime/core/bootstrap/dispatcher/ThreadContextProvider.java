package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

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
import com.google.inject.name.Named;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dispatcher.context.ThreadContext;

import javax.annotation.Nullable;

public class ThreadContextProvider implements Provider<Context> {
    public static final String THREAD_CONTEXT_DEBUG_THREAD_FORK = "threadContextDebugThreadFork";
    public static final String THREAD_CONTEXT_INHERITABLE_CONTEXT = "threadContextInheritableContext";

    @Inject
    DataTypeManager dataTypeManager;

    @Inject(optional = true)
    @Named(THREAD_CONTEXT_DEBUG_THREAD_FORK)
    @Nullable
    Boolean debugThreadFork = false;

    @Inject(optional = true)
    @Named(THREAD_CONTEXT_INHERITABLE_CONTEXT)
    @Nullable
    Boolean inheritableContext = true;

    @Override
    public Context get() {
        return new ThreadContext(debugThreadFork, inheritableContext, dataTypeManager);
    }
}
