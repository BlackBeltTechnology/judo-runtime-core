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
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.*;

public class DefaultVariableResolverProvider implements Provider<VariableResolver> {

    @Inject
    DataTypeManager dataTypeManager;

    @Inject
    Context context;

    @SuppressWarnings("rawtypes")
    @Inject
    Sequence sequence;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public VariableResolver get() {
        DefaultVariableResolver variableResolver = new DefaultVariableResolver(dataTypeManager, context);
        variableResolver.registerSupplier("SYSTEM", "current_timestamp", new CurrentTimestampProvider(), false);
        variableResolver.registerSupplier("SYSTEM", "current_date", new CurrentDateProvider(), false);
        variableResolver.registerSupplier("SYSTEM", "current_time", new CurrentTimeProvider(), false);
        variableResolver.registerFunction("ENVIRONMENT", new EnvironmentVariableProvider(), true);
        variableResolver.registerFunction("SEQUENCE", new SequenceProvider(sequence), false);
        return variableResolver;
    }
}
