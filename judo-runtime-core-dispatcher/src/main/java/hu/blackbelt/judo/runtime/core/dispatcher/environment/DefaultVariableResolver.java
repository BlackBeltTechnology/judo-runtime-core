package hu.blackbelt.judo.runtime.core.dispatcher.environment;

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

import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dispatcher.VariableResolverManager;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

@NoArgsConstructor
@RequiredArgsConstructor
@Slf4j
public class DefaultVariableResolver implements VariableResolver, VariableResolverManager {

    @NonNull
    @Setter
    DataTypeManager dataTypeManager;

    @NonNull
    @Setter
    Context context;

    private Map<String, Supplier<Object>> suppliers = new ConcurrentHashMap<>();
    private Map<String, Function<String, Object>> functions = new ConcurrentHashMap<>();

    private Collection<String> cacheableKeys = new HashSet<>();

    @Override
    public <T> T resolve(final Class<T> type, final String category, final String key) {
        final Map<String, Object> variables = context.putIfAbsent(Dispatcher.VARIABLES_KEY, new HashMap<>());
        final Object value;
        if (!variables.containsKey(category + ":" + key)) {
            final boolean cacheable;
            if (suppliers.containsKey(category + ":" + key)) {
                value = suppliers.get(category + ":" + key).get();
                cacheable = cacheableKeys.contains(category + ":" + key);
            } else if (functions.containsKey(category)) {
                value = functions.get(category).apply(key);
                cacheable = cacheableKeys.contains(category + ":*");
            } else {
                log.warn("Undefined variable: {}.{}", category, key);
                value = null;
                cacheable = false;
            }

            if (cacheable) {
                variables.put(category + ":" + key, value);
            }
        } else {
            value = variables.get(category + ":" + key);
        }

        return dataTypeManager.getCoercer().coerce(value, type);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void registerSupplier(final String category, final String key, @SuppressWarnings("rawtypes") final Supplier supplier, final boolean cacheable) {
        suppliers.put(category + ":" + key, supplier);
        if (cacheable) {
            cacheableKeys.add(category + ":" + (key != null ? key : "*"));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void registerFunction(final String category, @SuppressWarnings("rawtypes") final Function function, final boolean cacheable) {
        functions.put(category, function);
        if (cacheable) {
            cacheableKeys.add(category + ":*");
        }
    }

    @Override
    public void unregisterSupplier(final String category, final String key) {
        suppliers.remove(category + ":" + key);
    }

    @Override
    public void unregisterFunction(final String category) {
        functions.remove(category);
    }

}
