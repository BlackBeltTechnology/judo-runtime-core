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
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Map;
import java.util.function.Function;

@NoArgsConstructor
@RequiredArgsConstructor
public class RequestParametersVariableProvider<T> implements Function<String, T> {

    public static final String REQUEST_PARAMETERS_KEY = "__requestParameters";

    @NonNull
    @Setter
    Context context;

    @Override
    public T apply(final String parameterName) {
        Object value = null;
        Map map = context.getAs(Map.class, REQUEST_PARAMETERS_KEY);
        if (map != null && map.containsKey(parameterName)) {
            value = (String) context.getAs(Map.class, REQUEST_PARAMETERS_KEY).get(parameterName);
        }
        return (T) value;

    }
}
