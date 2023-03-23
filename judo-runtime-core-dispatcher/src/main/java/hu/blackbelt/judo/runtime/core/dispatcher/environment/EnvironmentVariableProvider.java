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

import lombok.NoArgsConstructor;

import java.util.function.Function;

@NoArgsConstructor
public class EnvironmentVariableProvider implements Function<String, Object> {

    @Override
    public Object apply(final String key) {
        final String value;
        if (System.getenv().containsKey(key)) {
            value = System.getenv(key);
        } else {
            value = System.getProperty(key);
        }

        return value;
    }
}
