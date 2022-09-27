package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

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

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultDispatcher;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.Map;

@RequiredArgsConstructor
public class CollectedIdRemover<ID> {

    @NonNull
    private final String key;

    public void removeIdentifiers(final Payload payload, final Collection<ID> ids) {
        final Object id = payload.get(key);
        if (id != null && ids.contains(id)) {
            payload.remove("__$created");
            payload.remove(key);
            payload.remove(DefaultDispatcher.UPDATEABLE_KEY);
            payload.remove(DefaultDispatcher.DELETEABLE_KEY);
        }

        payload.values().forEach(v -> processRemoval(v, ids));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	private void processRemoval(final Object o, final Collection<ID> ids) {
        if (o instanceof Payload) {
            removeIdentifiers((Payload) o, ids);
        } else if (o instanceof Map) {
            removeIdentifiers(Payload.asPayload((Map<String, Object>) o), ids);
        } else if (o instanceof Collection) {
            ((Collection) o).forEach(i -> processRemoval(i, ids));
        }
    }
}
