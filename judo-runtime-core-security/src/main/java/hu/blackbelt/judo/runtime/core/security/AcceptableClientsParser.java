package hu.blackbelt.judo.runtime.core.security;

/*-
 * #%L
 * JUDO Runtime Core :: Security
 * %%
 * Copyright (C) 2018 - 2023 BlackBelt Technology
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

import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public final class AcceptableClientsParser {

    private AcceptableClientsParser() {
    }

    public static Map<String, Set<String>> parseAcceptableClients(final String acceptableClients) {
        if (acceptableClients == null || acceptableClients.trim().isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, Set<String>> result = new LinkedHashMap<>();
        final Map<String, String> clientToActor = new LinkedHashMap<>();

        for (String entry : acceptableClients.split(";")) {
            final String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            final String[] parts = trimmed.split("=", 2);
            if (parts.length != 2 || parts[0].trim().isEmpty() || parts[1].trim().isEmpty()) {
                throw new IllegalArgumentException("Invalid acceptable clients entry: " + trimmed);
            }
            final String actorFQN = parts[0].trim();
            final Set<String> clients = new LinkedHashSet<>();
            for (String client : parts[1].split(",")) {
                final String clientName = client.trim().replaceAll("-", ".");
                if (!clientName.isEmpty()) {
                    if (clientToActor.containsKey(clientName)) {
                        throw new IllegalArgumentException(
                                "Ambiguous acceptable client mapping: client '" + clientName
                                        + "' is mapped to both '" + clientToActor.get(clientName)
                                        + "' and '" + actorFQN + "'");
                    }
                    clientToActor.put(clientName, actorFQN);
                    clients.add(clientName);
                }
            }
            if (!clients.isEmpty()) {
                result.merge(actorFQN, clients, (existing, newSet) -> {
                    existing.addAll(newSet);
                    return existing;
                });
            }
        }

        if (!result.isEmpty()) {
            log.info("Acceptable clients configured: {}", result);
        }

        return Collections.unmodifiableMap(result);
    }

    public static Map<String, String> buildClientToActorMap(final Map<String, Set<String>> actorToClientsMap) {
        if (actorToClientsMap == null || actorToClientsMap.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, String> clientToActor = new LinkedHashMap<>();
        for (Map.Entry<String, Set<String>> entry : actorToClientsMap.entrySet()) {
            for (String client : entry.getValue()) {
                clientToActor.put(client, entry.getKey());
            }
        }
        return Collections.unmodifiableMap(clientToActor);
    }
}
