package hu.blackbelt.judo.runtime.core.dispatcher.security;

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
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import org.eclipse.emf.ecore.EClass;

import java.util.Map;
import java.util.Optional;

/**
 * Actor (instance) resolver.
 */
public interface ActorResolver {

    /**
     * Authenticate actor (check if exists in database, load data).
     *
     * @param exchange exchange
     */
    void authenticateActor(Map<String, Object> exchange);

    /**
     * Authenticate actor by principal (check if exists in database, load data).
     *
     * @param principal JUDO principal
     */
    Optional<Payload> authenticateByPrincipal(JudoPrincipal principal);

    /**
     * Get actor instance by claims (token).
     *
     * @param actorType actor type
     * @param claims    claims
     * @return payload of actor (instance)
     */
    Payload getActorByClaims(EClass actorType, Map<String, Object> claims);
}
