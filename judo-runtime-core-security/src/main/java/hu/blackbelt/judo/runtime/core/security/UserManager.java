package hu.blackbelt.judo.runtime.core.security;

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

import org.eclipse.emf.ecore.EClass;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * User manager for actors.
 */
public interface UserManager<ID> {

    /**
     * Get user by username.
     *
     * @param actor    actor type
     * @param username username
     * @return actor instance loaded from user store
     */
    Optional<Map<String, Object>> getUser(final EClass actor, final ID username);

    /**
     * Get all users.
     *
     * @param actor actor type
     * @return actor instances loaded from user store
     */
    List<Map<String, Object>> getAllUsers(final EClass actor);

    /**
     * Create a new user.
     *
     * @param actor actor type
     * @param user  user data
     */
    void createUser(EClass actor, Map<String, Object> user);

    /**
     * Update a user.
     *
     * @param actor    actor type
     * @param username username
     * @param user     user data
     */
    void updateUser(EClass actor, ID username, Map<String, Object> user);

    /**
     * Delete a user.
     *
     * @param actor    actor type
     * @param username user data
     */
    void deleteUser(EClass actor, ID username);

    /**
     * Get managed actors of principals
     *
     * @param principalType principal type
     * @return managed actor (if found)
     */
    Optional<EClass> getManagedActorOfPrincipal(EClass principalType);

    /**
     * Get mapping of principal and actor types
     *
     * @param principalType principal type
     * @return mappings
     */
    Map<String, String> getPrincipalAttributeMapping(EClass principalType);

    /**
     * Get username of a given user (principal)
     *
     * @param principalType principal type
     * @param user          user data
     * @return username
     */
    Optional<ID> getUsername(EClass principalType, Map<String, Object> user);
}
