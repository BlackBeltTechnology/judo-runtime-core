package hu.blackbelt.judo.runtime.core.dispatcher;

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

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class DefaultActorResolverAcceptableClientsTest {

    @Test
    void parseValidConfig() {
        Map<String, Set<String>> result = DefaultActorResolver.parseAcceptableClients(
                "MyModel.UserActor=frontend.app,mobile.app;MyModel.AdminActor=admin.tool");

        assertEquals(2, result.size());
        assertEquals(Set.of("frontend.app", "mobile.app"), result.get("MyModel.UserActor"));
        assertEquals(Set.of("admin.tool"), result.get("MyModel.AdminActor"));
    }

    @Test
    void parseNullReturnsEmptyMap() {
        Map<String, Set<String>> result = DefaultActorResolver.parseAcceptableClients(null);
        assertTrue(result.isEmpty());
    }

    @Test
    void parseEmptyStringReturnsEmptyMap() {
        Map<String, Set<String>> result = DefaultActorResolver.parseAcceptableClients("");
        assertTrue(result.isEmpty());
    }

    @Test
    void parseBlankStringReturnsEmptyMap() {
        Map<String, Set<String>> result = DefaultActorResolver.parseAcceptableClients("   ");
        assertTrue(result.isEmpty());
    }

    @Test
    void parseAmbiguousMappingThrows() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                DefaultActorResolver.parseAcceptableClients(
                        "MyModel.UserActor=shared.app;MyModel.AdminActor=shared.app"));

        assertTrue(ex.getMessage().contains("shared.app"));
        assertTrue(ex.getMessage().contains("Ambiguous"));
    }

    @Test
    void parseSingleEntry() {
        Map<String, Set<String>> result = DefaultActorResolver.parseAcceptableClients(
                "MyModel.UserActor=frontend.app");

        assertEquals(1, result.size());
        assertEquals(Set.of("frontend.app"), result.get("MyModel.UserActor"));
    }

    @Test
    void parseTrimsWhitespace() {
        Map<String, Set<String>> result = DefaultActorResolver.parseAcceptableClients(
                " MyModel.UserActor = frontend.app , mobile.app ; MyModel.AdminActor = admin.tool ");

        assertEquals(2, result.size());
        assertEquals(Set.of("frontend.app", "mobile.app"), result.get("MyModel.UserActor"));
        assertEquals(Set.of("admin.tool"), result.get("MyModel.AdminActor"));
    }

    @Test
    void parseTrailingSemicolon() {
        Map<String, Set<String>> result = DefaultActorResolver.parseAcceptableClients(
                "MyModel.UserActor=frontend.app;");

        assertEquals(1, result.size());
        assertEquals(Set.of("frontend.app"), result.get("MyModel.UserActor"));
    }

    @Test
    void parseDashesConvertedToDots() {
        Map<String, Set<String>> result = DefaultActorResolver.parseAcceptableClients(
                "MyModel.UserActor=frontend-app,my-mobile-app;MyModel.AdminActor=admin-tool");

        assertEquals(2, result.size());
        assertEquals(Set.of("frontend.app", "my.mobile.app"), result.get("MyModel.UserActor"));
        assertEquals(Set.of("admin.tool"), result.get("MyModel.AdminActor"));
    }

    @Test
    void parseMixedDashesAndDotsWork() {
        Map<String, Set<String>> result = DefaultActorResolver.parseAcceptableClients(
                "MyModel.UserActor=frontend.app,my-mobile-app");

        assertEquals(1, result.size());
        assertEquals(Set.of("frontend.app", "my.mobile.app"), result.get("MyModel.UserActor"));
    }

    @Test
    void parseInvalidEntryMissingEqualsThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                DefaultActorResolver.parseAcceptableClients("InvalidEntry"));
    }

    @Test
    void resultMapIsUnmodifiable() {
        Map<String, Set<String>> result = DefaultActorResolver.parseAcceptableClients(
                "MyModel.UserActor=frontend.app");

        assertThrows(UnsupportedOperationException.class, () ->
                result.put("new", Set.of("test")));
    }
}
