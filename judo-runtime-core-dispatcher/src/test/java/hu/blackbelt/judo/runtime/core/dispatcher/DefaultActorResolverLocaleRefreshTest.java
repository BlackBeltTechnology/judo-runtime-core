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

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.runtime.core.security.PrincipalLocaleConfig;
import org.eclipse.emf.ecore.EClass;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Behavioural spec for the login-time principal-locale refresh decision and its safe application.
 * Exercises the two testable seams of {@code DefaultActorResolver}:
 * <ul>
 *     <li>{@code computeLocaleRefresh(...)} — pure decision returning the update payload or null.</li>
 *     <li>{@code applyLocaleRefresh(...)} — wraps {@code dao.update} so a failure never breaks auth.</li>
 * </ul>
 * The ASM-model introspection glue (exists/mapped/transient → {@code persistable}) is covered by a
 * real-model integration test; here that outcome is injected as the {@code persistable} flag.
 * See {@code openspec/changes/add-principal-locale-resolution/specs/principal-locale-resolution/spec.md}.
 */
class DefaultActorResolverLocaleRefreshTest {

    private static final String ID_KEY = "__identifier";
    private static final String ATTR = "locale";
    private static final String DEFAULT = "en-US";

    /**
     * Convenience: config with the standard {en-US, hu-HU} supported set. The {@code browserCheck}
     * boolean maps to the {@link hu.blackbelt.judo.runtime.core.security.LocaleResolutionLevel}
     * ceiling per the historical convention: {@code true → BROWSER}, {@code false →
     * IDENTITY_PROVIDER}.
     */
    private static PrincipalLocaleConfig cfg(final String attribute, final boolean browserCheck) {
        return PrincipalLocaleConfig.builder()
                .principalLocaleAttribute(attribute)
                .supportedLanguages("en-US,hu-HU")
                .defaultLanguage(DEFAULT)
                .localeResolutionLevel(browserCheck
                        ? hu.blackbelt.judo.runtime.core.security.LocaleResolutionLevel.BROWSER
                        : hu.blackbelt.judo.runtime.core.security.LocaleResolutionLevel.IDENTITY_PROVIDER)
                .build();
    }

    @Nested
    @DisplayName("computeLocaleRefresh (decision)")
    class ComputeDecision {

        @Test
        void blankAttributeReturnsNull() {
            final Payload update = DefaultActorResolver.computeLocaleRefresh(
                    cfg("  ", false), true, ID_KEY, "id-1", "en-US", "hu-HU", null);
            assertThat(update, is(nullValue()));
        }

        @Test
        void nullAttributeReturnsNull() {
            final Payload update = DefaultActorResolver.computeLocaleRefresh(
                    cfg(null, false), true, ID_KEY, "id-1", "en-US", "hu-HU", null);
            assertThat(update, is(nullValue()));
        }

        @Test
        void notPersistableReturnsNull() {
            final Payload update = DefaultActorResolver.computeLocaleRefresh(
                    cfg(ATTR, false), false, ID_KEY, "id-1", "en-US", "hu-HU", null);
            assertThat(update, is(nullValue()));
        }

        @Test
        void resolvedEqualsStoredReturnsNull() {
            final Payload update = DefaultActorResolver.computeLocaleRefresh(
                    cfg(ATTR, false), true, ID_KEY, "id-1", "hu-HU", "hu-HU", null);
            assertThat(update, is(nullValue()));
        }

        @Test
        void resolvedDiffersFromStoredReturnsUpdatePayload() {
            final Payload update = DefaultActorResolver.computeLocaleRefresh(
                    cfg(ATTR, false), true, ID_KEY, "id-1", "en-US", "hu-HU", null);
            assertThat(update, is(notNullValue()));
            assertThat(update.get(ID_KEY), is(equalTo("id-1")));
            assertThat((String) update.get(ATTR), is(equalTo("hu-HU")));
        }

        @Test
        void browserHintWinsWhenGateOn() {
            final Payload update = DefaultActorResolver.computeLocaleRefresh(
                    cfg(ATTR, true), true, ID_KEY, "id-1", "en-US", "en-US", "hu-HU");
            assertThat(update, is(notNullValue()));
            assertThat((String) update.get(ATTR), is(equalTo("hu-HU")));
        }

        @Test
        void browserHintIgnoredWhenGateOff() {
            final Payload update = DefaultActorResolver.computeLocaleRefresh(
                    cfg(ATTR, false), true, ID_KEY, "id-1", "en-US", "en-US", "hu-HU");
            // browser hint ignored, claim==stored==en-US → resolved en-US == stored → no update
            assertThat(update, is(nullValue()));
        }

        @Test
        void storedTierUsedWhenBrowserAndClaimEmpty() {
            // resolved from stored hu-HU == stored → no update
            final Payload none = DefaultActorResolver.computeLocaleRefresh(
                    cfg(ATTR, true), true, ID_KEY, "id-1", "hu-HU", null, null);
            assertThat(none, is(nullValue()));
        }

        @Test
        void firstLoginEmptyStoredSeedsFromClaim() {
            final Payload update = DefaultActorResolver.computeLocaleRefresh(
                    cfg(ATTR, true), true, ID_KEY, "id-1", null, "hu-HU", null);
            assertThat(update, is(notNullValue()));
            assertThat((String) update.get(ATTR), is(equalTo("hu-HU")));
        }

        @Test
        void unsupportedClaimFallsBackToDefaultAndSeedsWhenStoredEmpty() {
            final Payload update = DefaultActorResolver.computeLocaleRefresh(
                    cfg(ATTR, true), true, ID_KEY, "id-1", null, "de-DE", null);
            assertThat(update, is(notNullValue()));
            assertThat((String) update.get(ATTR), is(equalTo("en-US")));
        }
    }

    /**
     * JNG-6415 review feedback: the backend no longer persists the resolved locale. The
     * {@code dao.update} call in {@link DefaultActorResolver#applyLocaleRefresh} is commented out
     * (persistence deferred to a follow-up capability). These tests lock in that behaviour so a
     * future accidental re-enable is caught.
     */
    @Nested
    @DisplayName("applyLocaleRefresh (persistence deferred)")
    class ApplySafeWrite {

        @Test
        void nullUpdateSkipsDaoUpdate() {
            final DAO dao = mock(DAO.class);
            final EClass actorType = mock(EClass.class);
            DefaultActorResolver.applyLocaleRefresh(dao, actorType, null);
            verify(dao, never()).update(any(), any(), any());
        }

        @Test
        void nonNullUpdateDoesNotPersist() {
            // Persistence intentionally disabled in this branch (JNG-6415 review feedback —
            // backend does not write the user's stored locale). Machinery is preserved so a
            // follow-up capability can re-enable dao.update; until then, no write occurs.
            final DAO dao = mock(DAO.class);
            final EClass actorType = mock(EClass.class);
            final Payload update = Payload.map(ID_KEY, "id-1", ATTR, "hu-HU");
            DefaultActorResolver.applyLocaleRefresh(dao, actorType, update);
            verify(dao, never()).update(any(), any(), any());
        }
    }
}
