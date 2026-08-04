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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static hu.blackbelt.judo.runtime.core.security.LocaleResolutionLevel.BROWSER;
import static hu.blackbelt.judo.runtime.core.security.LocaleResolutionLevel.DEFAULT;
import static hu.blackbelt.judo.runtime.core.security.LocaleResolutionLevel.IDENTITY_PROVIDER;
import static hu.blackbelt.judo.runtime.core.security.LocaleResolutionLevel.PRINCIPAL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Behavioural spec for {@link LocaleResolutionLevel}: a ceiling-style enum ordering the three
 * locale-resolution tiers (`PRINCIPAL &lt; IDENTITY_PROVIDER &lt; BROWSER`), with a lenient
 * {@link LocaleResolutionLevel#parse(String) parse} that never throws and a
 * {@link LocaleResolutionLevel#DEFAULT} pointing at {@code BROWSER}.
 *
 * <p>See the change spec
 * {@code openspec/changes/replace-browser-check-with-resolution-level/specs/principal-locale-resolution/spec.md}
 * (ADDED requirement "`LocaleResolutionLevel` enum shape").
 */
class LocaleResolutionLevelTest {

    @Nested
    @DisplayName("includes(tier) — ceiling semantics")
    class Includes {

        @Test
        void browserIncludesEveryTier() {
            assertThat(BROWSER.includes(BROWSER), is(true));
            assertThat(BROWSER.includes(IDENTITY_PROVIDER), is(true));
            assertThat(BROWSER.includes(PRINCIPAL), is(true));
        }

        @Test
        void identityProviderIncludesClaimAndStoredButNotBrowser() {
            assertThat(IDENTITY_PROVIDER.includes(BROWSER), is(false));
            assertThat(IDENTITY_PROVIDER.includes(IDENTITY_PROVIDER), is(true));
            assertThat(IDENTITY_PROVIDER.includes(PRINCIPAL), is(true));
        }

        @Test
        void principalIncludesOnlyItself() {
            assertThat(PRINCIPAL.includes(BROWSER), is(false));
            assertThat(PRINCIPAL.includes(IDENTITY_PROVIDER), is(false));
            assertThat(PRINCIPAL.includes(PRINCIPAL), is(true));
        }
    }

    @Nested
    @DisplayName("parse(value) — lenient, never throws")
    class Parse {

        @Test
        void exactUppercase() {
            assertThat(LocaleResolutionLevel.parse("BROWSER"), is(BROWSER));
            assertThat(LocaleResolutionLevel.parse("IDENTITY_PROVIDER"), is(IDENTITY_PROVIDER));
            assertThat(LocaleResolutionLevel.parse("PRINCIPAL"), is(PRINCIPAL));
        }

        @Test
        void lowerAndMixedCase() {
            assertThat(LocaleResolutionLevel.parse("browser"), is(BROWSER));
            assertThat(LocaleResolutionLevel.parse("Browser"), is(BROWSER));
            assertThat(LocaleResolutionLevel.parse("identity_provider"), is(IDENTITY_PROVIDER));
            assertThat(LocaleResolutionLevel.parse("Identity_Provider"), is(IDENTITY_PROVIDER));
            assertThat(LocaleResolutionLevel.parse("principal"), is(PRINCIPAL));
        }

        @Test
        void surroundingWhitespaceTrimmed() {
            assertThat(LocaleResolutionLevel.parse("  BROWSER  "), is(BROWSER));
            assertThat(LocaleResolutionLevel.parse("\tprincipal\n"), is(PRINCIPAL));
        }

        @Test
        void nullFallsBackToDefault() {
            assertThat(LocaleResolutionLevel.parse(null), is(DEFAULT));
        }

        @Test
        void blankFallsBackToDefault() {
            assertThat(LocaleResolutionLevel.parse(""), is(DEFAULT));
            assertThat(LocaleResolutionLevel.parse("   "), is(DEFAULT));
        }

        @Test
        void garbageFallsBackToDefault() {
            assertThat(LocaleResolutionLevel.parse("foobar"), is(DEFAULT));
            assertThat(LocaleResolutionLevel.parse("!"), is(DEFAULT));
        }

        @Test
        void defaultIsBrowser() {
            assertThat(DEFAULT, is(sameInstance(BROWSER)));
        }
    }
}
