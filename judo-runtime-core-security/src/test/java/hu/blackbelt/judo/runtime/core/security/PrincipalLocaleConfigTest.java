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
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for {@link PrincipalLocaleConfig} — the immutable value object that groups the four
 * JNG-6415 locale settings ({@code principalLocaleAttribute}, {@code supportedLanguages},
 * {@code defaultLanguage}, {@code localeResolutionLevel}) into a single bundle carried through
 * the runtime's Guice/Spring wiring and its two consumers ({@code DefaultActorResolver},
 * {@code PrincipalLocaleProvider}).
 *
 * <p>These tests pin the contract this refactor promises: identical parsing semantics to the
 * pre-refactor per-scalar wiring (delegating to {@link PrincipalLocaleResolver#parseSupportedLanguages(String)}),
 * a null-tolerant surface, the "blank attribute ⇒ feature off" gate expressed via
 * {@link PrincipalLocaleConfig#isEnabled()}, the {@code localeResolutionLevel = BROWSER} default via
 * {@code @Builder.Default}, and the parse-once memoization of the supported-set (so
 * {@code PrincipalLocaleProvider} — a singleton — no longer re-parses on every construction).
 */
class PrincipalLocaleConfigTest {

    @Test
    @DisplayName("builder() with no fields set: all nulls except localeResolutionLevel=BROWSER, feature disabled")
    void defaultBuilder_disabledFeature_defaultsBrowserCeilingOn() {
        final PrincipalLocaleConfig cfg = PrincipalLocaleConfig.builder().build();

        assertThat(cfg.getPrincipalLocaleAttribute(), is(nullValue()));
        assertThat(cfg.getSupportedLanguages(), is(nullValue()));
        assertThat(cfg.getDefaultLanguage(), is(nullValue()));
        assertThat(cfg.getLocaleResolutionLevel(), is(LocaleResolutionLevel.BROWSER));
        assertThat("blank attribute ⇒ feature off", cfg.isEnabled(), is(false));
        assertThat("null CSV parses to empty set", cfg.getParsedSupportedLanguages(),
                is(equalTo(Set.of())));
    }

    @Test
    @DisplayName("isEnabled() is false for null, empty, and whitespace-only principalLocaleAttribute")
    void isEnabled_falseForBlankAttribute() {
        assertThat(PrincipalLocaleConfig.builder().principalLocaleAttribute(null).build().isEnabled(),
                is(false));
        assertThat(PrincipalLocaleConfig.builder().principalLocaleAttribute("").build().isEnabled(),
                is(false));
        assertThat(PrincipalLocaleConfig.builder().principalLocaleAttribute("   ").build().isEnabled(),
                is(false));
    }

    @Test
    @DisplayName("isEnabled() is true for a non-blank principalLocaleAttribute (trimming irrelevant)")
    void isEnabled_trueForNonBlankAttribute() {
        assertThat(PrincipalLocaleConfig.builder().principalLocaleAttribute("locale").build().isEnabled(),
                is(true));
        assertThat(PrincipalLocaleConfig.builder().principalLocaleAttribute("  locale  ").build().isEnabled(),
                is(true));
    }

    @Test
    @DisplayName("getParsedSupportedLanguages() matches PrincipalLocaleResolver.parseSupportedLanguages semantics")
    void parsedSupportedLanguages_matchesResolverParser() {
        final String csv = "en-US, hu-HU ,, en-US ,de-DE";
        final PrincipalLocaleConfig cfg = PrincipalLocaleConfig.builder()
                .supportedLanguages(csv)
                .build();

        // Parity contract: exact same set (order + de-dup + trim) as the pure helper.
        assertThat(cfg.getParsedSupportedLanguages(),
                is(equalTo(PrincipalLocaleResolver.parseSupportedLanguages(csv))));
        // Sanity: order preserved, duplicates removed, blanks skipped.
        assertThat(cfg.getParsedSupportedLanguages(), contains("en-US", "hu-HU", "de-DE"));
    }

    @Test
    @DisplayName("getParsedSupportedLanguages() memoizes: same Set reference on repeat calls")
    void parsedSupportedLanguages_memoized() {
        final PrincipalLocaleConfig cfg = PrincipalLocaleConfig.builder()
                .supportedLanguages("en-US,hu-HU")
                .build();

        final Set<String> first = cfg.getParsedSupportedLanguages();
        final Set<String> second = cfg.getParsedSupportedLanguages();
        assertThat("parse-once contract", second, is(sameInstance(first)));
    }

    @Test
    @DisplayName("localeResolutionLevel explicit non-default is preserved (not overridden by @Builder.Default)")
    void localeResolutionLevel_explicitPrincipalPreserved() {
        final PrincipalLocaleConfig cfg = PrincipalLocaleConfig.builder()
                .localeResolutionLevel(LocaleResolutionLevel.PRINCIPAL)
                .build();
        assertThat(cfg.getLocaleResolutionLevel(), is(LocaleResolutionLevel.PRINCIPAL));
    }

    @Test
    @DisplayName("localeResolutionLevel explicit IDENTITY_PROVIDER (formerly browserLanguageCheck=false) preserved")
    void localeResolutionLevel_explicitIdentityProviderPreserved() {
        // Historical mapping: browserLanguageCheck=false → IDENTITY_PROVIDER (browser off, claim on).
        final PrincipalLocaleConfig cfg = PrincipalLocaleConfig.builder()
                .localeResolutionLevel(LocaleResolutionLevel.IDENTITY_PROVIDER)
                .build();
        assertThat(cfg.getLocaleResolutionLevel(), is(LocaleResolutionLevel.IDENTITY_PROVIDER));
    }

    @Test
    @DisplayName("full builder wires every field through unmodified")
    void fullBuilder_fieldsRoundTrip() {
        final PrincipalLocaleConfig cfg = PrincipalLocaleConfig.builder()
                .principalLocaleAttribute("locale")
                .supportedLanguages("en-US,hu-HU")
                .defaultLanguage("en-US")
                .localeResolutionLevel(LocaleResolutionLevel.IDENTITY_PROVIDER)
                .build();

        assertThat(cfg.getPrincipalLocaleAttribute(), is(equalTo("locale")));
        assertThat(cfg.getSupportedLanguages(), is(equalTo("en-US,hu-HU")));
        assertThat(cfg.getDefaultLanguage(), is(equalTo("en-US")));
        assertThat(cfg.getLocaleResolutionLevel(), is(LocaleResolutionLevel.IDENTITY_PROVIDER));
        assertThat(cfg.isEnabled(), is(true));
        assertThat(cfg.getParsedSupportedLanguages(), contains("en-US", "hu-HU"));
    }

    @Test
    @DisplayName("value object identity: equal contents ⇒ equal & same hashCode")
    void valueSemantics_equalsAndHashCode() {
        final PrincipalLocaleConfig a = PrincipalLocaleConfig.builder()
                .principalLocaleAttribute("locale").supportedLanguages("en-US")
                .defaultLanguage("en-US").localeResolutionLevel(LocaleResolutionLevel.BROWSER).build();
        final PrincipalLocaleConfig b = PrincipalLocaleConfig.builder()
                .principalLocaleAttribute("locale").supportedLanguages("en-US")
                .defaultLanguage("en-US").localeResolutionLevel(LocaleResolutionLevel.BROWSER).build();

        assertThat(a, is(equalTo(b)));
        assertThat(a.hashCode(), is(equalTo(b.hashCode())));
        // toString exists (Lombok @Value) — not asserting content, just non-null.
        assertThat(a.toString(), is(notNullValue()));
    }
}
