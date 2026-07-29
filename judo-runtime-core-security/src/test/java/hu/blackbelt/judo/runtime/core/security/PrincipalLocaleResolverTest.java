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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Behavioural spec for {@link PrincipalLocaleResolver}, covering:
 * <ul>
 *     <li>Supported-language CSV parsing (trimming, empty, duplicates).</li>
 *     <li>Fixed precedence walk: browser → claim → stored → default.</li>
 *     <li>Browser tier gate ({@code browserLanguageCheck}).</li>
 *     <li>RFC-4647 lookup (language-range → language-tag).</li>
 *     <li>Malformed tag skip, blank/null inputs, fall-through to next tier.</li>
 *     <li>Terminal {@code default} tier always resolves.</li>
 *     <li>Referential transparency.</li>
 * </ul>
 * See {@code openspec/changes/add-principal-locale-resolution/specs/principal-locale-resolution/spec.md}.
 */
class PrincipalLocaleResolverTest {

    private static final Set<String> EN_HU = supported("en-US", "hu-HU");
    private static final Set<String> EN_ONLY = supported("en-US");

    private static Set<String> supported(final String... tags) {
        return new LinkedHashSet<>(java.util.Arrays.asList(tags));
    }

    // -------- parseSupportedLanguages --------

    @Nested
    @DisplayName("parseSupportedLanguages")
    class ParseSupportedLanguages {

        @Test
        void emptyOrBlankReturnsEmpty() {
            assertThat(PrincipalLocaleResolver.parseSupportedLanguages(null), is(equalTo(Collections.emptySet())));
            assertThat(PrincipalLocaleResolver.parseSupportedLanguages(""), is(equalTo(Collections.emptySet())));
            assertThat(PrincipalLocaleResolver.parseSupportedLanguages("   "), is(equalTo(Collections.emptySet())));
        }

        @Test
        void commaSeparatedTagsAreTrimmedAndOrderPreserved() {
            final Set<String> parsed = PrincipalLocaleResolver.parseSupportedLanguages(" en-US , hu-HU , de-DE ");
            assertThat(parsed, contains("en-US", "hu-HU", "de-DE"));
        }

        @Test
        void emptyEntriesAreSkipped() {
            final Set<String> parsed = PrincipalLocaleResolver.parseSupportedLanguages("en-US,,hu-HU,");
            assertThat(parsed, contains("en-US", "hu-HU"));
        }

        @Test
        void duplicatesAreDeduplicatedPreservingFirstOccurrence() {
            final Set<String> parsed = PrincipalLocaleResolver.parseSupportedLanguages("en-US,hu-HU,en-US");
            assertThat(parsed, contains("en-US", "hu-HU"));
        }
    }

    // -------- browser tier --------

    @Nested
    @DisplayName("browser tier")
    class BrowserTier {

        @Test
        void browserWinsWhenGateOnAndSupported() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    "hu-HU,en-US;q=0.7", "en-US", "en-US", "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("hu-HU")));
        }

        @Test
        void gateOffSkipsBrowserTierEvenWithHeader() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    "hu-HU", "en-US", "en-US", "en-US", EN_HU, false);
            assertThat(resolved, is(equalTo("en-US")));
        }

        @Test
        void languageOnlyRangeMatchesRegionTaggedSupported() {
            // "hu" -> "hu-HU" via RFC-4647 lookup
            final String resolved = PrincipalLocaleResolver.resolve(
                    "hu", null, null, "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("hu-HU")));
        }

        @Test
        void unsupportedBrowserTagFallsThroughToClaim() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    "de-DE", "hu-HU", null, "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("hu-HU")));
        }

        @Test
        void regionMismatchFallsThrough() {
            // en-GB does not match en-US (lookup narrows, never widens)
            final String resolved = PrincipalLocaleResolver.resolve(
                    "en-GB", null, null, "en-US", EN_ONLY, true);
            assertThat(resolved, is(equalTo("en-US"))); // default terminal
        }

        @Test
        void qualityOrderedBrowserRangesPickHighestSupported() {
            // de-DE unsupported (q=1.0), hu-HU supported (q=0.7) — hu-HU wins
            final String resolved = PrincipalLocaleResolver.resolve(
                    "de-DE,hu-HU;q=0.7", null, null, "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("hu-HU")));
        }

        @Test
        void malformedBrowserHeaderIsSkipped() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    "!!! not a tag !!!", "hu-HU", null, "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("hu-HU")));
        }

        @Test
        void nullBrowserHeaderIsSkipped() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    null, "hu-HU", null, "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("hu-HU")));
        }

        @Test
        void blankBrowserHeaderIsSkipped() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    "   ", "hu-HU", null, "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("hu-HU")));
        }
    }

    // -------- claim tier --------

    @Nested
    @DisplayName("claim tier")
    class ClaimTier {

        @Test
        void claimWinsWhenBrowserEmpty() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    null, "hu-HU", "en-US", "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("hu-HU")));
        }

        @Test
        void claimWinsWhenBrowserGateOff() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    "en-US", "hu-HU", "en-US", "en-US", EN_HU, false);
            assertThat(resolved, is(equalTo("hu-HU")));
        }

        @Test
        void claimLanguageOnlyMatchesRegionTagged() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    null, "hu", null, "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("hu-HU")));
        }

        @Test
        void unsupportedClaimFallsThroughToStored() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    null, "de-DE", "hu-HU", "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("hu-HU")));
        }

        @Test
        void nullClaimFallsThroughToStored() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    null, null, "hu-HU", "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("hu-HU")));
        }

        @Test
        void malformedClaimIsSkipped() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    null, "!!! bad !!!", "hu-HU", "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("hu-HU")));
        }
    }

    // -------- stored tier --------

    @Nested
    @DisplayName("stored tier")
    class StoredTier {

        @Test
        void storedWinsWhenBrowserAndClaimEmpty() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    null, null, "hu-HU", "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("hu-HU")));
        }

        @Test
        void unsupportedStoredFallsThroughToDefault() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    null, null, "de-DE", "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("en-US")));
        }

        @Test
        void nullStoredFallsThroughToDefault() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    null, null, null, "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("en-US")));
        }

        @Test
        void blankStoredFallsThroughToDefault() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    null, null, "   ", "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("en-US")));
        }
    }

    // -------- default tier (terminal) --------

    @Nested
    @DisplayName("default tier (terminal)")
    class DefaultTier {

        @Test
        void defaultAlwaysResolvesWhenAllHigherTiersEmpty() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    null, null, null, "en-US", EN_HU, true);
            assertThat(resolved, is(equalTo("en-US")));
        }

        @Test
        void defaultReturnedEvenIfNotInSupportedSet() {
            // default is terminal — not filtered against supportedLanguages
            final String resolved = PrincipalLocaleResolver.resolve(
                    null, null, null, "fr-FR", EN_HU, true);
            assertThat(resolved, is(equalTo("fr-FR")));
        }

        @Test
        void nullDefaultFallsBackToEnUs() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    null, null, null, null, EN_HU, true);
            assertThat(resolved, is(equalTo("en-US")));
        }

        @Test
        void blankDefaultFallsBackToEnUs() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    null, null, null, "  ", EN_HU, true);
            assertThat(resolved, is(equalTo("en-US")));
        }
    }

    // -------- supported set edge cases --------

    @Nested
    @DisplayName("supportedLanguages")
    class SupportedLanguages {

        @Test
        void emptySupportedSetOnlyAllowsDefault() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    "hu-HU", "hu-HU", "hu-HU", "en-US", Collections.emptySet(), true);
            assertThat(resolved, is(equalTo("en-US")));
        }

        @Test
        void nullSupportedSetTreatedAsEmpty() {
            final String resolved = PrincipalLocaleResolver.resolve(
                    "hu-HU", null, null, "en-US", null, true);
            assertThat(resolved, is(equalTo("en-US")));
        }
    }

    // -------- referential transparency --------

    @Nested
    @DisplayName("referential transparency")
    class Purity {

        @Test
        void sameInputsProduceSameOutput() {
            final String first = PrincipalLocaleResolver.resolve(
                    "hu", "en-US", "hu-HU", "en-US", EN_HU, true);
            final String second = PrincipalLocaleResolver.resolve(
                    "hu", "en-US", "hu-HU", "en-US", EN_HU, true);
            assertThat(first, is(equalTo(second)));
        }
    }
}
