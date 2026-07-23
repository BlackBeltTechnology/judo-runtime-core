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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Pure, side-effect-free helper that resolves the effective BCP-47 locale of an authenticated
 * principal from a fixed precedence of candidate sources.
 *
 * <p>Precedence (fixed order): <b>browser</b> (only when {@code browserLanguageCheck} is
 * {@code true}) → <b>claim</b> (OIDC {@code locale}) → <b>stored</b> (DB value) → <b>default</b>.
 * Each of the first three tiers is filtered against {@code supportedLanguages} using RFC-4647
 * <em>filtering</em> semantics (a broad range such as {@code hu} matches a specific supported tag
 * such as {@code hu-HU}); a candidate that does not match any supported tag is skipped so the next
 * tier is consulted. The {@code default} tier is terminal and is returned verbatim (never filtered).
 *
 * <p>This class has no dependency on the DAO, the ASM model, or the request context, so it is fully
 * unit-testable in isolation. See
 * {@code openspec/changes/add-principal-locale-resolution/specs/principal-locale-resolution/spec.md}.
 */
public final class PrincipalLocaleResolver {

    private static final String FALLBACK_DEFAULT = "en-US";

    private PrincipalLocaleResolver() {
    }

    /**
     * Parse a comma-separated list of BCP-47 tags into an ordered, de-duplicated set. Blank entries
     * are skipped; surrounding whitespace is trimmed; first-occurrence order is preserved.
     *
     * @param csv comma-separated supported-language list (may be {@code null} or blank)
     * @return an unmodifiable, insertion-ordered set of tags (empty if {@code csv} is null/blank)
     */
    public static Set<String> parseSupportedLanguages(final String csv) {
        if (csv == null || csv.trim().isEmpty()) {
            return Collections.emptySet();
        }
        final Set<String> result = new LinkedHashSet<>();
        for (final String token : csv.split(",")) {
            final String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return Collections.unmodifiableSet(result);
    }

    /**
     * Resolve the effective locale by walking the fixed precedence tiers.
     *
     * @param acceptLanguageHeader raw {@code Accept-Language} header value (quality-ordered ranges);
     *                             may be {@code null}. Only consulted when {@code browserLanguageCheck}
     *                             is {@code true}.
     * @param claimLocale          OIDC {@code locale} claim value; may be {@code null}
     * @param storedLocale         current DB value of the locale attribute; may be {@code null}
     * @param defaultLanguage      terminal fallback; when {@code null}/blank, {@code en-US} is used
     * @param supportedLanguages   set of offered BCP-47 tags; when {@code null}/empty only the
     *                             {@code default} tier can resolve
     * @param browserLanguageCheck when {@code true}, the browser tier is the top precedence source
     * @return the resolved BCP-47 tag (always non-null; the {@code default} tier is terminal)
     */
    public static String resolve(final String acceptLanguageHeader,
                                 final String claimLocale,
                                 final String storedLocale,
                                 final String defaultLanguage,
                                 final Set<String> supportedLanguages,
                                 final boolean browserLanguageCheck) {
        final Set<String> supported = supportedLanguages == null ? Collections.emptySet() : supportedLanguages;

        if (browserLanguageCheck) {
            final String browser = matchAgainstSupported(acceptLanguageHeader, supported);
            if (browser != null) {
                return browser;
            }
        }

        final String claim = matchAgainstSupported(claimLocale, supported);
        if (claim != null) {
            return claim;
        }

        final String stored = matchAgainstSupported(storedLocale, supported);
        if (stored != null) {
            return stored;
        }

        if (defaultLanguage != null && !defaultLanguage.trim().isEmpty()) {
            return defaultLanguage.trim();
        }
        return FALLBACK_DEFAULT;
    }

    /**
     * Filter a candidate (a single tag or a quality-ordered {@code Accept-Language} list) against the
     * supported set using RFC-4647 filtering. Returns the best-matching supported tag, or
     * {@code null} when the candidate is blank, unparseable, or matches nothing.
     */
    private static String matchAgainstSupported(final String candidate, final Set<String> supported) {
        if (candidate == null || candidate.trim().isEmpty() || supported.isEmpty()) {
            return null;
        }
        // Tolerate the Java Locale.toString() form ("hu_HU") in addition to BCP-47 ("hu-HU").
        final String normalized = candidate.trim().replace('_', '-');
        try {
            final List<Locale.LanguageRange> ranges = Locale.LanguageRange.parse(normalized);
            final List<String> matches = Locale.filterTags(ranges, new ArrayList<>(supported));
            return matches.isEmpty() ? null : matches.get(0);
        } catch (final IllegalArgumentException e) {
            // Malformed language range/tag — treat the tier as absent.
            return null;
        }
    }
}
