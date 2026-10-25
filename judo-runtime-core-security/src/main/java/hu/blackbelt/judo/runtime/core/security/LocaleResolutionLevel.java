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

import java.util.Locale;

/**
 * Precedence <em>ceiling</em> that names the highest-priority tier the runtime is allowed to
 * consult when resolving a principal's effective locale. Lower tiers are always active; the enum
 * is a ceiling, not a bitmask. Constants are declared in ascending precedence order so
 * {@link #ordinal()} matches "how much the runtime interferes":
 *
 * <ul>
 *   <li>{@link #PRINCIPAL} — only the stored value on the actor payload is consulted.</li>
 *   <li>{@link #IDENTITY_PROVIDER} — the OIDC {@code locale} claim is also consulted.</li>
 *   <li>{@link #BROWSER} — the request {@code Accept-Language} header is also consulted (the
 *       default; {@link #DEFAULT} points here).</li>
 * </ul>
 *
 * <p>Replaces the boolean {@code browserLanguageCheck} introduced by
 * {@code add-principal-locale-resolution}: {@code true → BROWSER}, {@code false → IDENTITY_PROVIDER}.
 * See
 * {@code openspec/changes/replace-browser-check-with-resolution-level/specs/principal-locale-resolution/spec.md}
 * (ADDED requirement "`LocaleResolutionLevel` enum shape").
 */
public enum LocaleResolutionLevel {

    /** Ceiling floor — only the stored value on the loaded actor payload is consulted. */
    PRINCIPAL,

    /** Adds the OIDC {@code locale} claim (from principal attributes) above the stored tier. */
    IDENTITY_PROVIDER,

    /** Adds the request {@code Accept-Language} header above the claim tier. */
    BROWSER;

    /**
     * Deployment-wide default when the platform parameter is unset, blank, or malformed. Points at
     * {@link #BROWSER} so out-of-the-box deployments honour the user's browser hint (the
     * behaviour of the pre-refactor {@code browserLanguageCheck=true} default).
     */
    public static final LocaleResolutionLevel DEFAULT = BROWSER;

    /**
     * {@code true} iff this level's ceiling reaches (i.e., is at or above) the given tier. Equal
     * to {@code this.ordinal() >= tier.ordinal()}. Reads naturally: {@code BROWSER.includes(IDENTITY_PROVIDER)}
     * → {@code true}; {@code PRINCIPAL.includes(BROWSER)} → {@code false}.
     *
     * @param tier the tier being tested (must not be {@code null} — this is an internal API
     *             invoked with enum-constant arguments only, so {@code null} is a bug the caller
     *             should learn about immediately)
     * @throws NullPointerException if {@code tier} is {@code null}
     */
    public boolean includes(final LocaleResolutionLevel tier) {
        return ordinal() >= tier.ordinal();
    }

    /**
     * Lenient parse from a platform parameter value (env var or Spring property). Trims
     * surrounding whitespace and compares upper-cased against the enum's constant names.
     *
     * <p>{@code null}, blank, or unrecognised inputs SHALL return {@link #DEFAULT} rather than
     * throwing — a typo in an env var name reduces to the safe default instead of breaking
     * authentication. Matches the leniency of sibling parsers in this module
     * ({@code AcceptableClientsParser.parseAcceptableClients},
     * {@link PrincipalLocaleResolver#parseSupportedLanguages}).
     *
     * @param value raw parameter value (may be {@code null})
     * @return the parsed level, or {@link #DEFAULT} on any non-match
     */
    public static LocaleResolutionLevel parse(final String value) {
        if (value == null) {
            return DEFAULT;
        }
        final String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return DEFAULT;
        }
        try {
            return LocaleResolutionLevel.valueOf(trimmed.toUpperCase(Locale.ROOT));
        } catch (final IllegalArgumentException e) {
            return DEFAULT;
        }
    }
}
