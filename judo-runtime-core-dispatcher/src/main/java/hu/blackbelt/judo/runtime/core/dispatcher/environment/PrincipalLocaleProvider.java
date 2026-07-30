package hu.blackbelt.judo.runtime.core.dispatcher.environment;

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
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import hu.blackbelt.judo.runtime.core.RequestLocaleHolder;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultDispatcher;
import hu.blackbelt.judo.runtime.core.security.PrincipalLocaleConfig;
import hu.blackbelt.judo.runtime.core.security.PrincipalLocaleResolver;
import hu.blackbelt.osgi.i18n.api.LocaleProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Request-scoped {@link LocaleProvider} that resolves the effective BCP-47 locale for backend
 * message i18n. The backend never mutates the user's stored preference — it only <em>reads</em>
 * the candidate inputs from the request-scoped {@link Context} and picks one per RFC-4647.
 *
 * <p>Authenticated principal in scope: walks the tier order <b>browser</b> (Accept-Language
 * captured into principal attributes by the auth interceptor, only when
 * {@code browserLanguageCheck} is on) → <b>claim</b> (OIDC {@code locale} on principal attributes)
 * → <b>stored</b> (locale attribute on the loaded actor payload) → <b>default</b>. The first three
 * tiers are filtered against {@code supportedLanguages}. Reads go directly against {@link Context}
 * (no {@code GET_PRINCIPAL} dispatch) so message-key lookup is cheap and cannot recurse during
 * error formatting.
 *
 * <p>Anonymous request: honours an application-set {@link DefaultDispatcher#LOCALE_KEY} in the
 * exchange, then the captured {@code Accept-Language} in {@link RequestLocaleHolder}, then the
 * configured default (or empty when default is blank, letting {@code I18nServiceImpl} apply its
 * own configured default).
 */
@Slf4j
public class PrincipalLocaleProvider implements LocaleProvider {

    private final Context context;

    /**
     * JNG-6415 locale configuration — grouped into a single value object by the
     * {@code consolidate-locale-config-object} change. Never null: callers pass an empty
     * {@code PrincipalLocaleConfig.builder().build()} when the feature is unconfigured (matches the
     * pre-refactor "all three params null" state). {@link PrincipalLocaleConfig#getParsedSupportedLanguages()}
     * memoizes the CSV split, so this provider (a singleton) no longer re-parses on every construction.
     */
    private final PrincipalLocaleConfig localeConfig;

    public PrincipalLocaleProvider(final Context context, final PrincipalLocaleConfig localeConfig) {
        this.context = context;
        this.localeConfig = localeConfig == null ? PrincipalLocaleConfig.builder().build() : localeConfig;
    }

    @Override
    public Optional<Locale> getLocale() {
        // 1. Authenticated principal in scope: walk the browser → claim → stored tiers purely as a
        //    READ over context. Nothing is written back. Falls through to defaultLanguage below.
        if (isPrincipalBound()) {
            final Locale resolved = resolveAuthenticatedLocale();
            if (resolved != null) {
                return Optional.of(resolved);
            }
            return Optional.ofNullable(toLocale(localeConfig.getDefaultLanguage()));
        }
        // 2. Anonymous request: prefer an application-set request Locale (survives the dispatcher's
        //    per-operation context reset via the exchange copy), then the captured Accept-Language
        //    (held in a thread-local that survives that reset), then the default.
        try {
            if (context != null) {
                final Locale appLocale = context.getAs(Locale.class, DefaultDispatcher.LOCALE_KEY);
                if (appLocale != null) {
                    return Optional.of(appLocale);
                }
            }
            final String header = RequestLocaleHolder.getAcceptLanguage();
            if (header != null) {
                final String match = PrincipalLocaleResolver.matchSupportedLanguage(
                        header, localeConfig.getParsedSupportedLanguages());
                final Locale browserLocale = toLocale(match);
                if (browserLocale != null) {
                    return Optional.of(browserLocale);
                }
            }
        } catch (final RuntimeException e) {
            log.debug("Could not read anonymous request locale: {}", e.toString());
        }
        return Optional.ofNullable(toLocale(localeConfig.getDefaultLanguage()));
    }

    /**
     * Whether a principal (authenticated user) is bound to the current request — either via the
     * loaded actor payload or the principal in context. Distinguishes the authenticated tier walk
     * from the anonymous "browser Accept-Language" path.
     */
    private boolean isPrincipalBound() {
        if (context == null) {
            return false;
        }
        try {
            return context.getAs(Payload.class, Dispatcher.ACTOR_KEY) != null
                    || context.getAs(JudoPrincipal.class, Dispatcher.PRINCIPAL_KEY) != null;
        } catch (final RuntimeException e) {
            return false;
        }
    }

    /**
     * Read-only RFC-4647 tier walk for an authenticated request: extract the three candidate inputs
     * from context (browser hint on principal attributes, OIDC claim on principal attributes,
     * stored value on the loaded actor payload) and let {@link PrincipalLocaleResolver} pick one
     * against the supported set. Returns {@code null} when the feature is off, no candidate exists,
     * or anything goes wrong — caller falls back to {@code defaultLanguage}.
     */
    private Locale resolveAuthenticatedLocale() {
        if (context == null || !localeConfig.isEnabled()) {
            return null;
        }
        final String attribute = localeConfig.getPrincipalLocaleAttribute();
        try {
            final Payload actor = context.getAs(Payload.class, Dispatcher.ACTOR_KEY);
            final JudoPrincipal principal = context.getAs(JudoPrincipal.class, Dispatcher.PRINCIPAL_KEY);
            final Map<String, Object> claims = principal != null && principal.getAttributes() != null
                    ? principal.getAttributes() : Collections.emptyMap();

            final String storedLocale = asString(actor != null ? actor.get(attribute) : null);
            final String claimLocale = asString(claims.get(attribute));
            final String browserHint = asString(claims.get(PrincipalLocaleResolver.ACCEPT_LANGUAGE_ATTRIBUTE));

            final boolean browserLanguageCheck = localeConfig.getBrowserLanguageCheck() == null
                    || localeConfig.getBrowserLanguageCheck();
            final java.util.Set<String> supported = localeConfig.getParsedSupportedLanguages();
            if (browserLanguageCheck) {
                final Locale browser = toLocale(
                        PrincipalLocaleResolver.matchSupportedLanguage(browserHint, supported));
                if (browser != null) {
                    return browser;
                }
            }
            final Locale claim = toLocale(
                    PrincipalLocaleResolver.matchSupportedLanguage(claimLocale, supported));
            if (claim != null) {
                return claim;
            }
            return toLocale(
                    PrincipalLocaleResolver.matchSupportedLanguage(storedLocale, supported));
        } catch (final RuntimeException e) {
            log.debug("Could not resolve authenticated principal locale: {}", e.toString());
            return null;
        }
    }

    private static String asString(final Object value) {
        return value == null ? null : value.toString();
    }

    /**
     * Parse a BCP-47 (or Java {@code lang_COUNTRY}) tag into a {@link Locale}, returning {@code null}
     * for null/blank/malformed input. Never throws.
     */
    private static Locale toLocale(final String tag) {
        if (tag == null || tag.trim().isEmpty()) {
            return null;
        }
        final Locale locale = Locale.forLanguageTag(tag.trim().replace('_', '-'));
        // forLanguageTag never throws; it yields the "und" (undetermined) language for garbage input.
        if (locale.getLanguage().isEmpty() || "und".equals(locale.toLanguageTag())) {
            return null;
        }
        return locale;
    }
}
