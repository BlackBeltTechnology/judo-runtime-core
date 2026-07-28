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
import hu.blackbelt.judo.runtime.core.security.PrincipalLocaleResolver;
import hu.blackbelt.osgi.i18n.api.LocaleProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Request-scoped {@link LocaleProvider} that surfaces the authenticated principal's effective locale
 * (resolved and persisted at login by {@code DefaultActorResolver}) to backend message i18n.
 *
 * <p>Reads are O(1): the resolved locale is taken from the request-scoped {@link Context} — first
 * from the loaded actor payload ({@link Dispatcher#ACTOR_KEY}), then from the principal's token
 * attributes ({@link Dispatcher#PRINCIPAL_KEY}). This deliberately avoids invoking the {@code
 * GET_PRINCIPAL} operation (as {@code PrincipalVariableProvider} does), which would trigger a full
 * dispatch on every message-key lookup and risk recursion during error formatting.
 *
 * <p>Binding this as the {@code LocaleProvider} closes the long-standing gap where {@code
 * I18nServiceImpl}'s optional reference was unbound, so backend messages fall back to the configured
 * default / JVM locale regardless of the user. When no principal is in scope, or the attribute is
 * unset/blank/malformed, it falls back to {@code defaultLanguage} (or empty when that is also blank,
 * letting {@code I18nServiceImpl} apply its own configured default).
 */
@Slf4j
public class PrincipalLocaleProvider implements LocaleProvider {

    private final Context context;

    private final String principalLocaleAttribute;

    private final Set<String> supportedLanguages;

    private final String defaultLanguage;

    public PrincipalLocaleProvider(final Context context,
                                   final String principalLocaleAttribute,
                                   final String supportedLanguages,
                                   final String defaultLanguage) {
        this.context = context;
        this.principalLocaleAttribute = principalLocaleAttribute;
        this.supportedLanguages = PrincipalLocaleResolver.parseSupportedLanguages(supportedLanguages);
        this.defaultLanguage = defaultLanguage;
    }

    @Override
    public Optional<Locale> getLocale() {
        // 1. Authenticated: the principal's resolved locale (actor payload, then token attributes).
        final Locale principalLocale = toLocale(resolvePrincipalLocaleTag());
        if (principalLocale != null) {
            return Optional.of(principalLocale);
        }
        // 2. A principal is bound but carries no usable locale -> default (unchanged JNG-6415 path).
        if (isPrincipalBound()) {
            return Optional.ofNullable(toLocale(defaultLanguage));
        }
        // 3. Anonymous request: prefer an application-set request Locale (survives the dispatcher's
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
                final String match = PrincipalLocaleResolver.matchSupportedLanguage(header, supportedLanguages);
                final Locale browserLocale = toLocale(match);
                if (browserLocale != null) {
                    return Optional.of(browserLocale);
                }
            }
        } catch (final RuntimeException e) {
            log.debug("Could not read anonymous request locale: {}", e.toString());
        }
        return Optional.ofNullable(toLocale(defaultLanguage));
    }

    /**
     * Whether a principal (authenticated user) is bound to the current request — either via the
     * loaded actor payload or the principal in context. Distinguishes the authenticated "no usable
     * locale -> default" path from the anonymous "browser Accept-Language" path.
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
     * Cheaply read the principal's locale tag from the request-scoped context: prefer the loaded
     * actor payload, then the principal's token attributes. Returns {@code null} when the feature is
     * off (blank attribute), no principal is in scope, or anything goes wrong.
     */
    private String resolvePrincipalLocaleTag() {
        if (context == null || principalLocaleAttribute == null || principalLocaleAttribute.trim().isEmpty()) {
            return null;
        }
        try {
            final Payload actor = context.getAs(Payload.class, Dispatcher.ACTOR_KEY);
            if (actor != null) {
                final Object value = actor.get(principalLocaleAttribute);
                if (value != null) {
                    return value.toString();
                }
            }
            final JudoPrincipal principal = context.getAs(JudoPrincipal.class, Dispatcher.PRINCIPAL_KEY);
            if (principal != null && principal.getAttributes() != null) {
                final Object value = principal.getAttributes().get(principalLocaleAttribute);
                if (value != null) {
                    return value.toString();
                }
            }
        } catch (final RuntimeException e) {
            log.debug("Could not read principal locale from context: {}", e.toString());
        }
        return null;
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
