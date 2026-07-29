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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Behavioural spec for {@link PrincipalLocaleProvider}: it surfaces the principal's resolved locale
 * (from the request-scoped {@link Context}) to backend i18n via the {@code LocaleProvider} SPI, with
 * a cheap O(1) read and a safe fallback to the configured default language.
 * See {@code openspec/changes/add-principal-locale-resolution/specs/principal-locale-resolution/spec.md}.
 */
class PrincipalLocaleProviderTest {

    private static final String ATTR = "locale";
    private static final String DEFAULT = "en-US";
    private static final String SUPPORTED = "en-US,hu-HU";

    private static PrincipalLocaleProvider provider(final Context context, final String attr, final String defaultLanguage) {
        return new PrincipalLocaleProvider(context, PrincipalLocaleConfig.builder()
                .principalLocaleAttribute(attr)
                .supportedLanguages(SUPPORTED)
                .defaultLanguage(defaultLanguage)
                .build());
    }

    private static Context contextWithActor(final String localeValue) {
        final Context context = mock(Context.class);
        when(context.getAs(Payload.class, Dispatcher.ACTOR_KEY))
                .thenReturn(Payload.map(ATTR, localeValue));
        return context;
    }

    private static Context contextWithPrincipal(final Map<String, Object> attributes) {
        final Context context = mock(Context.class);
        when(context.getAs(Payload.class, Dispatcher.ACTOR_KEY)).thenReturn(null);
        when(context.getAs(JudoPrincipal.class, Dispatcher.PRINCIPAL_KEY)).thenReturn(
                JudoPrincipal.builder().name("u").realm("r").client("c").attributes(attributes).build());
        return context;
    }

    private static Context emptyContext() {
        final Context context = mock(Context.class);
        when(context.getAs(Payload.class, Dispatcher.ACTOR_KEY)).thenReturn(null);
        when(context.getAs(JudoPrincipal.class, Dispatcher.PRINCIPAL_KEY)).thenReturn(null);
        return context;
    }

    private static Context anonymousContextWithAcceptLanguage(final String header) {
        final Context context = mock(Context.class);
        when(context.getAs(Payload.class, Dispatcher.ACTOR_KEY)).thenReturn(null);
        when(context.getAs(JudoPrincipal.class, Dispatcher.PRINCIPAL_KEY)).thenReturn(null);
        when(context.getAs(Locale.class, DefaultDispatcher.LOCALE_KEY)).thenReturn(null);
        RequestLocaleHolder.set(header);
        return context;
    }

    @AfterEach
    void clearHolder() {
        RequestLocaleHolder.clear();
    }

    @Test
    void actorLocaleWins() {
        assertThat(provider(contextWithActor("hu-HU"), ATTR, DEFAULT).getLocale(),
                is(equalTo(Optional.of(Locale.forLanguageTag("hu-HU")))));
    }

    @Test
    void principalAttributeUsedWhenNoActor() {
        assertThat(provider(contextWithPrincipal(Map.of(ATTR, "de-DE")), ATTR, DEFAULT).getLocale(),
                is(equalTo(Optional.of(Locale.forLanguageTag("de-DE")))));
    }

    @Test
    void noPrincipalFallsBackToDefault() {
        assertThat(provider(emptyContext(), ATTR, DEFAULT).getLocale(),
                is(equalTo(Optional.of(Locale.forLanguageTag(DEFAULT)))));
    }

    @Test
    void blankAttributeFallsBackToDefault() {
        assertThat(provider(contextWithActor("hu-HU"), "  ", DEFAULT).getLocale(),
                is(equalTo(Optional.of(Locale.forLanguageTag(DEFAULT)))));
    }

    @Test
    void malformedStoredValueFallsBackToDefaultWithoutThrowing() {
        assertThat(provider(contextWithActor("!!! not a tag !!!"), ATTR, DEFAULT).getLocale(),
                is(equalTo(Optional.of(Locale.forLanguageTag(DEFAULT)))));
    }

    @Test
    void underscoreLocaleFormTolerated() {
        assertThat(provider(contextWithActor("hu_HU"), ATTR, DEFAULT).getLocale(),
                is(equalTo(Optional.of(Locale.forLanguageTag("hu-HU")))));
    }

    @Test
    void emptyWhenNoPrincipalAndNoDefault() {
        assertThat(provider(emptyContext(), ATTR, "  ").getLocale(), is(equalTo(Optional.empty())));
    }

    @Test
    void nullContextIsSafe() {
        assertThat(provider(null, ATTR, DEFAULT).getLocale(),
                is(equalTo(Optional.of(Locale.forLanguageTag(DEFAULT)))));
    }

    // -------- anonymous (no principal) request-locale branch --------

    @Test
    void anonymousSupportedAcceptLanguageWins() {
        assertThat(provider(anonymousContextWithAcceptLanguage("hu-HU,en-US;q=0.7"), ATTR, DEFAULT).getLocale(),
                is(equalTo(Optional.of(Locale.forLanguageTag("hu-HU")))));
    }

    @Test
    void anonymousLanguageOnlyRangeMatchesSupported() {
        assertThat(provider(anonymousContextWithAcceptLanguage("hu"), ATTR, DEFAULT).getLocale(),
                is(equalTo(Optional.of(Locale.forLanguageTag("hu-HU")))));
    }

    @Test
    void anonymousUnsupportedAcceptLanguageFallsBackToDefault() {
        assertThat(provider(anonymousContextWithAcceptLanguage("de-DE"), ATTR, DEFAULT).getLocale(),
                is(equalTo(Optional.of(Locale.forLanguageTag(DEFAULT)))));
    }

    @Test
    void anonymousNoAcceptLanguageFallsBackToDefault() {
        assertThat(provider(anonymousContextWithAcceptLanguage(null), ATTR, DEFAULT).getLocale(),
                is(equalTo(Optional.of(Locale.forLanguageTag(DEFAULT)))));
    }

    @Test
    void anonymousUnsupportedAndBlankDefaultIsEmpty() {
        assertThat(provider(anonymousContextWithAcceptLanguage("de-DE"), ATTR, "  ").getLocale(),
                is(equalTo(Optional.empty())));
    }

    @Test
    void anonymousApplicationSetLocaleKeyWins() {
        final Context context = mock(Context.class);
        when(context.getAs(Payload.class, Dispatcher.ACTOR_KEY)).thenReturn(null);
        when(context.getAs(JudoPrincipal.class, Dispatcher.PRINCIPAL_KEY)).thenReturn(null);
        when(context.getAs(Locale.class, DefaultDispatcher.LOCALE_KEY)).thenReturn(Locale.forLanguageTag("hu-HU"));
        RequestLocaleHolder.set("en-US");
        assertThat(provider(context, ATTR, DEFAULT).getLocale(),
                is(equalTo(Optional.of(Locale.forLanguageTag("hu-HU")))));
    }

    @Test
    void authenticatedPrincipalWinsOverBrowser() {
        // principal bound (actor payload) + an Accept-Language present -> principal wins
        final Context context = mock(Context.class);
        when(context.getAs(Payload.class, Dispatcher.ACTOR_KEY)).thenReturn(Payload.map(ATTR, "hu-HU"));
        RequestLocaleHolder.set("en-US");
        assertThat(provider(context, ATTR, DEFAULT).getLocale(),
                is(equalTo(Optional.of(Locale.forLanguageTag("hu-HU")))));
    }
}
