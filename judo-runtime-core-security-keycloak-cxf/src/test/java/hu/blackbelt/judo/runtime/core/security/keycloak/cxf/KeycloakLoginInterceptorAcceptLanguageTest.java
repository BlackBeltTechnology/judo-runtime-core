package hu.blackbelt.judo.runtime.core.security.keycloak.cxf;

/*-
 * #%L
 * JUDO Services Keycloak Security for CXF
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

import hu.blackbelt.judo.runtime.core.security.PrincipalLocaleResolver;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies {@code Accept-Language} capture in {@link KeycloakLoginInterceptor}: the header is only
 * read and stashed under {@link PrincipalLocaleResolver#ACCEPT_LANGUAGE_ATTRIBUTE} when the
 * {@code browserLanguageCheck} gate is on and a non-blank header is present.
 * See {@code openspec/changes/add-principal-locale-resolution/specs/principal-locale-resolution/spec.md}.
 */
class KeycloakLoginInterceptorAcceptLanguageTest {

    private static final String KEY = PrincipalLocaleResolver.ACCEPT_LANGUAGE_ATTRIBUTE;

    private static HttpServletRequest requestWithAcceptLanguage(final String value) {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getHeader("Accept-Language")).thenReturn(value);
        return request;
    }

    @Test
    void gateOffHeaderNotCaptured() {
        final Map<String, Object> attributes = new HashMap<>();
        KeycloakLoginInterceptor.captureAcceptLanguage(
                attributes, requestWithAcceptLanguage("hu-HU"), false);
        assertThat(attributes.containsKey(KEY), is(false));
    }

    @Test
    void gateOnHeaderPresentCaptured() {
        final Map<String, Object> attributes = new HashMap<>();
        KeycloakLoginInterceptor.captureAcceptLanguage(
                attributes, requestWithAcceptLanguage("hu-HU,en-US;q=0.7"), true);
        assertThat((String) attributes.get(KEY), is(equalTo("hu-HU,en-US;q=0.7")));
    }

    @Test
    void gateOnHeaderAbsentKeyAbsent() {
        final Map<String, Object> attributes = new HashMap<>();
        KeycloakLoginInterceptor.captureAcceptLanguage(
                attributes, requestWithAcceptLanguage(null), true);
        assertThat(attributes.containsKey(KEY), is(false));
    }

    @Test
    void gateOnHeaderBlankKeyAbsent() {
        final Map<String, Object> attributes = new HashMap<>();
        KeycloakLoginInterceptor.captureAcceptLanguage(
                attributes, requestWithAcceptLanguage("   "), true);
        assertThat(attributes.containsKey(KEY), is(false));
    }

    @Test
    void gateOnNullRequestNoException() {
        final Map<String, Object> attributes = new HashMap<>();
        KeycloakLoginInterceptor.captureAcceptLanguage(attributes, null, true);
        assertThat(attributes.containsKey(KEY), is(false));
    }

    @Test
    void capturedValueIsRawHeaderNotParsed() {
        final Map<String, Object> attributes = new HashMap<>();
        final String raw = "de-DE, hu-HU;q=0.9, en;q=0.5";
        KeycloakLoginInterceptor.captureAcceptLanguage(
                attributes, requestWithAcceptLanguage(raw), true);
        assertThat((String) attributes.get(KEY), is(equalTo(raw)));
        assertThat(attributes.get(KEY), is(not(equalTo("de-DE"))));
    }
}
