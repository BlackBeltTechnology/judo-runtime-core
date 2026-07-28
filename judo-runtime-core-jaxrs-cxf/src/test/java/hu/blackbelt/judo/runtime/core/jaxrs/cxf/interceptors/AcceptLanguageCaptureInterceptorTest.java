package hu.blackbelt.judo.runtime.core.jaxrs.cxf.interceptors;

/*-
 * #%L
 * CXF JAX-RS application manager
 * %%
 * Copyright (C) 2018 - 2023 BlackBelt Technology
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import hu.blackbelt.judo.runtime.core.RequestLocaleHolder;
import org.apache.cxf.message.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link AcceptLanguageCaptureInterceptor} moves the incoming {@code Accept-Language}
 * header into {@link RequestLocaleHolder} (case-insensitively), and always overwrites so a pooled
 * thread never keeps a previous request's value.
 */
class AcceptLanguageCaptureInterceptorTest {

    private final AcceptLanguageCaptureInterceptor interceptor = new AcceptLanguageCaptureInterceptor();

    @AfterEach
    void tearDown() {
        RequestLocaleHolder.clear();
    }

    private static Message messageWithHeaders(final Map<String, List<String>> headers) {
        final Message message = mock(Message.class);
        when(message.get(Message.PROTOCOL_HEADERS)).thenReturn(headers);
        return message;
    }

    @Test
    void capturesHeaderValue() {
        interceptor.handleMessage(messageWithHeaders(
                Collections.singletonMap("Accept-Language", List.of("hu-HU,en-US;q=0.7"))));
        assertThat(RequestLocaleHolder.getAcceptLanguage(), is("hu-HU,en-US;q=0.7"));
    }

    @Test
    void headerLookupIsCaseInsensitive() {
        final Map<String, List<String>> headers = new TreeMap<>();
        headers.put("accept-language", List.of("de-DE"));
        interceptor.handleMessage(messageWithHeaders(headers));
        assertThat(RequestLocaleHolder.getAcceptLanguage(), is("de-DE"));
    }

    @Test
    void absentHeaderClearsHolder() {
        RequestLocaleHolder.set("hu-HU"); // stale value from a previous request on this thread
        interceptor.handleMessage(messageWithHeaders(Collections.emptyMap()));
        assertThat(RequestLocaleHolder.getAcceptLanguage(), is(nullValue()));
    }

    @Test
    void noProtocolHeadersClearsHolder() {
        RequestLocaleHolder.set("hu-HU");
        final Message message = mock(Message.class);
        when(message.get(Message.PROTOCOL_HEADERS)).thenReturn(null);
        interceptor.handleMessage(message);
        assertThat(RequestLocaleHolder.getAcceptLanguage(), is(nullValue()));
    }

    @Test
    void handleFaultClearsHolder() {
        RequestLocaleHolder.set("hu-HU");
        interceptor.handleFault(mock(Message.class));
        assertThat(RequestLocaleHolder.getAcceptLanguage(), is(nullValue()));
    }
}
