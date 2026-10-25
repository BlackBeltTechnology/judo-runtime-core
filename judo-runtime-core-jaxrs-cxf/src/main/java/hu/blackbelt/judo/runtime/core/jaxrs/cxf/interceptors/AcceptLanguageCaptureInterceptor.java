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
import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.message.Message;
import org.apache.cxf.phase.AbstractPhaseInterceptor;
import org.apache.cxf.phase.Phase;

import java.util.List;
import java.util.Map;

/**
 * Captures the incoming {@code Accept-Language} header into {@link RequestLocaleHolder} at the start
 * of every request, so the request-aware {@code PrincipalLocaleProvider} can honor it for anonymous
 * (unauthenticated) requests when localizing backend messages.
 *
 * <p>Unlike the dispatcher's request-scoped {@code Context} (which is cleared per exposed operation),
 * the thread-local holder survives until message formatting. The value is set on every request
 * (with {@code null} when the header is absent) so a pooled thread never observes a stale value, and
 * is cleared on fault.
 */
public class AcceptLanguageCaptureInterceptor extends AbstractPhaseInterceptor<Message> {

    private static final String ACCEPT_LANGUAGE = "Accept-Language";

    public AcceptLanguageCaptureInterceptor() {
        super(Phase.RECEIVE);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void handleMessage(final Message message) throws Fault {
        String acceptLanguage = null;
        final Object protocolHeaders = message.get(Message.PROTOCOL_HEADERS);
        if (protocolHeaders instanceof Map) {
            final Map<String, List<String>> headers = (Map<String, List<String>>) protocolHeaders;
            for (final Map.Entry<String, List<String>> entry : headers.entrySet()) {
                if (ACCEPT_LANGUAGE.equalsIgnoreCase(entry.getKey())
                        && entry.getValue() != null && !entry.getValue().isEmpty()) {
                    acceptLanguage = entry.getValue().get(0);
                    break;
                }
            }
        }
        // Always set (possibly null) so a reused thread never keeps a previous request's value.
        RequestLocaleHolder.set(acceptLanguage);
    }

    @Override
    public void handleFault(final Message message) {
        RequestLocaleHolder.clear();
    }
}
