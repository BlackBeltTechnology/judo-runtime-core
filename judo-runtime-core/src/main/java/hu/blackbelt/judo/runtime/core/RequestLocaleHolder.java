package hu.blackbelt.judo.runtime.core;

/*-
 * #%L
 * JUDO Runtime Core
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

/**
 * Thread-local holder for the current request's raw {@code Accept-Language} header.
 *
 * <p>Needed because the dispatcher clears its request-scoped {@code Context}
 * ({@code context.removeAll()}) at the start of every exposed operation, so a value stashed there by
 * an HTTP-layer filter would not survive until message formatting. A transport interceptor (which has
 * access to the HTTP request) sets this holder at request start; the request-aware
 * {@code PrincipalLocaleProvider} reads it in the anonymous (no-principal) branch.
 *
 * <p>Lives in the core module so both the transport layer (writer) and the dispatcher (reader) can
 * share it without a cross-layer dependency.
 *
 * <p>The interceptor MUST {@link #set(String)} on every request (with {@code null} when the header is
 * absent) so a pooled thread never observes a previous request's value.
 */
public final class RequestLocaleHolder {

    private static final ThreadLocal<String> ACCEPT_LANGUAGE = new ThreadLocal<>();

    private RequestLocaleHolder() {
    }

    /**
     * Set (or clear, when {@code null}/blank) the current thread's {@code Accept-Language} value.
     */
    public static void set(final String acceptLanguage) {
        if (acceptLanguage == null || acceptLanguage.trim().isEmpty()) {
            ACCEPT_LANGUAGE.remove();
        } else {
            ACCEPT_LANGUAGE.set(acceptLanguage);
        }
    }

    /**
     * @return the current thread's raw {@code Accept-Language} header, or {@code null} if none.
     */
    public static String getAcceptLanguage() {
        return ACCEPT_LANGUAGE.get();
    }

    /**
     * Remove the current thread's value. Interceptors should call this when the request completes.
     */
    public static void clear() {
        ACCEPT_LANGUAGE.remove();
    }
}
