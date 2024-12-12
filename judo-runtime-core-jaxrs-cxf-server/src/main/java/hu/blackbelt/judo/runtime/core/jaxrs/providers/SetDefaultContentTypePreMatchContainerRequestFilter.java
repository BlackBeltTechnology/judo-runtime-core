package hu.blackbelt.judo.runtime.core.jaxrs.providers;

/*-
 * #%L
 * JUDO Services CXF
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

import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;
import java.util.Arrays;
import java.util.Optional;

@Provider
@PreMatching
@NoArgsConstructor
@Slf4j
public class SetDefaultContentTypePreMatchContainerRequestFilter implements ContainerRequestFilter {

    public static final String CONTENT_TYPE = "Content-Type";
    public static final String APPLICTION_JSON = "application/json";


    private String defaultRequestContentType = APPLICTION_JSON;

    @Builder
    public SetDefaultContentTypePreMatchContainerRequestFilter(String defaultRequestContentType) {
        this.defaultRequestContentType = Optional.ofNullable(defaultRequestContentType).orElse(this.defaultRequestContentType);

    }

    @Override
    public void filter(ContainerRequestContext containerRequestContext) {
        final String method = containerRequestContext.getMethod();
        if ("GET".equals(method) || "DELETE".equals(method)) {
            // do not set default content type if body is not supported
            return;
        }
        if (containerRequestContext.getHeaderString(CONTENT_TYPE) == null || "".equals(containerRequestContext.getHeaderString(CONTENT_TYPE).trim())) {
            containerRequestContext.getHeaders().put(CONTENT_TYPE, Arrays.asList(defaultRequestContentType));
        }
    }
}
