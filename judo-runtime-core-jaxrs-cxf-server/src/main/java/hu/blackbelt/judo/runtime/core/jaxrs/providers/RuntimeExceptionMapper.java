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

import hu.blackbelt.judo.dispatcher.api.BusinessException;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

@Provider
@Slf4j
@NoArgsConstructor
public class RuntimeExceptionMapper implements ExceptionMapper<RuntimeException> {

    private static final String FAULT_HEADER = "X-Fault";

    private Boolean returnRuntimeExceptions = false;
    private Boolean includeBusinessCause = false;

    @Builder
    public RuntimeExceptionMapper(
            Boolean returnRuntimeExceptions,
            Boolean includeBusinessCause
    ) {
        this.returnRuntimeExceptions = Optional.ofNullable(returnRuntimeExceptions).orElse(this.returnRuntimeExceptions);
        this.includeBusinessCause = Optional.ofNullable(includeBusinessCause).orElse(this.includeBusinessCause);
    }

    @Override
    public Response toResponse(final RuntimeException exception) {
        final Map<String, Object> result = new TreeMap<>();
        result.put("level", "ERROR");
        final Map<String, Object> details = new LinkedHashMap<>();
        result.put("details", details);
        final int statusCode;

        String fault = null;
        if (exception instanceof ClientErrorException) {
            ClientErrorException clientErrorException = (ClientErrorException) exception;
            return clientErrorException.getResponse();
        } else if (exception instanceof WebApplicationException) {
            if (exception.getCause() != null && exception.getCause().getClass().getName().startsWith("com.fasterxml.jackson")) {
                statusCode = HttpServletResponse.SC_BAD_REQUEST;
                result.put("code", "INVALID_JSON");
            } else if (exception.getCause() == null || exception.getCause() instanceof RuntimeException) {
                statusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
            } else {
                statusCode = HttpServletResponse.SC_BAD_REQUEST;
            }

            log.info("Request failed, JAX-RS exception thrown by application", exception);
        } else if (exception instanceof BusinessException) {
            fault = ((BusinessException) exception).getType();
            final String errorCode = ((BusinessException) exception).getErrorCode();
            final Throwable cause = exception.getCause();
            result.remove("level");
            result.remove("details");
            result.put(fault, ((BusinessException) exception).getDetails().entrySet().stream()
                    .filter(e -> !e.getKey().startsWith("_") && e.getValue() != null)
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
            if (errorCode != null) {
                result.put("code", errorCode);
            }
            if (cause != null && includeBusinessCause) {
                result.put("cause", cause);
            }
            statusCode = 422;

            log.info("Business exception thrown by operation", cause);
        } else {
            result.put("code", "INTERNAL_SERVER_ERROR");
            statusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;

            log.error("Request failed, runtime exception thrown by application", exception);
        }
        if (returnRuntimeExceptions && !(exception instanceof BusinessException)) {
            final StringWriter writer = new StringWriter();
            exception.printStackTrace(new PrintWriter(writer));
            details.put("exception", writer.toString());
        }

        Response.ResponseBuilder builder = Response.status(statusCode);
        if (fault != null) {
            builder = builder.header(FAULT_HEADER, fault);
        }
        return builder.entity(result)
                .type("application/json")
                .build();
    }
}
