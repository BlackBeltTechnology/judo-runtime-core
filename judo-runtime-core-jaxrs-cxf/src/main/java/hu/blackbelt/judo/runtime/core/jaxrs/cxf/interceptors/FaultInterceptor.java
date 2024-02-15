package hu.blackbelt.judo.runtime.core.jaxrs.cxf.interceptors;

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

import hu.blackbelt.judo.runtime.core.exception.ClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.message.FaultMode;
import org.apache.cxf.message.Message;
import org.apache.cxf.phase.AbstractPhaseInterceptor;
import org.apache.cxf.phase.Phase;
import org.apache.cxf.transport.http.AbstractHTTPDestination;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.InternalServerErrorException;

@Slf4j
public class FaultInterceptor extends AbstractPhaseInterceptor<Message> {

    public FaultInterceptor() {
        super(Phase.PRE_STREAM);
    }

    @Override
    public void handleMessage(Message message) throws Fault {
        handleFault(message);
    }

    @Override
    public void handleFault(final Message message) {
        final FaultMode mode = message.get(FaultMode.class);

        final Exception exception = message.getExchange().get(Exception.class);
        if (exception == null) {
            throw new RuntimeException("Exception is expected");
        }

        if (mode != null) {
            // handled by fault handler of the given mode
            return;
        }

        final int statusCode;
        if (exception instanceof InternalServerErrorException) {
            if (exception.getCause() != null && exception.getCause().getClass().getName().startsWith("com.fasterxml.jackson")) {
                log.debug("JSON processing failed", exception);
                statusCode = HttpServletResponse.SC_BAD_REQUEST;
                message.put(FaultMode.class, FaultMode.CHECKED_APPLICATION_FAULT);
            } else if (exception.getCause() == null || exception.getCause() instanceof RuntimeException) {
                log.info("Internal server error", exception);
                statusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
                message.put(FaultMode.class, FaultMode.RUNTIME_FAULT);
            } else {
                log.info("Internal server error, checked exception thrown by operation", exception);
                statusCode = HttpServletResponse.SC_BAD_REQUEST;
                message.put(FaultMode.class, FaultMode.UNCHECKED_APPLICATION_FAULT);
            }
        } else if (exception instanceof ClientException) {
            log.debug("Client exception thrown by operation", exception);
            statusCode = ((ClientException) exception).getStatusCode();
            message.put(FaultMode.class, FaultMode.CHECKED_APPLICATION_FAULT);
        } else if (exception instanceof RuntimeException) {
            log.info("Runtime exception thrown by operation", exception);
            statusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
            message.put(FaultMode.class, FaultMode.RUNTIME_FAULT);
        } else {
            log.debug("Checked exception thrown by operation", exception);
            statusCode = HttpServletResponse.SC_BAD_REQUEST;
            message.put(FaultMode.class, FaultMode.CHECKED_APPLICATION_FAULT);
        }

        final HttpServletResponse response = (HttpServletResponse)message.getExchange()
                .getInMessage().get(AbstractHTTPDestination.HTTP_RESPONSE);
        response.setStatus(statusCode);
    }
}
