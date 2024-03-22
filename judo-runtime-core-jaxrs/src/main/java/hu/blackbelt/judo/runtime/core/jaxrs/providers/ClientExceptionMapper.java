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

import hu.blackbelt.judo.runtime.core.exception.ClientException;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
@Slf4j
public class ClientExceptionMapper implements ExceptionMapper<ClientException> {

    @Setter
    boolean logException = false;

    @Override
    public Response toResponse(ClientException exception) {
        if (logException) {
            log.error("ClientError", exception);
        }
        return Response.status(exception.getStatusCode() != null ? exception.getStatusCode() : Response.Status.BAD_REQUEST.getStatusCode())
                .entity(exception.getDetails())
                .type("application/json")
                .build();
    }
}
