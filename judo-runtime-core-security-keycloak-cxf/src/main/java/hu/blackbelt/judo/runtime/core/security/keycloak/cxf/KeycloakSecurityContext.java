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

import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import lombok.Builder;
import lombok.Getter;
import org.apache.cxf.security.SecurityContext;

import java.util.Objects;

@Getter
@Builder
public class KeycloakSecurityContext implements SecurityContext {

    private JudoPrincipal userPrincipal;

    private static final String ISSUED_FOR_KEY = "azp";

    @Override
    public boolean isUserInRole(final String role) {
        return Objects.equals(role, userPrincipal.getAttributes().get(ISSUED_FOR_KEY));
    }
}
