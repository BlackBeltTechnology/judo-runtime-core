package hu.blackbelt.judo.runtime.core.exception;

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

import hu.blackbelt.judo.dao.api.ValidationResult;

public class AccessDeniedException extends ClientException {

	private static final long serialVersionUID = -8619727759658375103L;
	private final ValidationResult validationResult;

    public AccessDeniedException(ValidationResult validationResult) {
        super();
        this.validationResult = validationResult;
    }

    @Override
    public Integer getStatusCode() {
        return 403;
    }

    @Override
    public Object getDetails() {
        return validationResult;
    }
}
