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
import lombok.Getter;

import java.util.Collection;

public class ValidationException extends ClientException {

    private static final long serialVersionUID = 1550702685342981741L;

    @Getter
    private final Collection<ValidationResult> validationResults;

    public ValidationException(final Collection<ValidationResult> validationResults) {
        super();
        this.validationResults = validationResults;
    }

    public ValidationException(final String message, final Collection<ValidationResult> validationResults) {
        super(message);
        this.validationResults = validationResults;
    }

    @Override
    public Integer getStatusCode() {
        return 400;
    }

    @Override
    public Object getDetails() {
        return validationResults;
    }

    @Override
    public String toString() {
        String s = getClass().getName();
        String message = getLocalizedMessage();
        return (message != null) ? (s + ": " + message + " " + getDetails()) : (s + ": " + getDetails());
    }
}
