package hu.blackbelt.judo.runtime.core.dao.core.statements;

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

import hu.blackbelt.judo.runtime.core.dao.core.values.InstanceValue;
import lombok.Builder;
import lombok.Getter;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.time.LocalDateTime;
import java.util.Optional;


@Getter
public class InsertStatement<ID> extends Statement<ID> {

    private final EReference container;
    private final Object clientReferenceIdentifier;
    private final Integer version;
    private final ID userId;
    private final String userName;
    private final LocalDateTime timestamp;

    @Builder(builderMethodName = "buildInsertStatement")
    public InsertStatement(
            EClass type,
            ID identifier,
            Object clientReferenceIdentifier,
            EReference container,
            Integer version,
            ID userId,
            String username,
            LocalDateTime timestamp
    ) {

        super(InstanceValue
                        .<ID>buildInstanceValue()
                            .type(type)
                            .identifier(identifier)
                            .build());

        this.container = container;
        this.clientReferenceIdentifier = clientReferenceIdentifier;
        this.version = version;
        this.userId = userId;
        this.userName = username;
        this.timestamp = timestamp;
    }

    public Optional<EReference> getContainer() {
        return Optional.ofNullable(container);
    }

    public String toString() {
        return "InsertStatement(" + this.instance.toString() +  ")";
    }
}
