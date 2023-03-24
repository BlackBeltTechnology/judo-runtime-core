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

import java.time.LocalDateTime;

@Getter
public class UpdateStatement<ID> extends Statement<ID> {

    private final Integer version;
    private final ID userId;
    private final String userName;
    private final LocalDateTime timestamp;

    @Builder(builderMethodName = "buildUpdateStatement")
    public UpdateStatement(
            EClass type,
            ID identifier,
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
        this.version = version;
        this.userId = userId;
        this.userName = username;
        this.timestamp = timestamp;
    }

    public String toString() {
        return "UpdateStatement(" + this.instance.toString() +  ")";
    }
}
