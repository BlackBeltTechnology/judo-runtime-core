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

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.*;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

@Getter
public class RemoveReferenceStatement<ID> extends ReferenceStatement<ID> {

    @Builder(builderMethodName = "buildRemoveReferenceStatement")
    public RemoveReferenceStatement(
            EClass type,
            EReference reference,
            ID identifier,
            ID referenceIdentifier) {
        super(type, reference, identifier, referenceIdentifier);
    }

    public String toString() {
        return "RemoveReferenceStatement(" +
                "identifier=" + this.getIdentifier() +
                ", reference=" + AsmUtils.getReferenceFQName(this.getReference()) +
                ", instance=" + this.instance.toString() + ")";
    }
}
