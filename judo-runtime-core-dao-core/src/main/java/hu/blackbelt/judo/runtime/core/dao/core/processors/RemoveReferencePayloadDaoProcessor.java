package hu.blackbelt.judo.runtime.core.dao.core.processors;

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

import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.core.statements.InstanceExistsValidationStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.Statement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.RemoveReferenceStatement;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.resource.ResourceSet;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newHashSet;


/**
 * Creating statement for unsetting a references. It will remove and detach all the required data.
 */
public class RemoveReferencePayloadDaoProcessor<ID> extends PayloadDaoProcessor<ID> {

    public RemoveReferencePayloadDaoProcessor(ResourceSet resourceSet, IdentifierProvider<ID> idIdentifierProvider,
                                              QueryFactory queryFactory, InstanceCollector<ID> instanceCollector) {
        super(resourceSet, idIdentifierProvider, queryFactory, instanceCollector);
    }

    public Collection<Statement<ID>> removeReference(EReference reference, Collection<ID> identifiers, ID parentIdentifier, boolean existenceCheck) {
        checkArgument(reference != null, "Type is mandatory");
        checkArgument(parentIdentifier != null, "Parent identifier is mandatory");

        Collection<Statement<ID>> statements = newHashSet();

        // If reference madatory it is not allowed
        if (reference.isRequired()) {
            // statements.add(new UnsetRelationStatement(reference.getEReferenceType(), payload, reference, parentIdentifier));
        }

        identifiers.stream().forEach(identifier -> {
            if (existenceCheck) {
                statements.add(InstanceExistsValidationStatement.<ID>buildInstanceExistsValidationStatement()
                        .type(reference.getEReferenceType())
                        .identifier(identifier)
                        .build());
            }

            statements.add(RemoveReferenceStatement.<ID>buildRemoveReferenceStatement()
                    .type(reference.getEReferenceType())
                    .reference(reference)
                    .identifier(parentIdentifier)
                    .referenceIdentifier(identifier)
                    .build());
        });


        return ImmutableSet.copyOf(statements);
    }

}
