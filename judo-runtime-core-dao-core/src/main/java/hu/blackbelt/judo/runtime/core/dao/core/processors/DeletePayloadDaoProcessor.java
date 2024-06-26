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
import com.google.common.collect.Sets;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceGraph;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceReference;
import hu.blackbelt.judo.runtime.core.dao.core.statements.DeleteStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.InstanceExistsValidationStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.RemoveReferenceStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.Statement;
import hu.blackbelt.judo.runtime.core.dao.core.values.InstanceValue;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.resource.ResourceSet;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getClassifierFQName;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;


/**
 * Analyzing the deleted entities recursively and generating the required executable statements.
 * The entities can be transfer objects which are mapped to other entities via aliases.
 * <p>
 * Two API is used to determinate states:
 * - All possible statement for a deletion of a type over query factory - structural behaviour
 * - Instance collector which contain all of the containments / references for given instance.
 * <p>
 * The API traverse the instances graph with the structural graph which will collects all the statements.
 */
@Slf4j(topic = "dao-core")
public class DeletePayloadDaoProcessor<ID> extends PayloadDaoProcessor<ID> {

    public DeletePayloadDaoProcessor(ResourceSet resourceSet, IdentifierProvider<ID> identifierProvider,
                                     QueryFactory queryFactory, InstanceCollector<ID> instanceCollector) {
        super(resourceSet, identifierProvider, queryFactory, instanceCollector);
    }

    public Collection<Statement<ID>> delete(EClass mappedTransferObjectType,
                                            Collection<ID> ids) {

        checkArgument(mappedTransferObjectType != null, "Type is mandatory");
        checkArgument(ids != null, "ID list is mandatory");

        Optional<EClass> mappedEntity = getAsmUtils().getMappedEntityType(mappedTransferObjectType);
        checkState(mappedEntity.isPresent(),
                format("No mapped type is presented, and given class is not type: %s",
                        getClassifierFQName(mappedTransferObjectType)));


        EClass entityType = mappedEntity.get();

        Collection<Statement<ID>> statements = Sets.newHashSet();

        Map<ID, InstanceGraph<ID>> instanceGraphMap = getInstanceCollector().collectGraph(entityType, ids);

        // Just for check all of the given ID's existence are checked
        statements.addAll(ids.stream().map(
                id -> InstanceExistsValidationStatement.<ID>buildInstanceExistsValidationStatement()
                        .identifier(id)
                        .type(entityType)
                        .build())
                .collect(toSet()));

        // All of returned instancegraph elements have to be processed
        for (InstanceGraph<ID> e:  instanceGraphMap.values()) {
            // Collect contained elements
            collectStatements(entityType, e, statements, null, null);
        }

        return ImmutableSet.copyOf(statements);
    }

    void collectStatements(EClass entityType,
                         InstanceGraph<ID> instanceGraph,
                         Collection<Statement<ID>> statements,
                         InstanceGraph<ID> containerInstanceGraph,
                         EReference container) {

        statementCollector(entityType, instanceGraph, statements, containerInstanceGraph, container);
        Collection<InstanceValue<ID>> visited = Sets.newHashSet();
        checkMandatoryBackReferences(entityType, instanceGraph, statements.stream().filter(s -> s instanceof DeleteStatement<ID>).collect(toSet()), container, visited);
    }


    private void statementCollector(EClass entityType,
                         InstanceGraph<ID> instanceGraph,
                         Collection<Statement<ID>> statements,
                         InstanceGraph<ID> containerInstanceGraph,
                         EReference container) {
        // Add current instance graph element as DeleteStatement
        InstanceValue<ID> instanceValue = (InstanceValue<ID>) InstanceValue.<ID>buildInstanceValue()
                .type(entityType)
                .identifier(instanceGraph.getId())
                .build();

        if (statements.stream().anyMatch(s -> s instanceof DeleteStatement && Objects.equals(instanceValue, s.getInstance()))) {
            log.debug("Circular delete found, stop collecting statements");
            return;
        }
        statements.add(new DeleteStatement<ID>(instanceValue));
        if (container != null) {
            statements.add(InstanceExistsValidationStatement.<ID>buildInstanceExistsValidationStatement()
                    .type(container.getEContainingClass())
                    .identifier(containerInstanceGraph.getId())
                    .build());

            statements.add(InstanceExistsValidationStatement.<ID>buildInstanceExistsValidationStatement()
                    .type(entityType)
                    .identifier(instanceGraph.getId())
                    .build());

            statements.add(RemoveReferenceStatement.<ID>buildRemoveReferenceStatement()
                    .reference(container)
                    .identifier(containerInstanceGraph.getId())
                    .type(entityType)
                    .referenceIdentifier(instanceGraph.getId())
                    .build());
        }

        Collection<EReference> processedReferences = Sets.newHashSet();
        if (container != null) {
            processedReferences.add(container);
            if (container.getEOpposite() != null) {
                processedReferences.add(container.getEOpposite());
            }
        }

        Set<ID> collectRemoveReferenceStatementIds = statements.stream().filter(s -> s instanceof RemoveReferenceStatement<ID>).map(s -> s.getInstance().getIdentifier()).collect(toSet());

        // All references have to remove before deleting. It will contain the containment references too.
        for (InstanceReference<ID> ref :  instanceGraph.getReferences().stream()
                .filter(r -> !processedReferences.contains(r.getReference()))
                .filter(r -> r.getReference().getEOpposite() == null || !processedReferences.contains(r.getReference().getEOpposite())).toList()) {

            if(ref.getReference().getEOpposite() == null || !collectRemoveReferenceStatementIds.contains(ref.getReferencedElement().getId())) {
                collectRemoveReferenceStatementIds.add(ref.getReferencedElement().getId());
                statements.add(InstanceExistsValidationStatement.<ID>buildInstanceExistsValidationStatement()
                        .type(ref.getReference().getEContainingClass())
                        .identifier(instanceGraph.getId())
                        .build());

                statements.add(InstanceExistsValidationStatement.<ID>buildInstanceExistsValidationStatement()
                        .type(ref.getReference().getEReferenceType())
                        .identifier(ref.getReferencedElement().getId())
                        .build());

                statements.add(RemoveReferenceStatement.<ID>buildRemoveReferenceStatement()
                        .type(ref.getReference().getEContainingClass())
                        .reference(ref.getReference())
                        .referenceIdentifier(ref.getReferencedElement().getId())
                        .identifier(instanceGraph.getId())
                        .build());
            }
            if (ref.getReference().getEOpposite() != null && AsmUtils.annotatedAsTrue(ref.getReference().getEOpposite(), "reverseCascadeDelete")) {
                statementCollector(ref.getReference().getEReferenceType(),
                        getInstanceCollector().collectGraph(ref.getReference().getEReferenceType(), ref.getReferencedElement().getId()),
                        statements,
                        null,
                        null);
            }
        }

        // All back references to remove before deleting. It will contain the containment references too.
        for (InstanceReference<ID> ref : instanceGraph.getBackReferences().stream()
                .filter(r -> !processedReferences.contains(r.getReference()))
                .filter(r -> r.getReference().getEOpposite() == null).toList()) {

            statements.add(InstanceExistsValidationStatement.<ID>buildInstanceExistsValidationStatement()
                    .type(ref.getReference().getEContainingClass())
                    .identifier(ref.getReferencedElement().getId())
                    .build());

            statements.add(InstanceExistsValidationStatement.<ID>buildInstanceExistsValidationStatement()
                    .type(ref.getReference().getEReferenceType())
                    .identifier(instanceGraph.getId())
                    .build());

            if (AsmUtils.annotatedAsTrue(ref.getReference(), "reverseCascadeDelete")) {
                statementCollector(ref.getReference().getEContainingClass(),
                        getInstanceCollector().collectGraph(ref.getReference().getEContainingClass(), ref.getReferencedElement().getId()),
                        statements,
                        null,
                        null);
            } else {
                statements.add(RemoveReferenceStatement.<ID>buildRemoveReferenceStatement()
                        .type(ref.getReference().getEContainingClass())
                        .reference(ref.getReference())
                        .referenceIdentifier(instanceGraph.getId())
                        .identifier(ref.getReferencedElement().getId())
                        .build());
            }
        }

        // Make delete for all containment
        for (InstanceReference<ID> containment : instanceGraph.getContainments()) {
            InstanceGraph<ID> containedGraph = containment.getReferencedElement();
            statementCollector(containment.getReference().getEReferenceType(), containedGraph, statements, instanceGraph, containment.getReference());
        }
    }

    private void checkMandatoryBackReferences(EClass entityType,
                         InstanceGraph<ID> instanceGraph,
                         Collection<Statement<ID>> statements,
                         EReference container,
                         Collection<InstanceValue<ID>> visited) {

        InstanceValue<ID> instanceValue = InstanceValue.<ID>buildInstanceValue()
                .type(entityType)
                .identifier(instanceGraph.getId())
                .build();

        if (visited.stream().anyMatch(s -> Objects.equals(instanceValue, s))) {
            log.debug("Circular delete found, stop collecting statements");
            return;
        }
        visited.add(instanceValue);

        // Check if there is any Remove Reference related to mandatory relation
        Collection<InstanceReference<ID>> mandatoryReferencesToRemove = instanceGraph.getBackReferences().stream()
                .filter(r -> r.getReference().getLowerBound() > 0 && !AsmUtils.annotatedAsTrue(r.getReference(), "reverseCascadeDelete")).collect(Collectors.toSet());
        checkState(mandatoryReferencesToRemove.stream()
                        .noneMatch(r -> statements.stream().noneMatch(s -> s instanceof DeleteStatement && Objects.equals(s.getInstance().getIdentifier(), r.getReferencedElement().getId()))),
                "There are mandatory references that cannot be removed: " +
                        " Type: " + entityType + " ID: " + instanceGraph.getId() + " Mandatory back references: " +
                        mandatoryReferencesToRemove.stream().map(
                                r -> r.getReference().getEContainingClass() + "#"
                                        + r.getReference().getName() + " ID: "
                                        + r.getReferencedElement().getId()).collect(toSet()));

        Collection<EReference> processedReferences = Sets.newHashSet();
        if (container != null) {
            processedReferences.add(container);
            if (container.getEOpposite() != null) {
                processedReferences.add(container.getEOpposite());
            }
        }

        for (InstanceReference<ID> ref : instanceGraph.getReferences()
                .stream()
                .filter(r -> !processedReferences.contains(r.getReference()))
                .filter(r -> r.getReference().getEOpposite() == null || !processedReferences.contains(r.getReference().getEOpposite()))
                .toList()) {
            if (ref.getReference().getEOpposite() != null && AsmUtils.annotatedAsTrue(ref.getReference().getEOpposite(), "reverseCascadeDelete")) {
                checkMandatoryBackReferences(ref.getReference().getEReferenceType(),
                        getInstanceCollector().collectGraph(ref.getReference().getEReferenceType(), ref.getReferencedElement().getId()),
                        statements,
                        null,
                        visited);
            }
        }

        for (InstanceReference<ID> ref : instanceGraph.getBackReferences().stream().filter(r -> !processedReferences.contains(r.getReference())).filter(r -> r.getReference().getEOpposite() == null).toList()) {
            if (AsmUtils.annotatedAsTrue(ref.getReference(), "reverseCascadeDelete")) {
                checkMandatoryBackReferences(ref.getReference().getEContainingClass(),
                        getInstanceCollector().collectGraph(ref.getReference().getEContainingClass(), ref.getReferencedElement().getId()),
                        statements,
                        null,
                        visited);
            }
        }

        // Check containments
        for (InstanceReference<ID> containment : instanceGraph.getContainments()) {
            InstanceGraph<ID> containedGraph = containment.getReferencedElement();
            checkMandatoryBackReferences(containment.getReference().getEReferenceType(), containedGraph, statements, containment.getReference(),visited);
        }

    }
}
