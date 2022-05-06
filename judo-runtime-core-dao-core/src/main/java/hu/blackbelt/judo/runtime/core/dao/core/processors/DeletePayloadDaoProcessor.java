package hu.blackbelt.judo.runtime.core.dao.core.processors;

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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
                                     QueryFactory queryFactory, InstanceCollector instanceCollector) {
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
        instanceGraphMap.values().stream().forEach(e -> {
            // Collect contained elements
            collectStaments(entityType, e, statements, null, null);
        });

        return ImmutableSet.copyOf(statements);
    }

    void collectStaments(EClass entityType,
                         InstanceGraph<ID> instanceGraph,
                         Collection<Statement<ID>> statements,
                         InstanceGraph<ID> containerInstanceGraph,
                         EReference container) {
        // Add current instance graph element as DeleteStatement
        InstanceValue<ID> instanceValue = (InstanceValue<ID>) InstanceValue.buildInstanceValue()
                .type(entityType)
                .identifier(instanceGraph.getId())
                .build();

        if (statements.stream().anyMatch(s -> s instanceof DeleteStatement && Objects.equals(instanceValue, s.getInstance()))) {
            log.debug("Circular delete found, stop collecting statements");
            return;
        }
        statements.add(new DeleteStatement(instanceValue));
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

        // Check if there is any Remove Reference related to mandatory relation
        Collection<InstanceReference> mandatoryReferencesToRemove = instanceGraph.getBackReferences().stream()
                .filter(r -> r.getReference().getLowerBound() > 0 && !AsmUtils.annotatedAsTrue(r.getReference(), "reverseCascadeDelete")).collect(Collectors.toSet());
        checkState(mandatoryReferencesToRemove.stream()
                        .noneMatch(r -> statements.stream().noneMatch(s -> s instanceof DeleteStatement && Objects.equals(s.getInstance().getIdentifier(), r.getReferencedElement().getId()))),
                "There is mandatory references which is not removable: " +
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

        // All reference have to remove before delete. It will contain the containment references too.
        instanceGraph.getReferences().stream()
                .filter(r -> !processedReferences.contains(r.getReference()))
                .filter(r -> r.getReference().getEOpposite() == null || !processedReferences.contains(r.getReference().getEOpposite()))
                .forEach(r -> {
                    statements.add(InstanceExistsValidationStatement.<ID>buildInstanceExistsValidationStatement()
                            .type(r.getReference().getEContainingClass())
                            .identifier(instanceGraph.getId())
                            .build());

                    statements.add(InstanceExistsValidationStatement.<ID>buildInstanceExistsValidationStatement()
                            .type(r.getReference().getEReferenceType())
                            .identifier(r.getReferencedElement().getId())
                            .build());

                    statements.add(RemoveReferenceStatement.<ID>buildRemoveReferenceStatement()
                            .type(r.getReference().getEContainingClass())
                            .reference(r.getReference())
                            .referenceIdentifier(r.getReferencedElement().getId())
                            .identifier(instanceGraph.getId())
                            .build());

                    if (r.getReference().getEOpposite() != null && AsmUtils.annotatedAsTrue(r.getReference().getEOpposite(), "reverseCascadeDelete")) {
                        collectStaments(r.getReference().getEReferenceType(),
                                getInstanceCollector().collectGraph(r.getReference().getEReferenceType(), r.getReferencedElement().getId()),
                                statements,
                                null,
                                null);
                    }
                });


        // All back reference to remove before delete. It will contain the containment references too.
        instanceGraph.getBackReferences().stream()
                .filter(r -> !processedReferences.contains(r.getReference()))
                .filter(r -> r.getReference().getEOpposite() == null)
                .forEach(r -> {
                    statements.add(InstanceExistsValidationStatement.<ID>buildInstanceExistsValidationStatement()
                            .type(r.getReference().getEContainingClass())
                            .identifier(r.getReferencedElement().getId())
                            .build());

                    statements.add(InstanceExistsValidationStatement.<ID>buildInstanceExistsValidationStatement()
                            .type(r.getReference().getEReferenceType())
                            .identifier(instanceGraph.getId())
                            .build());

                    if (AsmUtils.annotatedAsTrue(r.getReference(), "reverseCascadeDelete")) {
                        collectStaments(r.getReference().getEContainingClass(),
                                getInstanceCollector().collectGraph(r.getReference().getEContainingClass(), r.getReferencedElement().getId()),
                                statements,
                                null,
                                null);
                    } else {
                        statements.add(RemoveReferenceStatement.<ID>buildRemoveReferenceStatement()
                                .type(r.getReference().getEContainingClass())
                                .reference(r.getReference())
                                .referenceIdentifier(instanceGraph.getId())
                                .identifier(r.getReferencedElement().getId())
                                .build());
                    }
                });


        // Make delete for all containment
        instanceGraph.getContainments().stream()
                .forEach(i -> {
                    InstanceGraph<ID> containedGraph = i.getReferencedElement();
                    collectStaments(i.getReference().getEReferenceType(), containedGraph, statements, instanceGraph, i.getReference());
                });
    }
}
