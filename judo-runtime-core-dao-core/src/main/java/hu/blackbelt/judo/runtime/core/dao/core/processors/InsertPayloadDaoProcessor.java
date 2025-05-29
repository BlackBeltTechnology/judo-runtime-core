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
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.core.statements.AddReferenceStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.InsertStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.Statement;
import hu.blackbelt.judo.runtime.core.dao.core.values.Metadata;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.resource.ResourceSet;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getReferenceFQName;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;


/**
 * Analyze the inserted entities recursively and generate the required executable statements.
 * The entities must be transfer objects mapped to other entities through aliases.
 *
 * Rules:
 *    - The root type cannot have ID - because its is update
 *    - Any relations which are NULL ignored
 *    - Any entities which have ID attached relations are ignored.
 *    - Any entities which hae no ID are inserted recursively - does not matter it is containment or association
 */
public class InsertPayloadDaoProcessor extends PayloadDaoProcessor {

    private final AddReferencePayloadDaoProcessor addReferenceProcessor;

    private final BiConsumer<EClass, Payload> defaultValuesApplier;

    Metadata metadata;

    public InsertPayloadDaoProcessor(ResourceSet resourceSet, IdentifierProvider identifierProvider,
                                     QueryFactory queryFactory, InstanceCollector instanceCollector,
                                     BiConsumer<EClass, Payload> defaultValuesApplier,
                                     Metadata metadata) {
        super(resourceSet, identifierProvider, queryFactory, instanceCollector);
        addReferenceProcessor =
                new AddReferencePayloadDaoProcessor(resourceSet, identifierProvider, queryFactory, instanceCollector);
        this.defaultValuesApplier = defaultValuesApplier;
        this.metadata = metadata;
    }

    public Collection<Statement> insert(EClass type,
                                  Payload payload,
                                  boolean checkMandatoryFeatures) {
        Collection<Statement> statements = newHashSet();
        collectStatements(type, payload, null, statements, checkMandatoryFeatures);
        return statements;
    }

    @SuppressWarnings("unchecked")
    Collection<Statement> collectStatements(EClass mappedTransferObjectType,
                                            Payload payload,
                                            EReference container, Collection<Statement> statements,
                                            boolean checkMandatoryFeatures) {

        checkArgument(mappedTransferObjectType != null, "Type is mandatory");
        checkArgument(payload != null, "Payload is mandatory");

        checkArgument(getAsmUtils().isMappedTransferObjectType(mappedTransferObjectType), "Type have to be mapped transfer object");

        defaultValuesApplier.accept(mappedTransferObjectType, payload);

        InsertStatement.InsertStatementBuilder currentStatementBuilder =
                InsertStatement.buildInsertStatement()
                        .identifier(getIdentifierProvider().get())
                        .clientReferenceIdentifier(payload.get(REFERENCE_ID))
                        .container(container)
                        .version(1)
                        .userId(metadata.getUserId())
                        .username(metadata.getUsername())
                        .timestamp(metadata.getTimestamp());

        List<EAttribute> attributes;
        List<EReference> references;

        // When the given EClass is EntityType type on that case it is used as it is, otherwise
        // checked it has mapped type and attributes.
        checkMappedObjectStructure(mappedTransferObjectType, container);
        Optional<EClass> mappedEntity = getAsmUtils().getMappedEntityType(mappedTransferObjectType);
        currentStatementBuilder.type(mappedEntity.get());

        Optional<EClass> defaultTransferObjectType = AsmUtils.getExtensionAnnotationValue(mappedEntity.get(), "defaultRepresentation", false)
                .map(defaultTransferObjectTypeName -> getAsmUtils().resolve(defaultTransferObjectTypeName).orElse(null))
                .filter(t -> t instanceof EClass).map(t -> (EClass) t);

        // Processing attributes
        attributes = mappedTransferObjectType.getEAllAttributes().stream()
                .filter(
                        isChangeable
                                .and(a -> getAsmUtils().getMappedAttribute((EAttribute) a).isPresent()))
                .collect(toList());

        // Processing relations
        references = mappedTransferObjectType.getEAllReferences().stream()
                .filter(
                        notParent(container)
                                .and(isChangeable)
                                .and(r -> getAsmUtils().getMappedReference(r).isPresent()))
                .collect(toList());

        Collection<Statement> currentStatements = newArrayList();
        InsertStatement currentStatement = currentStatementBuilder.build();

        currentStatements.add(currentStatement);

        if (checkMandatoryFeatures) {
            checkMandatoryAttributes(attributes, payload);
            checkMandatoryReferences(references, payload);
        }
        checkReferences(references, payload);

        checkForbiddenReferenceUpdates(references, payload);

        // Add attributes (mapped name of attribute resolved here)
        attributes.stream()
                .collect(Collectors.toMap(
                        identity(),
                        a -> getAsmUtils().getMappedAttribute(a).orElse(a)))
                .entrySet().forEach(
                        a -> currentStatement.getInstance().addAttributeValue(
                                a.getValue(),
                                getTransferObjectValueAsEntityValueFromPayload(payload, a.getKey(), a.getValue()))
        );

        // check compositions has no identifier in their payload ()
        references.stream()
                .filter(r -> getAsmUtils().getMappedReference(r).isPresent()
                        && getAsmUtils().getMappedReference(r).get().isContainment()
                        && payload.containsKey(r.getName())
                        && payload.get(r.getName()) != null)
                .collect(toReferencePayloadMapOfPayloadCollection(payload))
                .forEach((key, value) -> value.forEach(p ->
                        checkState(p.get(getIdentifierProvider().getName()) == null, "Identifier cannot be set on new composition reference element: %s Payload: %s", getReferenceFQName(key), p))
                );

        // Get default values of entity type
        final Payload entityDefaults = Payload.empty();

        boolean isDTOPresentAndDifferentThanMappedTO =
                defaultTransferObjectType.isPresent() && !AsmUtils.equals(defaultTransferObjectType.get(), mappedTransferObjectType);
        if (isDTOPresentAndDifferentThanMappedTO) {
            defaultValuesApplier.accept(defaultTransferObjectType.get(), entityDefaults);

            // Add entity default attributes that are not mapped to transfer object type
            Map<EAttribute, EAttribute> dtoAttributeDefaults =
                    defaultTransferObjectType.get().getEAllAttributes().stream()
                                             .filter(a -> entityDefaults.get(a.getName()) != null)
                                             .collect(Collectors.toMap(identity(), a -> getAsmUtils().getMappedAttribute(a).orElse(a)));
            for (Map.Entry<EAttribute, EAttribute> e : dtoAttributeDefaults.entrySet()) {
                EAttribute dtoAttribute = e.getKey();
                EAttribute mappedAttribute = e.getValue();
                if (mappedTransferObjectType.getEAllAttributes().stream().noneMatch(ta -> AsmUtils.equals(mappedAttribute, getAsmUtils().getMappedAttribute(ta).orElse(null)))) {
                    currentStatement.getInstance().addAttributeValue(mappedAttribute, getTransferObjectValueAsEntityValueFromPayload(entityDefaults, dtoAttribute, mappedAttribute));
                }
            }
        }

        // Inserting all embedded reference
        references.stream()
                .filter(hasEmbedded(payload, getIdentifierProvider().getName()))
                .collect(toReferencePayloadMapOfPayloadCollection(payload))
                .entrySet().stream()
                .forEach(entry -> entry.getValue().stream()
                        .forEach(
                                p -> {
                                    Collection<Statement> embeddedStatements = collectStatements(entry.getKey().getEReferenceType(),
                                            p,
                                            entry.getKey(), statements,
                                            checkMandatoryFeatures);

                                    // Collect created embedded InsertStatement to be able to create
                                    // AddReferenceStatement to the container
                                    @SuppressWarnings("rawtypes")
                                    Set containmentReferences = embeddedStatements.stream()
                                            .filter(InsertStatement.class :: isInstance)
                                            .map(o -> (InsertStatement) o)
                                            .filter(i -> i.getContainer().isPresent()
                                                    && i.getContainer().get().equals(entry.getKey()))
                                            .flatMap(i -> addReferenceProcessor.addReference(
                                                                    getAsmUtils().getMappedReference(entry.getKey())
                                                                            .orElseGet(() -> entry.getKey()),
                                                                    ImmutableSet.of(i.getInstance().getIdentifier()),
                                                                    currentStatement.getInstance().getIdentifier(),
                                                                    false
                                                            ).stream().map(AddReferenceStatement.class::cast)
                                            )
                                            .collect(Collectors.toSet());

                                    currentStatements.addAll(containmentReferences);
                                }
                        )
                );

        // Collecting updatable references
        // For all updatable reference have to make existence check
        references.stream()
                .filter(hasReferenced(payload, getIdentifierProvider().getName()))
                .collect(toReferencePayloadMapOfPayloadCollection(payload))
                .entrySet().stream()
                .forEach(entry -> entry.getValue().stream()
                        .forEach(
                                payloadStm -> currentStatements.addAll(
                                        addReferenceProcessor.addReference(
                                            getAsmUtils().getMappedReference(entry.getKey())
                                                        .orElseGet(() -> entry.getKey()),
                                            ImmutableSet.of((Serializable) payloadStm.get(getIdentifierProvider().getName())),
                                            currentStatement.getInstance().getIdentifier(),
                                            true
                                        )
                                )
                        )
                );

        // Add entity default references that are not mapped to transfer object type
        if (isDTOPresentAndDifferentThanMappedTO) {
            Map<EReference, EReference> dtoReferences =
                    defaultTransferObjectType.get().getEAllReferences().stream()
                                             .filter(r -> entityDefaults.get(r.getName()) != null)
                                             .collect(Collectors.toMap(identity(), r -> getAsmUtils().getMappedReference(r).orElse(r)));
            for (Map.Entry<EReference, EReference> e : dtoReferences.entrySet()) {
                EReference dtoReference = e.getKey();
                EReference mappedReference = e.getValue();
                if (mappedTransferObjectType.getEAllReferences().stream().noneMatch(tr -> AsmUtils.equals(mappedReference, getAsmUtils().getMappedReference(tr).orElse(null)))) {
                    Set<Serializable> ids;
                    if (dtoReference.isMany()) {
                        ids = entityDefaults.getAsCollectionPayload(dtoReference.getName()).stream()
                                            .map(p -> p.getAs(getIdentifierProvider().getType(), getIdentifierProvider().getName()))
                                            .collect(Collectors.toSet());
                    } else {
                        ids = Collections.singleton(entityDefaults.getAsPayload(dtoReference.getName())
                                                                  .getAs(getIdentifierProvider().getType(), getIdentifierProvider().getName()));
                    }
                    currentStatements.addAll(addReferenceProcessor.addReference(mappedReference, ids, currentStatement.getInstance().getIdentifier(), true));
                }
            }
        }

        statements.addAll(currentStatements);
        return ImmutableSet.copyOf(currentStatements);
    }
}
