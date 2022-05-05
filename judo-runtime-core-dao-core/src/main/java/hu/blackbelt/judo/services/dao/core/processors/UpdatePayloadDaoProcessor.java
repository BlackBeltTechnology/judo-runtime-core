package hu.blackbelt.judo.services.dao.core.processors;

import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.services.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.services.dao.core.collectors.InstanceGraph;
import hu.blackbelt.judo.services.dao.core.statements.*;
import hu.blackbelt.judo.services.dao.core.values.Metadata;
import hu.blackbelt.judo.services.query.QueryFactory;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.resource.ResourceSet;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newHashSet;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getReferenceFQName;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;


/**
 * Analyzing the entities recursively and generating the required executable statements.
 * The entities have be transfer objects which are mapped to other entities via aliases.
 */
@Slf4j(topic = "dao-core")
public class UpdatePayloadDaoProcessor<ID> extends PayloadDaoProcessor<ID> {

    private static final String ENTITY_TYPE_KEY = "__entityType";

    InsertPayloadDaoProcessor<ID> insertPayloadDaoProcessor;
    DeletePayloadDaoProcessor<ID> deletePayloadDaoProcessor;
    AddReferencePayloadDaoProcessor<ID> addReferencePayloadDaoProcessor;
    RemoveReferencePayloadDaoProcessor<ID> removeReferencePayloadDaoProcessor;

    Metadata<ID> metadata;

    private final boolean optimisticLockEnabled;

    public UpdatePayloadDaoProcessor(ResourceSet resourceSet, IdentifierProvider<ID> identifierProvider,
                                     QueryFactory queryFactory, InstanceCollector instanceCollector,
                                     Function<EClass, Payload> defaultValuesProvider,
                                     Metadata<ID> metadata,
                                     boolean optimisticLockEnabled) {
        super(resourceSet, identifierProvider, queryFactory, instanceCollector);
        this.metadata = metadata;
        this.optimisticLockEnabled = optimisticLockEnabled;
        insertPayloadDaoProcessor = new InsertPayloadDaoProcessor(resourceSet, identifierProvider,
                queryFactory, instanceCollector, defaultValuesProvider, metadata);
        deletePayloadDaoProcessor = new DeletePayloadDaoProcessor(resourceSet, identifierProvider,
                queryFactory, instanceCollector);
        addReferencePayloadDaoProcessor = new AddReferencePayloadDaoProcessor(resourceSet, identifierProvider,
                queryFactory, instanceCollector);
        removeReferencePayloadDaoProcessor = new RemoveReferencePayloadDaoProcessor(resourceSet, identifierProvider,
                queryFactory, instanceCollector);
    }

    public Collection<Statement<ID>> update(EClass type,
                                  Payload originalPayload,
                                  Payload updatedPayload,
                                  boolean checkMandatoryFeatures) {
        Collection<Statement<ID>> allStatements = newHashSet();

        update(type, originalPayload, updatedPayload, null, allStatements, checkMandatoryFeatures);
        return allStatements;
    }

    void update(EClass transferObjectType,
                                         Payload originalPayload,
                                         Payload updatePayload,
                                         EReference container, Collection<Statement<ID>> statements,
                                         boolean checkMandatoryFeatures) {

        checkArgument(transferObjectType != null, "Type is mandatory");
        checkArgument(originalPayload != null, "Original Payload is mandatory");
        checkArgument(updatePayload != null, "Update Payload is mandatory");

        checkArgument(getAsmUtils().isMappedTransferObjectType(transferObjectType), "Type have to be mapped transfer object");

        ID originalIdentifier = (ID) originalPayload.get(getIdentifierProvider().getName());
        ID updatedIdentifier = (ID) updatePayload.get(getIdentifierProvider().getName());

        checkArgument(originalIdentifier.equals(updatedIdentifier), "The original identifier does not match with the original");

        EClass entityType = getAsmUtils().getMappedEntityType(transferObjectType).get();

        InstanceGraph instanceGraph = getInstanceCollector().collectGraph(entityType, updatedIdentifier);

        // The elements have to be processed
        collectStaments(transferObjectType, instanceGraph, originalPayload, updatePayload, statements, null, null, checkMandatoryFeatures);
    }

    void collectStaments(EClass transferObjectType,
                                            InstanceGraph<ID> instanceGraph,
                                            Payload originalPayload,
                                            Payload updatePayload,
                                            Collection<Statement<ID>> statements,
                                            InstanceGraph<ID> containerInstanceGraph,
                                            EReference container,
                                            boolean checkMandatoryFeatures) {
        EClass entityType;
        if (originalPayload.containsKey(ENTITY_TYPE_KEY)) {
            entityType = (EClass) getAsmUtils().resolve(originalPayload.getAs(String.class, ENTITY_TYPE_KEY)).get();
        } else {
            log.warn("Entity type is not found in payload");
            entityType = getAsmUtils().getMappedEntityType(transferObjectType).get();
        }
        ID identifier = (ID) originalPayload.get(getIdentifierProvider().getName());
        checkArgument(identifier != null, "Identifier is mandatory: " + originalPayload.toString());

        final Integer originalVersion = originalPayload.getAs(Integer.class, VERSION);
        if (optimisticLockEnabled) {
            final Integer updateVersion = updatePayload.getAs(Integer.class, VERSION);
            if (updateVersion != null) {
                checkArgument(Objects.equals(updateVersion, originalVersion), "Outdated instance to update");
            }
        }

        UpdateStatement.UpdateStatementBuilder<ID> currentStatementBuilder =
                UpdateStatement.<ID>buildUpdateStatement()
                        .identifier(identifier)
                        .version(optimisticLockEnabled ? originalVersion : null)
                        .userId(metadata.getUserId())
                        .username(metadata.getUsername())
                        .timestamp(metadata.getTimestamp());

        currentStatementBuilder.type(entityType);

        // Processing attributes
        //    - The relation have to be changeable
        //    - And the attribute have to be mapped in entity level
        List<EAttribute> attributes = transferObjectType.getEAllAttributes().stream()
                .filter(
                        isChangeable
                                .and(a -> getAsmUtils().getMappedAttribute((EAttribute) a).isPresent())
                                .and(a -> updatePayload.containsKey(a.getName())))
                .collect(toList());

        // Processing relations
        //    - Only embedded relation are handled, all other are ignored.
        //    - The relation have to be changeable
        //    - And the relation have to be mapped in entity level
        List<EReference> references = transferObjectType.getEAllReferences().stream()
                .filter(
                        notParent(container)
                                .and(isContainment)
                                .and(isChangeable)
                                .and(r -> getAsmUtils().getMappedReference(r).isPresent())
                                .and(r -> updatePayload.containsKey(r.getName())))
                .collect(toList());


        // Remove all attributes/references which is not defined in TransferObjectType.
        Payload updatePayloadCleaned =
                Payload.asPayload(updatePayload.entrySet().stream().filter(e ->
                        e.getKey().equals(getIdentifierProvider().getName()) ||
                                attributes.stream().map(a -> a.getName()).collect(toSet()).contains(e.getKey()) ||
                                references.stream().map(r -> r.getName()).collect(toSet()).contains(e.getKey())

                )
                .collect(HashMap::new, (m, v)->m.put(v.getKey(), v.getValue()), HashMap::putAll));

        UpdateStatement currentStatement = currentStatementBuilder.build();

        // Add attributes (mapped name of attribute resolved here)
        attributes.stream()
                .collect(Collectors.toMap(
                        identity(),
                        a -> getAsmUtils().getMappedAttribute(a).orElse(a)))
                .entrySet().forEach(
                a -> {
                    // TODO: Check the differences more proper way
                    Object original = originalPayload.get(a.getKey().getName());
                    Object update = updatePayloadCleaned.get(a.getKey().getName());;
                    if ((original == null && update != null)
                            || (original != null && update == null)
                            || (original != null && update != null && !original.equals(update))) {
                        currentStatement.getInstance().addAttributeValue(
                                a.getValue(), getTransferObjectValueAsEntityValueFromPayload(updatePayloadCleaned, a.getKey(), a.getValue()));
                    }
                }
        );

        if (currentStatement.getInstance().getAttributes().size() > 0) {
            statements.add(InstanceExistsValidationStatement.<ID>buildInstanceExistsValidationStatement()
                    .type(entityType)
                    .identifier(identifier)
                    .build());
            statements.add(currentStatement);
        }

        references.stream().forEach(currentReference -> {
                if (currentReference.getUpperBound() == 1) {
                            mergePayloads(currentReference,
                                    instanceGraph,
                                    originalPayload.getAsPayload(currentReference.getName()),
                                    updatePayloadCleaned.getAsPayload(currentReference.getName()),
                                    statements,
                                    checkMandatoryFeatures);
                } else {
                    Collection<Payload> referenceOriginalPayloads = originalPayload.getAsCollectionPayload(currentReference.getName());
                    Collection<Payload> referenceUpdatePayloads = updatePayloadCleaned.getAsCollectionPayload(currentReference.getName());

                    if (referenceOriginalPayloads != null && referenceUpdatePayloads != null) {

                        // The reference's update payload(s) which have no ID
                        Collection<Payload> referenceUpdatePayloadsWithoutId =
                                referenceUpdatePayloads.stream().filter(p -> !p.containsKey(getIdentifierProvider().getName())).collect(toSet());

                        // The reference's update payload(s) which have ID
                        Collection<Payload> referenceUpdatePayloadsWithId =
                                referenceUpdatePayloads.stream().filter(p -> p.containsKey(getIdentifierProvider().getName())).collect(toSet());

                        Collection<Payload> referenceOriginalPayloadsDoesNotExists =
                                referenceOriginalPayloads.stream()
                                        .filter(p -> !referenceUpdatePayloads.stream()
                                                .anyMatch(p2 -> p.get(getIdentifierProvider().getName()).equals(p2.get(getIdentifierProvider().getName()))))
                                        .collect(toSet());

                        Collection<Payload> referenceUpdatePayloadsDoesNotExists =
                                referenceUpdatePayloadsWithId.stream()
                                        .filter(p -> !referenceOriginalPayloads.stream()
                                                .anyMatch(p2 -> p2.get(getIdentifierProvider().getName()).equals(p.get(getIdentifierProvider().getName()))))
                                        .collect(toSet());


                        Map<Payload, Payload> referenceOriginalPayloadsWithUpdate =
                                referenceOriginalPayloads.stream()
                                        .filter(p -> referenceUpdatePayloads.stream()
                                                .anyMatch(p2 -> p.get(getIdentifierProvider().getName()).equals(p2.get(getIdentifierProvider().getName()))))
                                        .collect(
                                                toMap(identity(),
                                                        p -> referenceUpdatePayloads.stream()
                                                                .filter(p2 -> p.get(getIdentifierProvider().getName())
                                                                        .equals(p2.get(getIdentifierProvider().getName())))
                                                                .findFirst().get()));

                        referenceOriginalPayloadsDoesNotExists.stream().forEach(originalReferencePayload -> {
                                    mergePayloads(currentReference,
                                            instanceGraph,
                                            originalReferencePayload,
                                            null,
                                            statements,
                                            checkMandatoryFeatures);
                        });

                        referenceUpdatePayloadsDoesNotExists.stream().forEach(updateReferencePayload -> {
                                    mergePayloads(currentReference,
                                            instanceGraph,
                                            null,
                                            updateReferencePayload,
                                            statements,
                                            checkMandatoryFeatures);
                        });

                        referenceUpdatePayloadsWithoutId.stream().forEach(updateReferencePayload -> {
                                    mergePayloads(currentReference,
                                            instanceGraph,
                                            null,
                                            updateReferencePayload,
                                            statements,
                                            checkMandatoryFeatures);
                        });

                        referenceOriginalPayloadsWithUpdate.entrySet().stream().forEach(referenceOriginalAndUpdate -> {
                                    mergePayloads(currentReference,
                                            instanceGraph,
                                            referenceOriginalAndUpdate.getKey(),
                                            referenceOriginalAndUpdate.getValue(),
                                            statements,
                                            checkMandatoryFeatures);
                        });

                    }
                }
        });
    }


    private void mergePayloads(final EReference mappedReference,
                                                    final InstanceGraph<ID> parentInstanceGraph,
                                                    final Payload originalPayload,
                                                    final Payload updatePayload,
                                                    final Collection<Statement<ID>> statements,
                                                    final boolean checkMandatoryFeatures) {

        checkArgument(getAsmUtils().getMappedReference(mappedReference).isPresent(),
                "Reference have to be mapped to an entity reference");

        EReference entityReference = getAsmUtils().getMappedReference(mappedReference).get();

        boolean isContainment = entityReference.isContainment();
        InstanceGraph<ID> instanceGraph = null;

        final ID originalIdentifier;
        if (originalPayload != null) {
            originalIdentifier = (ID) originalPayload.get(getIdentifierProvider().getName());
        } else {
            originalIdentifier = null;
        }

        final ID updateIdentifier;
        if (updatePayload != null) {
            updateIdentifier = (ID) updatePayload.get(getIdentifierProvider().getName());
        } else {
            updateIdentifier = null;
        }

        // Lookup the existing instancegraph related to original payload
        if (originalIdentifier != null) {
            if (isContainment) {
                instanceGraph = parentInstanceGraph.getContainments().stream()
                        .filter(ir -> ir.getReference().equals(entityReference) &&
                                ir.getReferencedElement().getId().equals(originalIdentifier))
                        .map(ir -> ir.getReferencedElement()).findFirst()
                        .orElseThrow(() ->
                                new IllegalStateException("Identifier are not presented in InstanceGraph: " +
                                        originalIdentifier.toString()));
            }
        }

        if (updatePayload != null) {
            if (updateIdentifier == null && originalIdentifier == null) {
                checkState(isContainment, "Identifier is mandatory on reference association: " +
                        getReferenceFQName(mappedReference) + " Payload: " + updatePayload);

                // INSERT NEW INSTANCE and mandatory reference add (filter out check existence)
                Collection<Statement<ID>> insertStatementsWithoutCheckExistence
                        = insertPayloadDaoProcessor.insert(mappedReference.getEReferenceType(), updatePayload, checkMandatoryFeatures).stream()
                        .filter(s -> !(s instanceof InstanceExistsValidationStatement)).collect(toSet());

                EReference insertedEntityReference = getAsmUtils().getMappedReference(mappedReference).get();

                InsertStatement<ID> insertStatement = insertStatementsWithoutCheckExistence.stream()
                        .filter(InsertStatement.class::isInstance)
                        .map(InsertStatement.class::cast)
                        .filter(i -> i.getInstance().getType().equals(insertedEntityReference.getEReferenceType()))
                        .findFirst().get();

                statements.addAll(insertStatementsWithoutCheckExistence);
                statements.addAll(
                        addReferencePayloadDaoProcessor.addReference(
                                entityReference,
                                ImmutableSet.of(insertStatement.getInstance().getIdentifier()),
                                parentInstanceGraph.getId(), false
                        ));
            } else if (updateIdentifier != null && originalIdentifier == null) {
                checkState(!isContainment, "Identifier cannot be set on new association reference element: " +
                        getReferenceFQName(mappedReference) + " Payload: " + updatePayload);
                // ADD NEW REFERENCE
                statements.addAll(
                        addReferencePayloadDaoProcessor.addReference(
                                entityReference,
                                ImmutableSet.of(updateIdentifier),
                                parentInstanceGraph.getId(), true
                        ));
            } else if (updateIdentifier == null && originalIdentifier != null) {
                if (isContainment) {
                    // DELETE ORIGINAL INSTANCE
                    deletePayloadDaoProcessor.collectStaments(entityReference.getEReferenceType(),
                            instanceGraph, statements, parentInstanceGraph, entityReference);

                    // INSERT When there is any attributes in payload
                    if (updatePayload != null && updatePayload.entrySet().size() > 0) {

                        // INSERT NEW INSTANCE and mandatory reference add (filter out check existence)
                        Collection<Statement<ID>> insertStatementsWithoutCheckExistence
                                = insertPayloadDaoProcessor.insert(mappedReference.getEReferenceType(), updatePayload, checkMandatoryFeatures).stream()
                                .filter(s -> !(s instanceof InstanceExistsValidationStatement)).collect(toSet());

                        EReference insertedEntityReference = getAsmUtils().getMappedReference(mappedReference).get();

                        InsertStatement<ID> insertStatement = insertStatementsWithoutCheckExistence.stream()
                                .filter(InsertStatement.class::isInstance)
                                .map(InsertStatement.class::cast)
                                .filter(i -> i.getInstance().getType().equals(insertedEntityReference.getEReferenceType()))
                                .findFirst().get();

                        statements.add(insertStatement);
                        statements.addAll(
                                addReferencePayloadDaoProcessor.addReference(
                                        entityReference,
                                        ImmutableSet.of(insertStatement.getInstance().getIdentifier()),
                                        parentInstanceGraph.getId(), false
                                ));
                    }
                } else {
                    // DELETE ORIGINAL REFERENCE
                    checkState(updatePayload.entrySet().size() == 0,
                            "On association there can be any attribute / reference inside the payload: " +
                            getReferenceFQName(mappedReference) + " Payload: " + updatePayload);

                    statements.addAll(
                            removeReferencePayloadDaoProcessor.removeReference(
                                    entityReference,
                                    ImmutableSet.of(originalIdentifier),
                                    parentInstanceGraph.getId(),
                                    false
                            ));
                }
            } else if (updateIdentifier != null && originalIdentifier != null) {
                if (originalIdentifier.equals(updateIdentifier)) {
                    if (isContainment) {
                        // UPDATE embedded instances INSTANCE
                        collectStaments(mappedReference.getEReferenceType(), instanceGraph, originalPayload,
                                updatePayload, statements, parentInstanceGraph, mappedReference, checkMandatoryFeatures);
                    }
                } else {
                    checkState(!isContainment, "Identifier cannot be different on containment reference element: " +
                            getReferenceFQName(mappedReference) + " Payload: " + updatePayload);

                    // DELETE ORIGINAL REFERENCE
                    statements.addAll(
                            removeReferencePayloadDaoProcessor.removeReference(
                                    entityReference,
                                    ImmutableSet.of(originalIdentifier),
                                    parentInstanceGraph.getId(),
                                    false
                            ));

                    // ADD NEW REFERENCE
                    statements.addAll(
                            addReferencePayloadDaoProcessor.addReference(
                                    entityReference,
                                    ImmutableSet.of(updateIdentifier),
                                    parentInstanceGraph.getId(), true
                            ));
                }
            }
        } else if (originalIdentifier != null) {
            if (isContainment) {
                // DELETE ORIGINAL INSTANCE
                deletePayloadDaoProcessor.collectStaments(entityReference.getEReferenceType(),
                        instanceGraph, statements, parentInstanceGraph, entityReference);
            } else {
                // DELETE ORIGINAL REFERENCE
                statements.addAll(
                        removeReferencePayloadDaoProcessor.removeReference(
                                entityReference,
                                ImmutableSet.of(originalIdentifier),
                                parentInstanceGraph.getId(),
                                false
                        ));
            }
        }
    }
}
