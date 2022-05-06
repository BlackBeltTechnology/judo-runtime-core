package hu.blackbelt.judo.runtime.core.dao.core.processors;

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
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.resource.ResourceSet;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;


/**
 * Analyzing the inserted entities recursively and generating the required executable statements.
 * The entities have be transfer objects which are mapped to other entities via aliases.
 *
 * Rules:
 *    - The root type cannot have ID - because its is update
 *    - Any relations which are NULL ignored
 *    - Any entities which have ID attached relations are ignored.
 *    - Any entities which hae no ID are inserted recursively - does not matter it is containment or association
 */
@Slf4j(topic = "dao-core")
public class InsertPayloadDaoProcessor<ID> extends PayloadDaoProcessor<ID> {

    private final AddReferencePayloadDaoProcessor<ID> addReferenceProcessor;

    private final Function<EClass, Payload> defaultValuesProvider;

    Metadata<ID> metadata;

    public InsertPayloadDaoProcessor(ResourceSet resourceSet, IdentifierProvider<ID> identifierProvider,
                                     QueryFactory queryFactory, InstanceCollector instanceCollector,
                                     Function<EClass, Payload> defaultValuesProvider,
                                     Metadata<ID> metadata) {
        super(resourceSet, identifierProvider, queryFactory, instanceCollector);
        addReferenceProcessor =
                new AddReferencePayloadDaoProcessor<ID>(resourceSet, identifierProvider, queryFactory, instanceCollector);
        this.defaultValuesProvider = defaultValuesProvider;
        this.metadata = metadata;
    }

    public Collection<Statement<ID>> insert(EClass type,
                                  Payload payload,
                                  boolean checkMandatoryFeatures) {
        Collection<Statement<ID>> statements = newHashSet();
        collectStatements(type, payload, null, statements, checkMandatoryFeatures);
        return statements;
    }

    Collection<Statement<ID>> collectStatements(EClass mappedTransferObjectType,
                                            Payload payload,
                                            EReference container, Collection<Statement<ID>> statements,
                                            boolean checkMandatoryFeatures) {

        checkArgument(mappedTransferObjectType != null, "Type is mandatory");
        checkArgument(payload != null, "Payload is mandatory");

        checkArgument(getAsmUtils().isMappedTransferObjectType(mappedTransferObjectType), "Type have to be mapped transfer object");

        // Set default values of transfer object type (that are missing from payload)
        Payload defaults = defaultValuesProvider.apply(mappedTransferObjectType);
        payload.putAll(defaults.entrySet().stream()
                .filter(e -> !payload.containsKey(e.getKey()) && e.getValue() != null)
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));

        InsertStatement.InsertStatementBuilder<ID> currentStatementBuilder =
                InsertStatement.<ID>buildInsertStatement()
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

        // Get default values of entity type
        final Payload entityDefaults = defaultTransferObjectType.map(t -> defaultValuesProvider.apply(t)).orElse(Payload.empty());

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

        Collection<Statement<ID>> currentStatements = newArrayList();
        InsertStatement<ID> currentStatement = currentStatementBuilder.build();

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

        // Add entity default attributes that are not mapped to transfer object type
        defaultTransferObjectType.ifPresent(t -> t.getEAllAttributes().stream()
                .filter(a -> entityDefaults.get(a.getName()) != null)
                .collect(Collectors.toMap(
                        identity(),
                        a -> getAsmUtils().getMappedAttribute(a).orElse(a)))
                .entrySet().stream()
                .filter(e -> !mappedTransferObjectType.getEAllAttributes().stream().anyMatch(ta -> AsmUtils.equals(e.getValue(), getAsmUtils().getMappedAttribute(ta).orElse(null))))
                .forEach(
                        a -> currentStatement.getInstance().addAttributeValue(
                                a.getValue(),
                                getTransferObjectValueAsEntityValueFromPayload(entityDefaults, a.getKey(), a.getValue()))
                )
        );

        // Inserting all embedded reference
        references.stream()
                .filter(hasEmbedded(payload, getIdentifierProvider().getName()))
                .collect(toReferencePayloadMapOfPayloadCollection(payload))
                .entrySet().stream()
                .forEach(entry -> entry.getValue().stream()
                        .forEach(
                                p -> {
                                    Collection<Statement<ID>> embeddedStatements = collectStatements(entry.getKey().getEReferenceType(),
                                            p,
                                            entry.getKey(), statements,
                                            checkMandatoryFeatures);

                                    // Collect created embedded InsertStatement to be able to create
                                    // AddReferenceStatement to the container
                                    Set containmentReferences = embeddedStatements.stream()
                                            .filter(InsertStatement.class :: isInstance)
                                            .map(o -> (InsertStatement) o)
                                            .filter(i -> i.getContainer().isPresent()
                                                    && i.getContainer().get().equals(entry.getKey()))
                                            .flatMap(i -> addReferenceProcessor.addReference(
                                                                    getAsmUtils().getMappedReference(entry.getKey())
                                                                            .orElseGet(() -> entry.getKey()),
                                                                    ImmutableSet.of((ID) i.getInstance().getIdentifier()),
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
                                            ImmutableSet.of((ID) payloadStm.get(getIdentifierProvider().getName())),
                                            currentStatement.getInstance().getIdentifier(),
                                            true
                                        )
                                )
                        )
                );

        // Add entity default references that are not mapped to transfer object type
        defaultTransferObjectType.ifPresent(t -> t.getEAllReferences().stream()
                .filter(r -> entityDefaults.get(r.getName()) != null)
                .collect(Collectors.toMap(
                        identity(),
                        r -> getAsmUtils().getMappedReference(r).orElse(r)))
                .entrySet().stream()
                .filter(e -> !mappedTransferObjectType.getEAllReferences().stream().anyMatch(tr -> AsmUtils.equals(e.getValue(), getAsmUtils().getMappedReference(tr).orElse(null))))
                .forEach(
                        r -> currentStatements.addAll(
                                addReferenceProcessor.addReference(
                                        r.getValue(),
                                        r.getKey().isMany()
                                                ? entityDefaults.getAsCollectionPayload(r.getKey().getName()).stream().map(p -> p.getAs(getIdentifierProvider().getType(), getIdentifierProvider().getName())).collect(Collectors.toSet())
                                                : Collections.singleton(entityDefaults.getAsPayload(r.getKey().getName()).getAs(getIdentifierProvider().getType(), getIdentifierProvider().getName())),
                                        currentStatement.getInstance().getIdentifier(),
                                        true
                                )
                        )
                )
        );

        statements.addAll(currentStatements);
        return ImmutableSet.copyOf(currentStatements);
    }
}
