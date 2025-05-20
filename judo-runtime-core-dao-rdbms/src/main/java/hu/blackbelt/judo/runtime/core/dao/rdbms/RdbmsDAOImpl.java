package hu.blackbelt.judo.runtime.core.dao.rdbms;

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

import java.io.Serializable;
import java.security.Principal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.core.processors.AddReferencePayloadDaoProcessor;
import hu.blackbelt.judo.runtime.core.dao.core.processors.DeletePayloadDaoProcessor;
import hu.blackbelt.judo.runtime.core.dao.core.processors.InsertPayloadDaoProcessor;
import hu.blackbelt.judo.runtime.core.dao.core.processors.PayloadDaoProcessor;
import hu.blackbelt.judo.runtime.core.dao.core.processors.RemoveReferencePayloadDaoProcessor;
import hu.blackbelt.judo.runtime.core.dao.core.processors.UpdatePayloadDaoProcessor;
import hu.blackbelt.judo.runtime.core.dao.core.statements.InsertStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.Statement;
import hu.blackbelt.judo.runtime.core.dao.core.values.Metadata;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.ModifyStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.SelectStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.UniqueEList;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getReferenceFQName;
import static hu.blackbelt.judo.runtime.core.dao.rdbms.PayloadTraverser.traversePayload;
import static java.util.Objects.requireNonNullElse;
import static java.util.function.Function.identity;

/**
 * It contains all plumbing logic for {@link AbstractRdbmsDAO}.
 * It is not created automatically, other DataSource and Model dependent mechanism have to
 * manage the lifecycle, or constructor based creation is supported.
 */
@Slf4j
@SuppressWarnings({"rawtypes", "unchecked"})
public class RdbmsDAOImpl extends AbstractRdbmsDAO implements DAO {

    private static final String STATEFUL = "STATEFUL";
    private static final String ROLLBACK = "ROLLBACK";
    public static final String CREATED = "__$created";
    public static final String DEFAULT_VALUES_LOADED_KEY = "__defaultValuesLoaded";

    @Getter private final AsmModel asmModel;
    private final DataSource dataSource;
    @Getter private final IdentifierProvider identifierProvider;
    private final InstanceCollector instanceCollector;
    private final QueryFactory queryFactory;
    private final boolean optimisticLockEnabled;
    private final Context context;
    private final MetricsCollector metricsCollector;
    private final SelectStatementExecutor selectStatementExecutor;
    private final ModifyStatementExecutor modifyStatementExecutor;
    private final Map<EClass, Boolean> hasDefaultsMap = new ConcurrentHashMap<>();

    @Builder
    private RdbmsDAOImpl(
            @NonNull AsmModel asmModel,
            @NonNull DataSource dataSource,
            @NonNull IdentifierProvider identifierProvider,
            @NonNull Context context,
            @NonNull MetricsCollector metricsCollector,
            @NonNull InstanceCollector instanceCollector,
            @NonNull ModifyStatementExecutor modifyStatementExecutor,
            @NonNull SelectStatementExecutor selectStatementExecutor,
            @NonNull QueryFactory queryFactory,
            Boolean optimisticLockEnabled) {
        this.asmModel = asmModel;
        this.dataSource = dataSource;
        this.identifierProvider = identifierProvider;
        this.instanceCollector = instanceCollector;
        this.queryFactory = queryFactory;

        this.optimisticLockEnabled = requireNonNullElse(optimisticLockEnabled, true);

        this.context = context;
        this.metricsCollector = metricsCollector;

        this.selectStatementExecutor = selectStatementExecutor;
        this.modifyStatementExecutor = modifyStatementExecutor;
    }

    private BiConsumer<EClass, Payload> getDefaultValuesApplier() {
        return (clazz, payload) -> {
            if (hasDefaults(clazz)) {
                applyDefaultsOf(clazz, payload);
            }
        };
    }

    private boolean hasDefaults(EClass clazz) {
        if (hasDefaultsMap.containsKey(clazz)) {
            return hasDefaultsMap.get(clazz);
        } else {
            final boolean hasDefaults = haveDefaultsAnyOf(Collections.singleton(clazz), new UniqueEList<>());
            hasDefaultsMap.put(clazz, hasDefaults);
            return hasDefaults;
        }
    }

    private boolean haveDefaultsAnyOf(final Collection<EClass> classes, final Collection<EClass> checked) {
        AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());
        if (classes.isEmpty()) {
            return false;
        } else if (classes.stream()
                .flatMap(c -> c.getEAllStructuralFeatures().stream())
                .anyMatch(c -> AsmUtils.getExtensionAnnotationByName(c, "default", false).isPresent())) {
            return true;
        } else if (
             classes.stream()
                    .map(eClass -> asmUtils.getMappedEntityType(eClass))
                    .map(eClass ->
                            eClass.flatMap(e -> AsmUtils.getExtensionAnnotationValue(e, "defaultRepresentation", false)
                                    .flatMap(dr -> asmUtils.resolve(dr)))
                            .filter(t -> t instanceof EClass).map(t -> (EClass) t))
                    .filter(eClass -> eClass.isPresent())
                    .flatMap(eClass -> eClass.get().getEAllStructuralFeatures().stream())
                    .anyMatch(c -> AsmUtils.getExtensionAnnotationByName(c, "default", false).isPresent())) {
            return true;
        }
        checked.addAll(classes);
        return haveDefaultsAnyOf(classes.stream()
                .flatMap(c -> c.getEAllReferences().stream()
                        .filter(r -> AsmUtils.isEmbedded(r) && !checked.contains(r.getEReferenceType()))
                        .map(EReference::getEReferenceType))
                .collect(Collectors.toList()), checked);
    }

    protected InsertPayloadDaoProcessor getInsertPayloadProcessor(Metadata metadata) {
        return new InsertPayloadDaoProcessor(asmModel.getResourceSet(),
                                                 getIdentifierProvider(),
                                                 queryFactory,
                                                 instanceCollector,
                                                 getDefaultValuesApplier(),
                                                 metadata);
    }

    protected DeletePayloadDaoProcessor getDeletePayloadProcessor() {
        return new DeletePayloadDaoProcessor(asmModel.getResourceSet(),
                getIdentifierProvider(),
                queryFactory,
                instanceCollector);
    }

    protected UpdatePayloadDaoProcessor getUpdatePayloadProcessor(Metadata metadata) {
        return new UpdatePayloadDaoProcessor(asmModel.getResourceSet(),
                                             getIdentifierProvider(),
                                             queryFactory,
                                             instanceCollector,
                                             getDefaultValuesApplier(),
                                             metadata,
                                             optimisticLockEnabled);
    }

    protected AddReferencePayloadDaoProcessor getAddReferencePayloadProcessor() {
        return new AddReferencePayloadDaoProcessor(asmModel.getResourceSet(),
                getIdentifierProvider(),
                queryFactory,
                instanceCollector);
    }

    protected RemoveReferencePayloadDaoProcessor getRemoveReferencePayloadProcessor() {
        return new RemoveReferencePayloadDaoProcessor(asmModel.getResourceSet(),
                getIdentifierProvider(),
                queryFactory,
                instanceCollector);
    }

    @Override
    public Payload readStaticFeatures(EClass clazz) {
        final Payload result = Payload.empty();
        AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());

        checkArgument(!asmUtils.isMappedTransferObjectType(clazz) && AsmUtils.annotatedAsTrue(clazz, "transferObjectType"), "Clazz must be an unmapped transfer object type");

        clazz.getEAllAttributes().stream().filter(EStructuralFeature::isDerived).forEach(attribute -> {
            if (log.isDebugEnabled()) {
                log.debug("Loading attribute: {}", AsmUtils.getAttributeFQName(attribute));
            }
            final Payload staticData = readStaticData(attribute, null);
            if (staticData.containsKey(attribute.getName())) {
                result.put(attribute.getName(), staticData.get(attribute.getName()));
            }
        });

        clazz.getEAllReferences().stream().filter(r -> r.isDerived() && AsmUtils.annotatedAsTrue(r, "embedded")).forEach(reference -> {
            if (log.isDebugEnabled()) {
                log.debug("Loading reference: {}", AsmUtils.getReferenceFQName(reference));
            }
            final Collection<Payload> nested = readAllReferences(reference, null);
            if (reference.isMany()) {
                result.put(reference.getName(), nested);
            } else {
                result.put(reference.getName(), nested.isEmpty() ? null : nested.iterator().next());
            }
        });

        return result;
    }

    @Override
    protected Payload readStaticData(EAttribute attribute, Map<String, Object> parameters) {
        return selectStatementExecutor.executeSelect(
                new NamedParameterJdbcTemplate(dataSource), attribute, parameters);
    }

    @Override
    protected List<Payload> readAll(EClass clazz) {
        return selectStatementExecutor.executeSelect(
                        new NamedParameterJdbcTemplate(dataSource), clazz, null, null)
                .stream().map(Payload::asPayload).collect(Collectors.toList());
    }

    @Override
    protected long countAll(EClass clazz) {
        return selectStatementExecutor.countSelect(
                        new NamedParameterJdbcTemplate(dataSource), clazz, null, null);
    }

    @Override
    protected List<Payload> searchByFilter(EClass clazz, QueryCustomizer queryCustomizer) {
        return selectStatementExecutor.executeSelect(
                        new NamedParameterJdbcTemplate(dataSource), clazz, null, queryCustomizer)
                .stream().map(Payload::asPayload).collect(Collectors.toList());
    }

    @Override
    protected long countByFilter(EClass clazz, QueryCustomizer queryCustomizer) {
        return selectStatementExecutor.countSelect(
                        new NamedParameterJdbcTemplate(dataSource), clazz, null, queryCustomizer);
    }

    @Override
    protected List<Payload> readAllReferences(EReference reference, Collection<Serializable> navigationSourceIdentifiers) {
        return selectStatementExecutor.executeSelect(
                        new NamedParameterJdbcTemplate(dataSource), reference, navigationSourceIdentifiers != null ? new HashSet<>(navigationSourceIdentifiers) : null, null)
                .stream().map(Payload::asPayload).collect(Collectors.toList());
    }

    @Override
    protected long countAllReferences(EReference reference, Collection<Serializable> navigationSourceIdentifiers) {
        return selectStatementExecutor.countSelect(
                        new NamedParameterJdbcTemplate(dataSource), reference, navigationSourceIdentifiers != null ? new HashSet<>(navigationSourceIdentifiers) : null, null);
    }

    @Override
    protected List<Payload> readByIdentifiers(EClass clazz, Collection<Serializable> identifiers, QueryCustomizer queryCustomizer) {
        return selectStatementExecutor.executeSelect(
                        new NamedParameterJdbcTemplate(dataSource), clazz, new HashSet<>(identifiers), queryCustomizer)
                .stream().map(Payload::asPayload).collect(Collectors.toList());
    }

    @Override
    protected Optional<Payload> readByIdentifier(EClass clazz, Serializable identifier, QueryCustomizer queryCustomizer) {
        return readByIdentifiers(clazz, ImmutableSet.of(identifier), queryCustomizer).stream()
                .map(Payload::asPayload)
                .filter(p -> identifier.equals(p.get(identifierProvider.getName())))
                .findFirst();
    }

    @Override
    protected List<Payload> searchReferences(EReference reference, Collection<Serializable> navigationSourceIdentifiers, QueryCustomizer queryCustomizer) {
        return selectStatementExecutor.executeSelect(
                        new NamedParameterJdbcTemplate(dataSource), reference, navigationSourceIdentifiers != null ? new HashSet<>(navigationSourceIdentifiers) : null, queryCustomizer)
                .stream().map(Payload::asPayload).collect(Collectors.toList());
    }

    @Override
    protected long countReferences(EReference reference, Collection<Serializable> navigationSourceIdentifiers, QueryCustomizer queryCustomizer) {
        return selectStatementExecutor.countSelect(
                        new NamedParameterJdbcTemplate(dataSource), reference, navigationSourceIdentifiers != null ? new HashSet<>(navigationSourceIdentifiers) : null, queryCustomizer);
    }

    @Override
    protected Payload insertPayload(EClass clazz, Payload payload, QueryCustomizer queryCustomizer, boolean checkMandatoryFeatures) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "INSERT is not supported in stateless operation");

        final Payload actor = context.getAs(Payload.class, Dispatcher.ACTOR_KEY);
        final Principal principal = context.getAs(Principal.class, Dispatcher.PRINCIPAL_KEY);
        final Metadata metadata = Metadata.<Serializable>buildMetadata()
                .timestamp(LocalDateTime.now())
                .userId(actor != null ? actor.getAs(identifierProvider.getType(), identifierProvider.getName()) : null)
                .username(principal != null ? principal.getName() : null)
                .build();
        Collection<Statement> statements = getInsertPayloadProcessor(metadata)
                .insert(clazz, payload, checkMandatoryFeatures);


        modifyStatementExecutor.executeStatements(new NamedParameterJdbcTemplate(dataSource), statements);

        // Get the root entity's
        Serializable identifier = (Serializable) statements.stream()
                .filter(InsertStatement.class::isInstance)
                .map(InsertStatement.class::cast)
                .filter(i -> i.getContainer().isEmpty())
                .findFirst().orElseThrow(() -> new IllegalStateException("Insert statement could not found")).getInstance().getIdentifier();

        final Optional<Payload> result = readByIdentifier(clazz, identifier, queryCustomizer);
        checkArgument(result.isPresent(), "Creation of " + AsmUtils.getClassifierFQName(clazz) + " failed");

        // Collect clientReferenceId recursively and map back to response
        Map<Serializable, Object> clientReferenceMap = new HashMap<>();
        traversePayload(
                getCollectPayloadClientReferenceConsumer(clientReferenceMap),
                PayloadTraverser.builder()
                        .transferObjectType(clazz)
                        .payload(payload)
                        .asmModel(asmModel)
                        .build());

        collectInsertStatementsClientReferenceId(clientReferenceMap, statements);
        Payload ret = result.get();

        Set<Serializable> insertedIds = statements.stream()
                .filter(st -> st instanceof InsertStatement)
                .map(st -> (Serializable) ((InsertStatement) st).getInstance().getIdentifier())
                .collect(Collectors.toSet());

        traversePayload(
                getApplyClientReferenceIdConsumer(clientReferenceMap)
                        .andThen(getMarkInsertedPayloadsConsumer(insertedIds)),
                PayloadTraverser.builder()
                        .transferObjectType(clazz)
                        .payload(ret)
                        .asmModel(asmModel)
                        .build());
        return ret;
    }

    @Override
    protected Payload insertPayloadAndAttach(EReference reference, Serializable identifier, Payload payload, QueryCustomizer queryCustomizer) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "INSERT is not supported in stateless operation");
        AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());

        EClass typeOfNewInstance = reference.getEReferenceType();
        EReference mappedReference = asmUtils.getMappedReference(reference)
                .orElseThrow(() -> new IllegalArgumentException("Mapping of transfer object relation not found: " + AsmUtils.getReferenceFQName(reference)));
        Payload result;

        if (mappedReference.isContainment()) {
            Payload container = getByIdentifier(reference.getEContainingClass(), identifier)
                    .orElseThrow(() -> new IllegalArgumentException("Container not found: " + AsmUtils.getReferenceFQName(reference)));
            if (reference.isMany()) {
                Collection<Payload> containments = container.getAsCollectionPayload(reference.getName());
                if (reference.getUpperBound() == -1 || containments == null || containments.size() < reference.getUpperBound()) {
                    Payload referenced = create(typeOfNewInstance, payload, queryCustomizer);
                    Serializable referencedId = referenced.getAs(identifierProvider.getType(), identifierProvider.getName());
                    addReferencesOfInstance(reference, identifier, Collections.singleton(referencedId));
                    result = referenced;
                } else {
                    throw new IllegalArgumentException("Upper cardinality violated");
                }
            } else {
                if ((container.get(reference.getName()) != null) ||  (!reference.isContainment() && !getNavigationResultAt(identifier, reference).isEmpty())) {
                    throw new IllegalArgumentException("Containment already set");
                }
                Payload referenced = create(typeOfNewInstance, payload, queryCustomizer);
                Serializable referencedId = referenced.getAs(identifierProvider.getType(), identifierProvider.getName());
                addReferencesOfInstance(reference, identifier, Collections.singleton(referencedId));
                result = referenced;
            }
        } else {
            // reference is not containment
            final EReference mappedBackReference = mappedReference.getEOpposite();
            final List<EReference> backReferences = typeOfNewInstance.getEAllReferences().stream()
                    .filter(br -> asmUtils.getMappedReference(br)
                            .filter(mbr -> Objects.equals(mbr, mappedBackReference))
                            .isPresent())
                    .collect(Collectors.toList());

            backReferences.stream()
                    .filter(br -> payload.containsKey(br.getName()))
                    .forEach(backReference -> {
                        if (backReference.isMany()) {
                            checkArgument(payload.get(backReference.getName()) != null, "Collection reference must not be null");
                            checkArgument(payload.getAsCollectionPayload(backReference.getName()).stream()
                                            .anyMatch(p -> Objects.equals(p.getAs(identifierProvider.getType(), identifierProvider.getName()), identifier)),
                                    "Back reference " + AsmUtils.getReferenceFQName(backReference) + " does not contain identifier: " + identifier);
                        } else {
                            checkArgument(payload.get(backReference.getName()) != null, "Back reference must not be null");
                            checkArgument(Objects.equals(payload.getAsPayload(backReference.getName()).getAs(identifierProvider.getType(), identifierProvider.getName()), identifier),
                                    "Back reference " + AsmUtils.getReferenceFQName(backReference) + " conflicts identifier: " + identifier);
                        }
                    });

            final Payload backReferencePayload = Payload.map(getIdentifierProvider().getName(), identifier);
            backReferences.stream()
                    .filter(br -> !payload.containsKey(br.getName()) && br.isRequired())
                    .forEach(backReference -> {
                        if (backReference.isMany()) {
                            payload.put(backReference.getName(), Collections.singleton(backReferencePayload));
                        } else {
                            payload.put(backReference.getName(), backReferencePayload);
                        }
                    });

            // opposite is not defined/sent so DAO create and set reference operations must be used to persist new instance
            final Payload referenced = create(typeOfNewInstance, payload, QueryCustomizer.<Serializable>builder()
                    .mask(Collections.emptyMap())
                    .build());
            final Serializable referencedId = referenced.getAs(identifierProvider.getType(), identifierProvider.getName());
            // do not add reference if relation is derived
            if (!mappedReference.isDerived()) {
                if (reference.isMany()) {
                    addReferencesOfInstance(reference, identifier, Collections.singleton(referencedId));
                } else {
                    setReferenceOfInstance(reference, identifier, Collections.singleton(referencedId));
                }
            }

            result = searchNavigationResultAt(identifier, reference, queryCustomizer).stream()
                    .filter(newInstance -> Objects.equals(newInstance.getAs(identifierProvider.getType(), identifierProvider.getName()), referencedId))
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("Unable to create and attached instance"));
        }
        return result;
    }

    @Override
    protected void deletePayload(EClass clazz, Collection<Serializable> ids) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "DELETE is not supported in stateless operation");

        Collection<Statement> statements = getDeletePayloadProcessor()
                .delete(clazz, ids);

        modifyStatementExecutor.executeStatements(new NamedParameterJdbcTemplate(dataSource), statements);
    }

    @Override
    protected Payload updatePayload(EClass clazz, Payload original, Payload updated, QueryCustomizer queryCustomizer, boolean checkMandatoryFeatures) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "UPDATE is not supported in stateless operation");

        final Payload actor = context.getAs(Payload.class, Dispatcher.ACTOR_KEY);
        final Principal principal = context.getAs(Principal.class, Dispatcher.PRINCIPAL_KEY);
        final Metadata metadata = Metadata.<Serializable>buildMetadata()
                .timestamp(LocalDateTime.now())
                .userId(actor != null ? actor.getAs(identifierProvider.getType(), identifierProvider.getName()) : null)
                .username(principal != null ? principal.getName() : null)
                .build();
        Collection<Statement> statements = getUpdatePayloadProcessor(metadata)
                .update(clazz, original, updated, checkMandatoryFeatures);

        modifyStatementExecutor.executeStatements(new NamedParameterJdbcTemplate(dataSource), statements);

        final Optional<Payload> result = readByIdentifier(clazz, original.getAs(identifierProvider.getType(), identifierProvider.getName()), queryCustomizer);
        checkArgument(result.isPresent(), "Updating " + AsmUtils.getClassifierFQName(clazz) + " failed");

        // Collect clientReferenceId recursively and map back to response
        Map<Serializable, Object> clientReferenceMap = new HashMap<>();
        traversePayload(
                getCollectPayloadClientReferenceConsumer(clientReferenceMap),
                PayloadTraverser.builder()
                        .transferObjectType(clazz)
                        .payload(updated)
                        .asmModel(asmModel)
                        .build());

        collectInsertStatementsClientReferenceId(clientReferenceMap, statements);
        Payload ret = result.get();

        Set<Serializable> insertedIds = statements.stream()
                .filter(st -> st instanceof InsertStatement)
                .map(st -> (Serializable) ((InsertStatement) st).getInstance().getIdentifier())
                .collect(Collectors.toSet());

        traversePayload(
                getApplyClientReferenceIdConsumer(clientReferenceMap)
                        .andThen(getMarkInsertedPayloadsConsumer(insertedIds)),
                PayloadTraverser.builder()
                        .transferObjectType(clazz)
                        .payload(ret)
                        .asmModel(asmModel)
                        .build());

        return ret;
    }

    private Collection<Statement> createAddAndRemoveReferenceForPayload(Collection<Serializable> identifiersExists, EReference mappedReference,
                                                                            Serializable id, Collection<Serializable> identifiersToAdd,
                                                                            Collection<Serializable> identifiersToRemove) {

        AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());

        // Collect which already added and it contained in the given collection.
        Collection<Serializable> identifiersAlreadyExistsInAdded = identifiersExists
                .stream()
                .filter(identifiersToAdd::contains).collect(Collectors.toSet());


        // Collect which already added and it contained in the given collection.
        Collection<Serializable> identifiersNotExistsInRemoved = identifiersExists
                .stream()
                .filter(i -> !identifiersToRemove.contains(i)).collect(Collectors.toSet());


        Collection<Serializable> idsToAdd = new HashSet<>(identifiersToAdd);
        idsToAdd.removeAll(identifiersAlreadyExistsInAdded);
        identifiersExists.removeAll(identifiersNotExistsInRemoved);

        // Check for containment entity could not add reference
        EReference entityReference = asmUtils
                .getMappedReference(mappedReference)
                .orElseThrow(() -> new IllegalStateException("Mapped reference not found: " + AsmUtils.getReferenceFQName(mappedReference)));

        Collection<Statement> removeReferenceStatements;

        if (entityReference.isContainment()) {
            // Delete phsically
            removeReferenceStatements =
                    getDeletePayloadProcessor()
                            .delete(mappedReference.getEReferenceType(), identifiersExists);
        } else {
            // Collect removable identifier
            removeReferenceStatements =
                    createRemoveReferencesForPayload(mappedReference, id, identifiersExists);
        }

        // Add the given collection
        Collection<Statement> addReferenceStatements =
                createAddReferencesForPayload(mappedReference, id, idsToAdd);

        return Stream.concat(removeReferenceStatements.stream(),
                addReferenceStatements.stream()).collect(Collectors.toSet());
    }

    public void setReferenceOfInstance(EReference mappedReference, Serializable id, Collection<Serializable> identifiersToSet) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "SET is not supported in stateless operation");

        List<Payload> referencedPayloads = readAllReferences(mappedReference, Collections.singleton(id));

        // Remove all existence reference
        Collection<Serializable> identifiersExists = referencedPayloads.stream().map(p -> (Serializable) p.get(getIdentifierProvider().getName())).collect(Collectors.toSet());
        Collection<Serializable> identifiersRemove = identifiersExists.stream()
                .filter(_id -> !identifiersToSet.contains(_id))
                .collect(Collectors.toSet());
        Collection<Serializable> identifiersAdd = identifiersToSet.stream()
                .filter(_id -> !identifiersExists.contains(_id))
                .collect(Collectors.toSet());

        int count = identifiersExists.size() - identifiersRemove.size() + identifiersAdd.size();
        checkArgument(count >= mappedReference.getLowerBound(), "Lower cardinality violated");
        if (mappedReference.getUpperBound() != -1) {
            checkArgument(count <= mappedReference.getUpperBound(), "Upper cardinality violated");
        }

        if (!identifiersAdd.isEmpty() || !identifiersRemove.isEmpty()) {
            Collection<Statement> statements = createAddAndRemoveReferenceForPayload(identifiersExists, mappedReference, id, identifiersAdd, identifiersRemove);

            modifyStatementExecutor.executeStatements(new NamedParameterJdbcTemplate(dataSource), statements);
        }
    }

    public void unsetReferenceOfInstance(EReference mappedReference, Serializable id) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "UNSET is not supported in stateless operation");

        checkArgument(mappedReference.getUpperBound() == 1 && mappedReference.getLowerBound() == 0, "This operation can be called on single optional reference only");

        List<Payload> referencedPayloads = readAllReferences(mappedReference, Collections.singleton(id));
        Collection<Serializable> identifiersExists = referencedPayloads.stream().map(p -> (Serializable) p.get(getIdentifierProvider().getName())).collect(Collectors.toSet());

        if (!identifiersExists.isEmpty()) {
            Collection<Statement> statements = createAddAndRemoveReferenceForPayload(identifiersExists, mappedReference, id, ImmutableSet.of(), identifiersExists);

            modifyStatementExecutor.executeStatements(new NamedParameterJdbcTemplate(dataSource), statements);
        }
    }

    public void addReferencesOfInstance(EReference mappedReference, Serializable id, Collection<Serializable> identifiersToAdd) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "ADD is not supported in stateless operation");

        List<Payload> referencedPayloads = readAllReferences(mappedReference, Collections.singleton(id));

        Collection<Serializable> identifiersExists = referencedPayloads.stream().map(p -> (Serializable) p.get(getIdentifierProvider().getName())).collect(Collectors.toSet());
        Collection<Serializable> identifiersToAddExistingRemoved = new HashSet<>(identifiersToAdd);
        identifiersToAddExistingRemoved.removeAll(identifiersExists);

        if (mappedReference.getUpperBound() != -1) {
            checkArgument(identifiersExists.size() + identifiersToAddExistingRemoved.size() <= mappedReference.getUpperBound(), "Upper cardinality violated");
        }

        if (!identifiersToAddExistingRemoved.isEmpty()) {
            Collection<Statement> statements = createAddAndRemoveReferenceForPayload(identifiersExists, mappedReference, id,
                    identifiersToAddExistingRemoved, ImmutableSet.of());

            modifyStatementExecutor.executeStatements(new NamedParameterJdbcTemplate(dataSource), statements);
        }
    }

    public void removeReferencesOfInstance(EReference mappedReference, Serializable id, Collection<Serializable> identifiersToRemove) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "REMOVE is not supported in stateless operation");

        List<Payload> referencedPayloads = readAllReferences(mappedReference, Collections.singleton(id));

        Collection<Serializable> identifiersExists = referencedPayloads.stream().map(p -> (Serializable) p.get(getIdentifierProvider().getName())).collect(Collectors.toSet());

        Collection<Serializable> identifiersToRemoveChecked = new HashSet<>(identifiersToRemove);
        identifiersToRemoveChecked.removeIf((missingId) -> !identifiersExists.contains(missingId));
        checkArgument(identifiersExists.size() - identifiersToRemoveChecked.size() >= mappedReference.getLowerBound(), "Lower cardinality violated");

        if (!identifiersToRemoveChecked.isEmpty()) {
            Collection<Statement> statements = createAddAndRemoveReferenceForPayload(identifiersExists, mappedReference, id, ImmutableSet.of(),
                    identifiersToRemoveChecked);

            modifyStatementExecutor.executeStatements(new NamedParameterJdbcTemplate(dataSource), statements);
        }
    }

    @Override
    protected Optional<Payload> readMetadataByIdentifier(EClass clazz, Serializable identifier) {
        return selectStatementExecutor.selectMetadata(new NamedParameterJdbcTemplate(dataSource), clazz, identifier);
    }

    @Override
    protected Payload readDefaultsOf(EClass clazz) {
        return readDefaultsOf(clazz, true);
    }

    @Override
    protected Payload readDefaultsOf(EClass clazz, boolean includeNonEmbeddedAssociations) {
        Payload template = Payload.empty();
        AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());

        List<EAttribute> attributes = clazz.getEAllAttributes().stream().filter(a -> a.isChangeable() && !a.isDerived()).toList();
        for (EAttribute attribute : attributes) {
            String defaultAttributeName = AsmUtils.getExtensionAnnotationValue(attribute, "default", false).orElse(null);
            if (defaultAttributeName != null) {
                Optional<EAttribute> defaultAttribute =
                        clazz.getEAllAttributes().stream()
                             .filter(a -> Objects.equals(a.getName(), defaultAttributeName))
                             .findAny();
                defaultAttribute.ifPresent(eAttribute -> template.put(attribute.getName(), getStaticData(eAttribute).get(eAttribute.getName())));
            }
        }

        // in case a composition has default value, it might cause problems
        List<EReference> references = clazz.getEAllReferences().stream().filter(r -> includeNonEmbeddedAssociations || AsmUtils.isEmbedded(r)).filter(r -> r.isChangeable() && !r.isDerived()).toList();
        for (EReference reference : references) {
            String defaultReferenceName = AsmUtils.getExtensionAnnotationValue(reference, "default", false).orElse(null);
            if (defaultReferenceName != null) {
                EReference defaultReference =
                        clazz.getEAllReferences().stream()
                             .filter(df -> Objects.equals(df.getName(), defaultReferenceName))
                             .findAny()
                             .orElseThrow(() -> new IllegalStateException("Default reference not found for %s: %s".formatted(AsmUtils.getReferenceFQName(reference), defaultReferenceName)));

                List<Payload> defaultValues = getAllReferencedInstancesOf(defaultReference, defaultReference.getEReferenceType());
                if (defaultReference.isMany()) {
                    template.put(reference.getName(), defaultValues);
                } else {
                    template.put(reference.getName(), !defaultValues.isEmpty() ? defaultValues.get(0) : null);
                }
            }
        }

        Optional<EClass> defaultTransferObjectType =
                asmUtils.getMappedEntityType(clazz)
                        .flatMap(e -> AsmUtils.getExtensionAnnotationValue(e, "defaultRepresentation", false)
                                              .flatMap(asmUtils::resolve))
                        .filter(t -> t instanceof EClass)
                        .map(t -> (EClass) t);

        if (defaultTransferObjectType.isPresent() && !Objects.equals(defaultTransferObjectType.get(), clazz)) {
            // if the transfer object has a mapping, read default values of the mapped features
            // and add them to the template if they are not already present
            Payload entityTypeDefaults = readDefaultsOf(defaultTransferObjectType.get());
            template.putAll(clazz.getEAllAttributes().stream()
                                 .filter(a -> !template.containsKey(a.getName()) && asmUtils.getMappedAttribute(a).isPresent())
                                 .collect(Collectors.toMap(identity(), a -> asmUtils.getMappedAttribute(a).get())).entrySet().stream()
                                 .filter(e -> entityTypeDefaults.get(e.getValue().getName()) != null && !AsmUtils.annotatedAsTrue(e.getValue(), "unmappedDefaultOnly"))
                                 .collect(Collectors.toMap(e -> e.getKey().getName(), e -> entityTypeDefaults.get(e.getValue().getName()))));
            template.putAll(clazz.getEAllReferences().stream()
                    .filter(r -> !template.containsKey(r.getName()) && asmUtils.getMappedReference(r).isPresent() && (includeNonEmbeddedAssociations || AsmUtils.isEmbedded(r)))
                    .collect(Collectors.toMap(identity(), r -> asmUtils.getMappedReference(r).get())).entrySet().stream()
                    .filter(e -> entityTypeDefaults.get(e.getValue().getName()) != null && !AsmUtils.annotatedAsTrue(e.getValue(), "unmappedDefaultOnly"))
                    .collect(Collectors.toMap(e -> e.getKey().getName(), e -> entityTypeDefaults.get(e.getValue().getName()))));
        }

        return template;
    }

    @Override
    protected void applyDeepDefaultsOf(EClass clazz, Payload payload) {
        AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());
        hu.blackbelt.judo.runtime.core.PayloadTraverser.builder()
                                                       .processor((_payload, context) -> {
                                                           // checking for identifier might be to strict here
                                                           // TODO: if this causes issues later, it should be a parameter
                                                           if (!requireNonNullElse(_payload.getAs(Boolean.class, DEFAULT_VALUES_LOADED_KEY), false)
                                                               && !_payload.containsKey(identifierProvider.getName())) {
                                                               Payload defaultValues = readDefaultsOf(context.getType());
                                                               for (Map.Entry<String, Object> e : defaultValues.entrySet()) {
                                                                   // putIfAbsent is intentionally avoided to keep explicitly set null values
                                                                   if (!_payload.containsKey(e.getKey())) {
                                                                       _payload.put(e.getKey(), e.getValue());
                                                                   }
                                                               }
                                                               _payload.put(DEFAULT_VALUES_LOADED_KEY, true);
                                                           }
                                                       })
                                                       .predicate(reference -> asmUtils.getMappedReference(reference).map(r -> r.isChangeable() && !r.isDerived()).orElse(false))
                                                       .build()
                                                       .traverse(payload, clazz);
    }

    @Override
    protected Collection<Payload> readRangeOf(final EReference reference, final Payload payload, QueryCustomizer queryCustomizer, boolean stateful, boolean markSelectedRangeItems) {
        final EReference rangeTransferRelation = AsmUtils.getExtensionAnnotationValue(reference, "range", false)
                .map(rangeTransferRelationName -> reference.getEContainingClass().getEAllReferences().stream().filter(r -> rangeTransferRelationName.equals(r.getName())).findAny()
                        .orElseThrow(() -> new IllegalStateException("Reference not found on containing class: " + rangeTransferRelationName)))
                .orElseThrow(() -> new IllegalStateException("No range defined"));

        Serializable instanceId = payload != null ? payload.getAs(identifierProvider.getType(), identifierProvider.getName()) : null;
        final BiFunction<Payload, Set<Serializable>, Payload> markSelected = (p, selected) -> {
            final Serializable id = p.getAs(identifierProvider.getType(), identifierProvider.getName());
            if (selected.contains(id)) {
                p.put(StatementExecutor.SELECTED_ITEM_KEY, Boolean.TRUE);
            }
            return p;
        };

        final Set<Serializable> currentReferences;
        if (!AsmUtils.annotatedAsTrue(reference, "transient") && markSelectedRangeItems && instanceId != null) {
            currentReferences = searchNavigationResultAt(instanceId, reference, QueryCustomizer.builder().withoutFeatures(true).build()).stream()
                    .map(p -> p.getAs(identifierProvider.getType(), identifierProvider.getName()))
                    .collect(Collectors.toSet());
        } else {
            currentReferences = Collections.emptySet();
        }

        if (queryFactory.isStaticReference(rangeTransferRelation)) {
            return searchReferencedInstancesOf(rangeTransferRelation, rangeTransferRelation.getEReferenceType(), queryCustomizer).stream()
                    .map(p -> markSelected.apply(p, currentReferences))
                    .collect(Collectors.toList());
        } else {
            if (stateful) {
                final Payload temporaryInstance;
                if (instanceId != null) {
                    temporaryInstance = update(reference.getEContainingClass(), payload, QueryCustomizer.<Serializable>builder()
                            .mask(Collections.emptyMap())
                            .build(), false);
                } else if (payload != null) {
                    temporaryInstance = create(reference.getEContainingClass(), payload, QueryCustomizer.<Serializable>builder()
                            .mask(Collections.emptyMap())
                            .build(), false);
                    instanceId = temporaryInstance.getAs(identifierProvider.getType(), identifierProvider.getName());
                } else {
                    throw new IllegalArgumentException("Missing input to get range");
                }

                if (log.isDebugEnabled()) {
                    log.debug("Saved temporary instance {} with ID: {}", AsmUtils.getClassifierFQName(reference.getEContainingClass()), temporaryInstance.get(identifierProvider.getName()));
                }
            } else {
                if (instanceId == null) {
                    throw new IllegalArgumentException("The given instance (payload) is not stored");
                }
            }

            return searchNavigationResultAt(instanceId, rangeTransferRelation, queryCustomizer).stream()
                    .map(p -> markSelected.apply(p, currentReferences))
                    .collect(Collectors.toList());
        }
    }

    @Override
    protected long calculateNumberRangeOf(final EReference reference, final Payload payload, QueryCustomizer queryCustomizer, boolean stateful) {
        final EReference rangeTransferRelation = AsmUtils.getExtensionAnnotationValue(reference, "range", false)
                .map(rangeTransferRelationName -> reference.getEContainingClass().getEAllReferences().stream().filter(r -> rangeTransferRelationName.equals(r.getName())).findAny()
                        .orElseThrow(() -> new IllegalStateException("Reference: " + rangeTransferRelationName + " not found on containing class: "
                                + AsmUtils.getClassifierFQName(reference.getEContainingClass()))))
                .orElseThrow(() -> new IllegalStateException("No range defined"));

        Serializable instanceId = payload != null ? payload.getAs(identifierProvider.getType(), identifierProvider.getName()) : null;

        if (queryFactory.isStaticReference(rangeTransferRelation)) {
            return countReferencedInstancesOf(rangeTransferRelation, rangeTransferRelation.getEReferenceType(), queryCustomizer);
        } else {
            if (stateful) {
                final Payload temporaryInstance;
                if (instanceId != null) {
                    temporaryInstance = update(reference.getEContainingClass(), payload, QueryCustomizer.<Serializable>builder()
                            .mask(Collections.emptyMap())
                            .build(), false);
                } else if (payload != null) {
                    temporaryInstance = create(reference.getEContainingClass(), payload, QueryCustomizer.<Serializable>builder()
                            .mask(Collections.emptyMap())
                            .build(), false);
                    instanceId = temporaryInstance.getAs(identifierProvider.getType(), identifierProvider.getName());
                } else {
                    throw new IllegalArgumentException("Missing input to get range");
                }

                if (log.isDebugEnabled()) {
                    log.debug("Saved temporary instance {} with ID: {}", AsmUtils.getClassifierFQName(reference.getEContainingClass()), temporaryInstance.get(identifierProvider.getName()));
                }
            } else {
                if (instanceId == null) {
                    throw new IllegalArgumentException("The given instance (payload) is not stored");
                }
            }

            return countNavigationResultAt(instanceId, rangeTransferRelation, queryCustomizer);
        }
    }

    @Override
    protected MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }

    private Collection<Statement> createAddReferencesForPayload(EReference mappedReference, Serializable id, Collection<Serializable> collection) {
        AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());

        // Check the reference is mapped
        checkArgument(asmUtils.getMappedReference(mappedReference).isPresent(),
                "Reference have to be mapped: " + getReferenceFQName(mappedReference) + " ID: " + id);
        EReference entityReference = asmUtils.getMappedReference(mappedReference).get();
        return getAddReferencePayloadProcessor().addReference(entityReference, collection, id, true);
    }

    private Collection<Statement> createRemoveReferencesForPayload(EReference mappedReference, Serializable id, Collection<Serializable> collection) {
        AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());

        // Check the reference is mapped
        checkArgument(asmUtils.getMappedReference(mappedReference).isPresent(),
                "Reference have to be mapped: " + getReferenceFQName(mappedReference) + " ID: " + id);
        EReference entityReference = asmUtils.getMappedReference(mappedReference).get();
        return getRemoveReferencePayloadProcessor().removeReference(entityReference, collection, id, true);
    }


    private void collectInsertStatementsClientReferenceId(Map<Serializable, Object> clientReferenceMap, Collection<Statement> statements) {

        clientReferenceMap.putAll(
                statements.stream()
                        .filter(InsertStatement.class::isInstance)
                        .map(InsertStatement.class::cast)
                        .filter(s -> s.getClientReferenceIdentifier() != null)
                        .collect(Collectors.toMap(i -> (Serializable) i.getInstance().getIdentifier(), InsertStatement::getClientReferenceIdentifier)));
    }

    private Consumer<PayloadTraverser> getCollectPayloadClientReferenceConsumer(final Map<Serializable, Object> clientReferenceMap) {
        return context -> {
            if (context.getPayload().containsKey(PayloadDaoProcessor.REFERENCE_ID)
                    && context.getPayload().containsKey(identifierProvider.getName())) {
                clientReferenceMap.put((Serializable) context.getPayload().get(identifierProvider.getName()),
                        context.getPayload().get(PayloadDaoProcessor.REFERENCE_ID));
            }
        };
    }

    private Consumer<PayloadTraverser> getApplyClientReferenceIdConsumer(final Map<Serializable, Object> clientReferenceMap) {
        return context -> {
            if (clientReferenceMap.containsKey((Serializable) context.getPayload().get(identifierProvider.getName()))) {
                context.getPayload().put(PayloadDaoProcessor.REFERENCE_ID,
                        clientReferenceMap.get((Serializable) context.getPayload().get(identifierProvider.getName())));
            }
        };
    }

    private Consumer<PayloadTraverser> getMarkInsertedPayloadsConsumer(final Set<Serializable> insertedIds) {
        return context -> {
            if (insertedIds.contains((Serializable) context.getPayload().get(identifierProvider.getName()))) {
                context.getPayload().put(CREATED, true);
            }
        };
    }
}
