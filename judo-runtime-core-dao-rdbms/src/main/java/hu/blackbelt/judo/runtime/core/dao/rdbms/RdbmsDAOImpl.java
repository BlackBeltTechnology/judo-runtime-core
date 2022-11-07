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
import hu.blackbelt.judo.runtime.core.dao.core.processors.*;
import hu.blackbelt.judo.runtime.core.dao.core.statements.InsertStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.Statement;
import hu.blackbelt.judo.runtime.core.dao.core.values.Metadata;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.ModifyStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.SelectStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.security.Principal;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
public class RdbmsDAOImpl<ID> extends AbstractRdbmsDAO<ID> implements DAO<ID> {

    private static final String STATEFUL = "STATEFUL";
    private static final String ROLLBACK = "ROLLBACK";
    public static final String CREATED = "__$created";

    private final AsmModel asmModel;
    private final DataSource dataSource;
    @Getter private final IdentifierProvider<ID> identifierProvider;
    private final InstanceCollector<ID> instanceCollector;
    private final QueryFactory queryFactory;
    private final boolean optimisticLockEnabled;
    private final boolean markSelectedRangeItems;
    private final Context context;
    private final MetricsCollector metricsCollector;
    private final SelectStatementExecutor<ID> selectStatementExecutor;
    @Getter private final AsmUtils asmUtils;
    private final ModifyStatementExecutor modifyStatementExecutor;
    private final EMap<EClass, Boolean> hasDefaultsMap = ECollections.asEMap(new ConcurrentHashMap<>());

    @Builder
    private RdbmsDAOImpl(
            @NonNull AsmModel asmModel,
            @NonNull DataSource dataSource,
            @NonNull IdentifierProvider<ID> identifierProvider,
            @NonNull Context context,
            @NonNull MetricsCollector metricsCollector,
            @NonNull InstanceCollector<ID> instanceCollector,
            @NonNull ModifyStatementExecutor modifyStatementExecutor,
            @NonNull SelectStatementExecutor selectStatementExecutor,
            @NonNull QueryFactory queryFactory,
            Boolean optimisticLockEnabled,
            Boolean markSelectedRangeItems) {
        this.asmModel = asmModel;
        this.dataSource = dataSource;
        this.identifierProvider = identifierProvider;
        this.instanceCollector = instanceCollector;
        this.queryFactory = queryFactory;

        this.optimisticLockEnabled = requireNonNullElse(optimisticLockEnabled, true);
        this.markSelectedRangeItems = requireNonNullElse(markSelectedRangeItems, false);

        this.context = context;
        this.metricsCollector = metricsCollector;

        this.selectStatementExecutor = selectStatementExecutor;
        this.modifyStatementExecutor = modifyStatementExecutor;
        this.asmUtils = new AsmUtils(this.asmModel.getResourceSet());
    }

    private Function<EClass, Payload> getDefaultValuesProvider() {
        return (clazz) -> hasDefaults(clazz) ? getDefaultsOf(clazz) : Payload.empty();
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
        if (classes.isEmpty()) {
            return false;
        } else if (classes.stream()
                .flatMap(c -> c.getEAllStructuralFeatures().stream())
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
        return new InsertPayloadDaoProcessor<ID>(asmModel.getResourceSet(),
                getIdentifierProvider(),
                queryFactory,
                instanceCollector,
                getDefaultValuesProvider(),
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
                getDefaultValuesProvider(),
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

        checkArgument(!getAsmUtils().isMappedTransferObjectType(clazz) && AsmUtils.annotatedAsTrue(clazz, "transferObjectType"), "Clazz must be an unmapped transfer object type");

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
    protected List<Payload> searchByFilter(EClass clazz, QueryCustomizer<ID> queryCustomizer) {
        return selectStatementExecutor.executeSelect(
                        new NamedParameterJdbcTemplate(dataSource), clazz, null, queryCustomizer)
                .stream().map(Payload::asPayload).collect(Collectors.toList());
    }

    @Override
    protected long countByFilter(EClass clazz, QueryCustomizer<ID> queryCustomizer) {
        return selectStatementExecutor.countSelect(
                        new NamedParameterJdbcTemplate(dataSource), clazz, null, queryCustomizer);
    }

    @Override
    protected List<Payload> readAllReferences(EReference reference, Collection<ID> navigationSourceIdentifiers) {
        return selectStatementExecutor.executeSelect(
                        new NamedParameterJdbcTemplate(dataSource), reference, navigationSourceIdentifiers != null ? new HashSet<>(navigationSourceIdentifiers) : null, null)
                .stream().map(Payload::asPayload).collect(Collectors.toList());
    }

    @Override
    protected long countAllReferences(EReference reference, Collection<ID> navigationSourceIdentifiers) {
        return selectStatementExecutor.countSelect(
                        new NamedParameterJdbcTemplate(dataSource), reference, navigationSourceIdentifiers != null ? new HashSet<>(navigationSourceIdentifiers) : null, null);
    }

    @Override
    protected List<Payload> readByIdentifiers(EClass clazz, Collection<ID> identifiers, QueryCustomizer<ID> queryCustomizer) {
        return selectStatementExecutor.executeSelect(
                        new NamedParameterJdbcTemplate(dataSource), clazz, new HashSet<>(identifiers), queryCustomizer)
                .stream().map(Payload::asPayload).collect(Collectors.toList());
    }

    @Override
    protected Optional<Payload> readByIdentifier(EClass clazz, ID identifier, QueryCustomizer<ID> queryCustomizer) {
        return readByIdentifiers(clazz, ImmutableSet.of(identifier), queryCustomizer).stream()
                .map(Payload::asPayload)
                .filter(p -> identifier.equals(p.get(identifierProvider.getName())))
                .findFirst();
    }

    @Override
    protected List<Payload> searchReferences(EReference reference, Collection<ID> navigationSourceIdentifiers, QueryCustomizer<ID> queryCustomizer) {
        return selectStatementExecutor.executeSelect(
                        new NamedParameterJdbcTemplate(dataSource), reference, navigationSourceIdentifiers != null ? new HashSet<>(navigationSourceIdentifiers) : null, queryCustomizer)
                .stream().map(Payload::asPayload).collect(Collectors.toList());
    }

    @Override
    protected long countReferences(EReference reference, Collection<ID> navigationSourceIdentifiers, QueryCustomizer<ID> queryCustomizer) {
        return selectStatementExecutor.countSelect(
                        new NamedParameterJdbcTemplate(dataSource), reference, navigationSourceIdentifiers != null ? new HashSet<>(navigationSourceIdentifiers) : null, queryCustomizer);
    }

    @Override
    protected Payload insertPayload(EClass clazz, Payload payload, QueryCustomizer<ID> queryCustomizer, boolean checkMandatoryFeatures) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "INSERT is not supported in stateless operation");

        final Payload actor = context.getAs(Payload.class, Dispatcher.ACTOR_KEY);
        final Principal principal = context.getAs(Principal.class, Dispatcher.PRINCIPAL_KEY);
        final Metadata<ID> metadata = Metadata.<ID>buildMetadata()
                .timestamp(OffsetDateTime.now())
                .userId(actor != null ? actor.getAs(identifierProvider.getType(), identifierProvider.getName()) : null)
                .username(principal != null ? principal.getName() : null)
                .build();
        Collection<Statement<ID>> statements = getInsertPayloadProcessor(metadata)
                .insert(clazz, payload, checkMandatoryFeatures);


        modifyStatementExecutor.executeStatements(new NamedParameterJdbcTemplate(dataSource), statements);

        // Get the root entity's
        ID identifier = (ID) statements.stream()
                .filter(InsertStatement.class::isInstance)
                .map(InsertStatement.class::cast)
                .filter(i -> i.getContainer().isEmpty())
                .findFirst().orElseThrow(() -> new IllegalStateException("Insert statement could not found")).getInstance().getIdentifier();

        final Optional<Payload> result = readByIdentifier(clazz, identifier, queryCustomizer);
        checkArgument(result.isPresent(), "Creation of " + AsmUtils.getClassifierFQName(clazz) + " failed");

        // Collect clientReferenceId recursively and map back to response
        Map<ID, Object> clientReferenceMap = new HashMap<>();
        traversePayload(
                getCollectPayloadClientReferenceConsumer(clientReferenceMap),
                PayloadTraverser.builder()
                        .transferObjectType(clazz)
                        .payload(payload)
                        .asmUtils(getAsmUtils())
                        .build());

        collectInsertStatementsClientReferenceId(clientReferenceMap, statements);
        Payload ret = result.get();

        Set<ID> insertedIds = statements.stream()
                .filter(st -> st instanceof InsertStatement)
                .map(st -> (ID) ((InsertStatement) st).getInstance().getIdentifier())
                .collect(Collectors.toSet());

        traversePayload(
                getApplyClientReferenceIdConsumer(clientReferenceMap)
                        .andThen(getMarkInsertedPayloadsConsumer(insertedIds)),
                PayloadTraverser.builder()
                        .transferObjectType(clazz)
                        .payload(ret)
                        .asmUtils(getAsmUtils())
                        .build());
        return ret;
    }

    @Override
    protected Payload insertPayloadAndAttach(EReference reference, ID identifier, Payload payload, QueryCustomizer<ID> queryCustomizer) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "INSERT is not supported in stateless operation");

        EClass typeOfNewInstance = reference.getEReferenceType();
        EReference mappedReference = getAsmUtils().getMappedReference(reference)
                .orElseThrow(() -> new IllegalArgumentException("Mapping of transfer object relation not found: " + AsmUtils.getReferenceFQName(reference)));
        Payload result;

        if (mappedReference.isContainment()) {
            Payload container = getByIdentifier(reference.getEContainingClass(), identifier)
                    .orElseThrow(() -> new IllegalArgumentException("Container not found: " + AsmUtils.getReferenceFQName(reference)));
            if (reference.isMany()) {
                Collection<Payload> containments = container.getAsCollectionPayload(reference.getName());
                if (reference.getUpperBound() == -1 || containments == null || containments.size() < reference.getUpperBound()) {
                    Payload referenced = create(typeOfNewInstance, payload, QueryCustomizer.<ID>builder()
                            .mask(Collections.emptyMap())
                            .build());
                    ID referencedId = referenced.getAs(identifierProvider.getType(), identifierProvider.getName());
                    addReferencesOfInstance(reference, identifier, Collections.singleton(referencedId));
                    result = referenced;
                } else {
                    throw new IllegalArgumentException("Upper cardinality violated");
                }
            } else {
                if (container.get(reference.getName()) != null) {
                    throw new IllegalArgumentException("Containment already set");
                }
                Payload referenced = create(typeOfNewInstance, payload, QueryCustomizer.<ID>builder()
                        .mask(Collections.emptyMap())
                        .build());
                ID referencedId = referenced.getAs(identifierProvider.getType(), identifierProvider.getName());
                addReferencesOfInstance(reference, identifier, Collections.singleton(referencedId));
                result = referenced;
            }
        } else {
            // reference is not containment
            final EReference mappedBackReference = mappedReference.getEOpposite();
            final EList<EReference> backReferences = ECollections.asEList(typeOfNewInstance.getEAllReferences().stream()
                    .filter(br -> getAsmUtils().getMappedReference(br)
                            .filter(mbr -> AsmUtils.equals(mbr, mappedBackReference))
                            .isPresent())
                    .collect(Collectors.toList()));

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
            final Payload referenced = create(typeOfNewInstance, payload, QueryCustomizer.<ID>builder()
                    .mask(Collections.emptyMap())
                    .build());
            final ID referencedId = referenced.getAs(identifierProvider.getType(), identifierProvider.getName());
            // do not add reference if relation is derived
            if (!mappedReference.isDerived()) {
                addReferencesOfInstance(reference, identifier, Collections.singleton(referencedId));
            }

            result = searchNavigationResultAt(identifier, reference, queryCustomizer).stream()
                    .filter(newInstance -> Objects.equals(newInstance.getAs(identifierProvider.getType(), identifierProvider.getName()), referencedId))
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("Unable to create and attached instance"));
        }
        return result;
    }

    @Override
    protected void deletePayload(EClass clazz, Collection<ID> ids) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "DELETE is not supported in stateless operation");

        Collection<Statement<ID>> statements = getDeletePayloadProcessor()
                .delete(clazz, ids);

        modifyStatementExecutor.executeStatements(new NamedParameterJdbcTemplate(dataSource), statements);
    }

    @Override
    protected Payload updatePayload(EClass clazz, Payload original, Payload updated, QueryCustomizer<ID> queryCustomizer, boolean checkMandatoryFeatures) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "UPDATE is not supported in stateless operation");

        final Payload actor = context.getAs(Payload.class, Dispatcher.ACTOR_KEY);
        final Principal principal = context.getAs(Principal.class, Dispatcher.PRINCIPAL_KEY);
        final Metadata<ID> metadata = Metadata.<ID>buildMetadata()
                .timestamp(OffsetDateTime.now())
                .userId(actor != null ? actor.getAs(identifierProvider.getType(), identifierProvider.getName()) : null)
                .username(principal != null ? principal.getName() : null)
                .build();
        Collection<Statement<ID>> statements = getUpdatePayloadProcessor(metadata)
                .update(clazz, original, updated, checkMandatoryFeatures);

        modifyStatementExecutor.executeStatements(new NamedParameterJdbcTemplate(dataSource), statements);

        final Optional<Payload> result = readByIdentifier(clazz, original.getAs(identifierProvider.getType(), identifierProvider.getName()), queryCustomizer);
        checkArgument(result.isPresent(), "Updating " + AsmUtils.getClassifierFQName(clazz) + " failed");

        // Collect clientReferenceId recursively and map back to response
        Map<ID, Object> clientReferenceMap = new HashMap<>();
        traversePayload(
                getCollectPayloadClientReferenceConsumer(clientReferenceMap),
                PayloadTraverser.builder()
                        .transferObjectType(clazz)
                        .payload(updated)
                        .asmUtils(getAsmUtils())
                        .build());

        collectInsertStatementsClientReferenceId(clientReferenceMap, statements);
        Payload ret = result.get();

        Set<ID> insertedIds = statements.stream()
                .filter(st -> st instanceof InsertStatement)
                .map(st -> (ID) ((InsertStatement) st).getInstance().getIdentifier())
                .collect(Collectors.toSet());

        traversePayload(
                getApplyClientReferenceIdConsumer(clientReferenceMap)
                        .andThen(getMarkInsertedPayloadsConsumer(insertedIds)),
                PayloadTraverser.builder()
                        .transferObjectType(clazz)
                        .payload(ret)
                        .asmUtils(getAsmUtils())
                        .build());

        return ret;
    }

    private Collection<Statement<ID>> createAddAndRemoveReferenceForPayload(Collection<ID> identifiersExists, EReference mappedReference,
                                                                            ID id, Collection<ID> identifiersToAdd,
                                                                            Collection<ID> identifiersToRemove) {

        // Collect which already added and it contained in the given collection.
        Collection<ID> identifiersAlreadyExistsInAdded = identifiersExists
                .stream()
                .filter(identifiersToAdd::contains).collect(Collectors.toSet());


        // Collect which already added and it contained in the given collection.
        Collection<ID> identifiersNotExistsInRemoved = identifiersExists
                .stream()
                .filter(i -> !identifiersToRemove.contains(i)).collect(Collectors.toSet());


        Collection<ID> idsToAdd = new HashSet<>(identifiersToAdd);
        idsToAdd.removeAll(identifiersAlreadyExistsInAdded);
        identifiersExists.removeAll(identifiersNotExistsInRemoved);

        // Check for containment entity could not add reference
        EReference entityReference = getAsmUtils()
                .getMappedReference(mappedReference)
                .orElseThrow(() -> new IllegalStateException("Mapped reference not found: " + AsmUtils.getReferenceFQName(mappedReference)));

        Collection<Statement<ID>> removeReferenceStatements;

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
        Collection<Statement<ID>> addReferenceStatements =
                createAddReferencesForPayload(mappedReference, id, idsToAdd);

        return Stream.concat(removeReferenceStatements.stream(),
                addReferenceStatements.stream()).collect(Collectors.toSet());
    }

    public void setReferenceOfInstance(EReference mappedReference, ID id, Collection<ID> identifiersToSet) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "SET is not supported in stateless operation");

        List<Payload> referencedPayloads = readAllReferences(mappedReference, Collections.singleton(id));

        // Remove all existence reference
        Collection<ID> identifiersExists = referencedPayloads.stream().map(p -> (ID) p.get(getIdentifierProvider().getName())).collect(Collectors.toSet());
        Collection<ID> identifiersRemove = identifiersExists.stream()
                .filter(_id -> !identifiersToSet.contains(_id))
                .collect(Collectors.toSet());
        Collection<ID> identifiersAdd = identifiersToSet.stream()
                .filter(_id -> !identifiersExists.contains(_id))
                .collect(Collectors.toSet());

        int count = identifiersExists.size() - identifiersRemove.size() + identifiersAdd.size();
        checkArgument(count >= mappedReference.getLowerBound(), "Lower cardinality violated");
        if (mappedReference.getUpperBound() != -1) {
            checkArgument(count <= mappedReference.getUpperBound(), "Upper cardinality violated");
        }

        if (!identifiersAdd.isEmpty() || !identifiersRemove.isEmpty()) {
            Collection<Statement<ID>> statements = createAddAndRemoveReferenceForPayload(identifiersExists, mappedReference, id, identifiersAdd, identifiersRemove);

            modifyStatementExecutor.executeStatements(new NamedParameterJdbcTemplate(dataSource), statements);
        }
    }

    public void unsetReferenceOfInstance(EReference mappedReference, ID id) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "UNSET is not supported in stateless operation");

        checkArgument(mappedReference.getUpperBound() == 1 && mappedReference.getLowerBound() == 0, "This operation can be called on single optional reference only");

        List<Payload> referencedPayloads = readAllReferences(mappedReference, Collections.singleton(id));
        Collection<ID> identifiersExists = referencedPayloads.stream().map(p -> (ID) p.get(getIdentifierProvider().getName())).collect(Collectors.toSet());

        if (!identifiersExists.isEmpty()) {
            Collection<Statement<ID>> statements = createAddAndRemoveReferenceForPayload(identifiersExists, mappedReference, id, ImmutableSet.of(), identifiersExists);

            modifyStatementExecutor.executeStatements(new NamedParameterJdbcTemplate(dataSource), statements);
        }
    }

    public void addReferencesOfInstance(EReference mappedReference, ID id, Collection<ID> identifiersToAdd) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "ADD is not supported in stateless operation");

        List<Payload> referencedPayloads = readAllReferences(mappedReference, Collections.singleton(id));

        Collection<ID> identifiersExists = referencedPayloads.stream().map(p -> (ID) p.get(getIdentifierProvider().getName())).collect(Collectors.toSet());
        Collection<ID> identifiersToAddExistingRemoved = new HashSet<>(identifiersToAdd);
        identifiersToAddExistingRemoved.removeAll(identifiersExists);

        if (mappedReference.getUpperBound() != -1) {
            checkArgument(identifiersExists.size() + identifiersToAddExistingRemoved.size() <= mappedReference.getUpperBound(), "Upper cardinality violated");
        }

        if (!identifiersToAddExistingRemoved.isEmpty()) {
            Collection<Statement<ID>> statements = createAddAndRemoveReferenceForPayload(identifiersExists, mappedReference, id,
                    identifiersToAddExistingRemoved, ImmutableSet.of());

            modifyStatementExecutor.executeStatements(new NamedParameterJdbcTemplate(dataSource), statements);
        }
    }

    public void removeReferencesOfInstance(EReference mappedReference, ID id, Collection<ID> identifiersToRemove) throws SQLException {
        checkState(!Boolean.FALSE.equals(context.getAs(Boolean.class, STATEFUL)) || Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK)), "REMOVE is not supported in stateless operation");

        List<Payload> referencedPayloads = readAllReferences(mappedReference, Collections.singleton(id));

        Collection<ID> identifiersExists = referencedPayloads.stream().map(p -> (ID) p.get(getIdentifierProvider().getName())).collect(Collectors.toSet());

        Collection<ID> identifiersToRemoveChecked = new HashSet<>(identifiersToRemove);
        identifiersToRemoveChecked.removeIf((missingId) -> !identifiersExists.contains(missingId));
        checkArgument(identifiersExists.size() - identifiersToRemoveChecked.size() >= mappedReference.getLowerBound(), "Lower cardinality violated");

        if (!identifiersToRemoveChecked.isEmpty()) {
            Collection<Statement<ID>> statements = createAddAndRemoveReferenceForPayload(identifiersExists, mappedReference, id, ImmutableSet.of(),
                    identifiersToRemoveChecked);

            modifyStatementExecutor.executeStatements(new NamedParameterJdbcTemplate(dataSource), statements);
        }
    }

    @Override
    protected Optional<Payload> readMetadataByIdentifier(EClass clazz, ID identifier) {
        return selectStatementExecutor.selectMetadata(new NamedParameterJdbcTemplate(dataSource), clazz, identifier);
    }

    @Override
    protected Payload readDefaultsOf(EClass clazz) {
        final Payload template = Payload.empty();

        clazz.getEAllAttributes().stream()
                .filter(EStructuralFeature::isChangeable)
                .forEach(a -> AsmUtils.getExtensionAnnotationValue(a, "default", false).ifPresent(defaultFeatureName -> {
                    final EAttribute defaultAttribute = clazz.getEAllAttributes().stream()
                            .filter(df -> Objects.equals(df.getName(), defaultFeatureName))
                            .findAny()
                            .orElseThrow(() -> new IllegalStateException("Default attribute not found: " + defaultFeatureName));

                    final Payload defaultValue = getStaticData(defaultAttribute);
                    if (defaultValue.get(defaultAttribute.getName()) == null && a.isRequired()) {
                        throw new IllegalStateException("Default attribute value is undefined on required attribute: " + defaultFeatureName);
                    }
                    template.put(a.getName(), defaultValue.get(defaultAttribute.getName()));
                }));

        clazz.getEAllReferences().stream()
                .filter(EStructuralFeature::isChangeable)
                .forEach(r -> AsmUtils.getExtensionAnnotationValue(r, "default", false).ifPresent(defaultReferenceName -> {
                    final EReference defaultReference = clazz.getEAllReferences().stream()
                            .filter(df -> Objects.equals(df.getName(), defaultReferenceName))
                            .findAny()
                            .orElseThrow(() -> new IllegalStateException("Default reference not found: " + defaultReferenceName));

                    final List<Payload> defaultValues = getAllReferencedInstancesOf(defaultReference, defaultReference.getEReferenceType());
                    if (defaultReference.isMany()) {
                        template.put(r.getName(), defaultValues);
                    } else {
                        final Payload defaultValue = !defaultValues.isEmpty() ? defaultValues.get(0) : null;
                        if (r.getLowerBound() > 0 && defaultValue == null) {
                            throw new IllegalStateException("Default reference value is undefined on required reference: " + defaultReferenceName);
                        }
                        template.put(r.getName(), defaultValue);
                    }
                }));

        final Optional<EClass> mappedEntityType = getAsmUtils().getMappedEntityType(clazz);

        final Optional<EClass> defaultTransferObjectType = mappedEntityType
                .flatMap(e -> AsmUtils.getExtensionAnnotationValue(e, "defaultRepresentation", false)
                        .flatMap(dr -> getAsmUtils().resolve(dr)))
                .filter(t -> t instanceof EClass).map(t -> (EClass) t);

        if (defaultTransferObjectType.isPresent() && !AsmUtils.equals(defaultTransferObjectType.get(), clazz)) {
            final Payload entityTypeDefaults = readDefaultsOf(defaultTransferObjectType.get());
            template.putAll(clazz.getEAllAttributes().stream()
                    .filter(a -> template.get(a.getName()) == null && getAsmUtils().getMappedAttribute(a).isPresent())
                    .collect(Collectors.toMap(
                            identity(),
                            a -> getAsmUtils().getMappedAttribute(a).orElseThrow(() -> new IllegalStateException("Mapped attribute not found: " + AsmUtils.getAttributeFQName(a)))))
                    .entrySet().stream()
                    .filter(e -> entityTypeDefaults.get(e.getValue().getName()) != null && !AsmUtils.annotatedAsTrue(e.getValue(), "unmappedDefaultOnly"))
                    .collect(Collectors.toMap(
                            e -> e.getKey().getName(),
                            e -> entityTypeDefaults.get(e.getValue().getName()))));
            template.putAll(clazz.getEAllReferences().stream()
                    .filter(r -> template.get(r.getName()) == null && getAsmUtils().getMappedReference(r).isPresent())
                    .collect(Collectors.toMap(
                            identity(),
                            r -> getAsmUtils().getMappedReference(r).orElseThrow(() -> new IllegalStateException("Mapped reference not found: " + AsmUtils.getReferenceFQName(r)))))
                    .entrySet().stream()
                    .filter(e -> entityTypeDefaults.get(e.getValue().getName()) != null && !AsmUtils.annotatedAsTrue(e.getValue(), "unmappedDefaultOnly"))
                    .collect(Collectors.toMap(
                            e -> e.getKey().getName(),
                            e -> entityTypeDefaults.get(e.getValue().getName()))));
        }

        return template;
    }

    @Override
    protected Collection<Payload> readRangeOf(final EReference reference, final Payload payload, QueryCustomizer<ID> queryCustomizer) {
        final EReference rangeTransferRelation = AsmUtils.getExtensionAnnotationValue(reference, "range", false)
                .map(rangeTransferRelationName -> reference.getEContainingClass().getEAllReferences().stream().filter(r -> rangeTransferRelationName.equals(r.getName())).findAny()
                        .orElseThrow(() -> new IllegalStateException("Refence not found on containing class: " + rangeTransferRelationName)))
                .orElseThrow(() -> new IllegalStateException("No range defined"));

        ID instanceId = payload != null ? payload.getAs(identifierProvider.getType(), identifierProvider.getName()) : null;
        final BiFunction<Payload, Set<ID>, Payload> markSelected = (p, selected) -> {
            final ID id = p.getAs(identifierProvider.getType(), identifierProvider.getName());
            if (selected.contains(id)) {
                p.put(StatementExecutor.SELECTED_ITEM_KEY, Boolean.TRUE);
            }
            return p;
        };

        final Set<ID> currentReferences;
        if (markSelectedRangeItems && instanceId != null) {
            currentReferences = searchNavigationResultAt(instanceId, reference, QueryCustomizer.<ID>builder().withoutFeatures(true).build()).stream()
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
            final Payload temporaryInstance;
            if (instanceId != null) {
                temporaryInstance = update(reference.getEContainingClass(), payload, QueryCustomizer.<ID>builder()
                        .mask(Collections.emptyMap())
                        .build(), false);
            } else if (payload != null) {
                temporaryInstance = create(reference.getEContainingClass(), payload, QueryCustomizer.<ID>builder()
                        .mask(Collections.emptyMap())
                        .build(), false);
                instanceId = temporaryInstance.getAs(identifierProvider.getType(), identifierProvider.getName());
            } else {
                throw new IllegalArgumentException("Missing input to get range");
            }

            if (log.isDebugEnabled()) {
                log.debug("Saved temporary instance {} with ID: {}", AsmUtils.getClassifierFQName(reference.getEContainingClass()), temporaryInstance.get(identifierProvider.getName()));
            }
            return searchNavigationResultAt(instanceId, rangeTransferRelation, queryCustomizer).stream()
                    .map(p -> markSelected.apply(p, currentReferences))
                    .collect(Collectors.toList());
        }
    }

    @Override
    protected long calculateNumberRangeOf(final EReference reference, final Payload payload, QueryCustomizer<ID> queryCustomizer) {
        final EReference rangeTransferRelation = AsmUtils.getExtensionAnnotationValue(reference, "range", false)
                .map(rangeTransferRelationName -> reference.getEContainingClass().getEAllReferences().stream().filter(r -> rangeTransferRelationName.equals(r.getName())).findAny()
                        .orElseThrow(() -> new IllegalStateException("Reference: " + rangeTransferRelationName + " not found on containing class: "
                                + AsmUtils.getClassifierFQName(reference.getEContainingClass()))))
                .orElseThrow(() -> new IllegalStateException("No range defined"));

        ID instanceId = payload != null ? payload.getAs(identifierProvider.getType(), identifierProvider.getName()) : null;

        if (queryFactory.isStaticReference(rangeTransferRelation)) {
            return countReferencedInstancesOf(rangeTransferRelation, rangeTransferRelation.getEReferenceType(), queryCustomizer);
        } else {
            final Payload temporaryInstance;
            if (instanceId != null) {
                temporaryInstance = update(reference.getEContainingClass(), payload, QueryCustomizer.<ID>builder()
                        .mask(Collections.emptyMap())
                        .build(), false);
            } else if (payload != null) {
                temporaryInstance = create(reference.getEContainingClass(), payload, QueryCustomizer.<ID>builder()
                        .mask(Collections.emptyMap())
                        .build(), false);
                instanceId = temporaryInstance.getAs(identifierProvider.getType(), identifierProvider.getName());
            } else {
                throw new IllegalArgumentException("Missing input to get range");
            }

            if (log.isDebugEnabled()) {
                log.debug("Saved temporary instance {} with ID: {}", AsmUtils.getClassifierFQName(reference.getEContainingClass()), temporaryInstance.get(identifierProvider.getName()));
            }
            return countNavigationResultAt(instanceId, rangeTransferRelation, queryCustomizer);
        }
    }

    @Override
    protected MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }

    private Collection<Statement<ID>> createAddReferencesForPayload(EReference mappedReference, ID id, Collection<ID> collection) {
        // Check the reference is mapped
        checkArgument(getAsmUtils().getMappedReference(mappedReference).isPresent(),
                "Reference have to be mapped: " + getReferenceFQName(mappedReference) + " ID: " + id);
        EReference entityReference = getAsmUtils().getMappedReference(mappedReference).get();
        return getAddReferencePayloadProcessor().addReference(entityReference, collection, id, true);
    }

    private Collection<Statement<ID>> createRemoveReferencesForPayload(EReference mappedReference, ID id, Collection<ID> collection) {
        // Check the reference is mapped
        checkArgument(getAsmUtils().getMappedReference(mappedReference).isPresent(),
                "Reference have to be mapped: " + getReferenceFQName(mappedReference) + " ID: " + id);
        EReference entityReference = getAsmUtils().getMappedReference(mappedReference).get();
        return getRemoveReferencePayloadProcessor().removeReference(entityReference, collection, id, true);
    }


    private void collectInsertStatementsClientReferenceId(Map<ID, Object> clientReferenceMap, Collection<Statement<ID>> statements) {

        clientReferenceMap.putAll(
                statements.stream()
                        .filter(InsertStatement.class::isInstance)
                        .map(InsertStatement.class::cast)
                        .filter(s -> s.getClientReferenceIdentifier() != null)
                        .collect(Collectors.toMap(i -> (ID) i.getInstance().getIdentifier(), InsertStatement::getClientReferenceIdentifier)));
    }

    private Consumer<PayloadTraverser> getCollectPayloadClientReferenceConsumer(final Map<ID, Object> clientReferenceMap) {
        return context -> {
            if (context.getPayload().containsKey(PayloadDaoProcessor.REFERENCE_ID)
                    && context.getPayload().containsKey(identifierProvider.getName())) {
                clientReferenceMap.put((ID) context.getPayload().get(identifierProvider.getName()),
                        context.getPayload().get(PayloadDaoProcessor.REFERENCE_ID));
            }
        };
    }

    private Consumer<PayloadTraverser> getApplyClientReferenceIdConsumer(final Map<ID, Object> clientReferenceMap) {
        return context -> {
            if (clientReferenceMap.containsKey((ID) context.getPayload().get(identifierProvider.getName()))) {
                context.getPayload().put(PayloadDaoProcessor.REFERENCE_ID,
                        clientReferenceMap.get((ID) context.getPayload().get(identifierProvider.getName())));
            }
        };
    }

    private Consumer<PayloadTraverser> getMarkInsertedPayloadsConsumer(final Set<ID> insertedIds) {
        return context -> {
            if (insertedIds.contains((ID) context.getPayload().get(identifierProvider.getName()))) {
                context.getPayload().put(CREATED, true);
            }
        };
    }
}
