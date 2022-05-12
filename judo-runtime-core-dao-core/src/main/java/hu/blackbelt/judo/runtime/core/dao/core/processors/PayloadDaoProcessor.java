package hu.blackbelt.judo.runtime.core.dao.core.processors;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.measure.Unit;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import lombok.*;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.resource.ResourceSet;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

@NoArgsConstructor
public class PayloadDaoProcessor<ID> {

    public static final int MEASURE_CONVERTING_SCALE = 20;
    public static final String REFERENCE_ID = "__referenceId";
    public static final String VERSION = "__version";

    @NonNull
    @Getter
    private ResourceSet resourceSet;

    @NonNull
    @Getter
    @Setter
    private IdentifierProvider<ID> identifierProvider;

    @NonNull
    @Getter
    @Setter
    private QueryFactory queryFactory;

    @NonNull
    @Getter
    @Setter
    private InstanceCollector<ID> instanceCollector;

    @Getter
    private AsmUtils asmUtils;

    public static Predicate<EStructuralFeature> isSingle = (r) -> !r.isMany();
    public static Predicate<EStructuralFeature> isCollection = (r) -> r.isMany();
    public static Predicate<EStructuralFeature> isMandatory = (r) -> r.isRequired();
    public static Predicate<EStructuralFeature> isDerived = (r) -> r.isDerived();
    public static Predicate<EStructuralFeature> notDerived = (r) -> !r.isDerived();
    public static Predicate<EStructuralFeature> isChangeable = (r) -> r.isChangeable();
    public static Predicate<EStructuralFeature> notChangeable = (r) -> !r.isChangeable();

    @Builder
    public PayloadDaoProcessor(ResourceSet resourceSet, IdentifierProvider<ID> identifierProvider, QueryFactory queryFactory, InstanceCollector<ID> instanceCollector) {
        this.resourceSet = resourceSet;
        this.identifierProvider = identifierProvider;
        this.queryFactory = queryFactory;
        this.instanceCollector = instanceCollector;
        this.asmUtils = new AsmUtils(resourceSet);
    }

    public void setResourceSet(ResourceSet resourceSet) {
        this.resourceSet = resourceSet;
        asmUtils = new AsmUtils(resourceSet);
    }

    public static Predicate<EStructuralFeature> hasNotPayload(Payload payload) {
        return (r) -> !payload.containsKey(r.getName());
    }

    public static Predicate<EStructuralFeature> hasNotPayloadOrNull(Payload payload) {
        return (r) -> !payload.containsKey(r.getName()) || payload.get(r.getName()) == null;
    }

    public static Predicate<EStructuralFeature> hasPayload(Payload payload) {
        return (r) -> payload.containsKey(r.getName());
    }

    public static Predicate<EStructuralFeature> hasPayloadNotNull(Payload payload) {
        return (r) -> payload.containsKey(r.getName()) && payload.get(r.getName()) != null;
    }

    @SuppressWarnings("unchecked")
	public static Predicate<EStructuralFeature> payloadTypeIsInstanceOf(Payload payload, @SuppressWarnings("rawtypes") Class type) {
        return hasPayloadNotNull(payload)
                .and(r -> type.isAssignableFrom(payload.get(r.getName()).getClass()));
    }

    @SuppressWarnings("unchecked")
	public static Predicate<EStructuralFeature> payloadTypeIsNotInstanceOf(Payload payload, @SuppressWarnings("rawtypes") Class type) {
        return hasPayloadNotNull(payload)
                .and(r -> !type.isAssignableFrom(payload.get(r.getName()).getClass()));
    }

    public static Predicate<EReference> notParent(EReference parentReference) {
        return (r) -> !ofNullable(parentReference).isPresent()
                || r.getEOpposite() == null
                || r.getEOpposite() != parentReference;
    }

    public static Predicate<EStructuralFeature> hasReferenced(Payload payload, String identifierName) {
        return hasPayloadNotNull(payload).and(
                (isSingle
                        .and(r -> payload.getAsPayload(r.getName()).containsKey(identifierName)))
                        .or (isCollection
                                .and(r -> payload.getAsCollectionPayload(r.getName()).stream()
                                        .filter(c -> c.containsKey(identifierName))
                                        .count() > 0))
        );
    }

    public static Predicate<EStructuralFeature> hasEmbedded(Payload payload, String identifierName) {
        return hasPayloadNotNull(payload).and(
                (isSingle
                        .and(r -> !payload.getAsPayload(r.getName()).containsKey(identifierName)))
                        .or (isCollection
                                .and(r -> payload.getAsCollectionPayload(r.getName()).stream()
                                        .filter(c -> c.containsKey(identifierName)).count() == 0))
        );
    }

    public static Predicate<EReference> isContainment = (r) -> r.isContainment();
    public static Predicate<EReference> hasOpposite = (r) -> r.getEOpposite() != null;


    public static Collector<EReference, ?, Map<EReference, Collection<Payload>>>
                                                toReferencePayloadMapOfPayloadCollection(Payload payload) {
        return Collectors.toMap(Function.identity(), (r) -> {
            if (isCollection.test(r)) {
                return payload.getAsCollectionPayload(r.getName());
            } else {
                return ImmutableList.of(payload.getAsPayload(r.getName()));
            }
        });
    }

    public void checkMandatoryReferences(List<EReference> references, Payload payload) {
        // Checking mandatory references
        List<EReference> missingReferences = references.stream()
                .filter(isMandatory
                        .and(hasNotPayloadOrNull(payload)))
                .collect(toList());

        checkArgument(missingReferences.size() == 0,
                format("There is missing mandatory attribute/reference on: %s Payload: %s",
                        Joiner.on(",").join(missingReferences), payload));
    }

    public void checkMandatoryAttributes(List<EAttribute> attributes, Payload payload) {
        List<EAttribute> missingAttributes = attributes.stream()
                .filter(isMandatory.and(hasNotPayloadOrNull(payload))).collect(toList());

        checkArgument(missingAttributes.size() == 0,
                format("There is missing mandatory attribute on: %s Payload: %s",
                        Joiner.on(",").join(missingAttributes), payload));

    }


    public void checkMappedObjectStructure(EClass type,
                                           EReference parentReference) {
        Optional<EClass> mappedEntity = asmUtils.getMappedEntityType(type);
        checkState(mappedEntity.isPresent(),
                format("No mapped type is presented, and given class is not type: %s",
                        AsmUtils.getClassifierFQName(type)));

        Optional<EClass> defaultTransferObjectType = AsmUtils.getExtensionAnnotationValue(mappedEntity.get(), "defaultRepresentation", false)
                .map(t -> (EClass) getAsmUtils().resolve(t).orElse(null));

        List<EAttribute> entityAttributesWithDefault = defaultTransferObjectType.isPresent() ? defaultTransferObjectType.get().getEAllAttributes().stream()
                .filter(transferAttribute -> AsmUtils.getExtensionAnnotationValue(transferAttribute, "default", false).isPresent())
                .map(transferAttribute -> AsmUtils.getExtensionAnnotationValue(transferAttribute, "binding", false).orElse(null))
                .map(entityAttributeName -> mappedEntity.get().getEAllAttributes().stream().filter(a -> Objects.equals(a.getName(), entityAttributeName)).findAny().orElse(null))
                .collect(toList()) : Collections.emptyList();

        List<EReference> entityReferencesWithDefault = defaultTransferObjectType.isPresent() ? defaultTransferObjectType.get().getEAllReferences().stream()
                .filter(transferReference -> AsmUtils.getExtensionAnnotationValue(transferReference, "default", false).isPresent())
                .map(transferReference -> AsmUtils.getExtensionAnnotationValue(transferReference, "binding", false).orElse(null))
                .map(entityRelation -> mappedEntity.get().getEAllReferences().stream().filter(a -> Objects.equals(a.getName(), entityRelation)).findAny().orElse(null))
                .collect(toList()) : Collections.emptyList();

        // Checking all type mandatory properties are presented
        List<EAttribute> missingMandatoryAttributesOnTransferObject = mappedEntity.get().getEAllAttributes().stream()
                .filter(isChangeable
                        .and(isMandatory)
                        .and(a -> !type.getEAllAttributes().stream()
                                .anyMatch(a2 -> a.equals(asmUtils.getMappedAttribute(a2).orElse(a2))))
                        .and(a -> !entityAttributesWithDefault.contains(a)))
                .collect(Collectors.toList());

        checkState(missingMandatoryAttributesOnTransferObject.size() == 0,
                format("There is missing mandatory attribute on MappedType: %s EntityType: %s Missing: %s",
                        AsmUtils.getClassifierFQName(type), AsmUtils.getClassifierFQName(mappedEntity.get()),
                        Joiner.on(",").join(missingMandatoryAttributesOnTransferObject)));

        // Checking all type mandatory properties are presented
        List<EReference> missingMandatoryReferencesOnTransferObject = mappedEntity.get().getEAllReferences().stream()
                .filter(notParent(parentReference)
                        .and(isChangeable)
                        .and(isMandatory)
                        .and(r -> !type.getEAllReferences().stream()
                                .anyMatch(r2 -> r.equals(asmUtils.getMappedReference(r2).orElse(r2))))
                        .and(r -> !entityReferencesWithDefault.contains(r)))
                .collect(Collectors.toList());

        checkState(missingMandatoryReferencesOnTransferObject.size() == 0,
                format("There is missing mandatory reference on MappedType: %s EntityType: %s Missing: %s",
                        AsmUtils.getClassifierFQName(type), AsmUtils.getClassifierFQName(mappedEntity.get()),
                        Joiner.on(",").join(missingMandatoryReferencesOnTransferObject)));
    }


    public void checkReferences(List<EReference> references, Payload payload) {
        List<EReference> singleReferencesWithIllegalTypes = references.stream()
                .filter(isSingle
                        .and(payloadTypeIsNotInstanceOf(payload, Map.class)))
                .collect(toList());

        checkArgument(singleReferencesWithIllegalTypes.size() == 0,
                format("There is single references (%s) which have no map type on payload: %s",
                        Joiner.on(",").join(singleReferencesWithIllegalTypes), payload));


        List<EReference> collectionReferencesWithIllegalTypes = references.stream()
                .filter(isCollection
                        .and(payloadTypeIsNotInstanceOf(payload, Collection.class)))
                .collect(toList());

        checkArgument(collectionReferencesWithIllegalTypes.size() == 0,
                format("There is collection references (%s) which have no collection type on payload: %s",
                        Joiner.on(",").join(collectionReferencesWithIllegalTypes), payload));

        // TODO: Attribute types
    }


    public void checkForbiddenReferenceUpdates(List<EReference> references, Payload payload) {
        // Checking when the other side is opposite we could not update, becouse
        // there is an implicit detach which cause violations on
        List<EReference> referencesWithIdentifierWhichAreNotUpdatable = references.stream()
                .filter(hasOpposite
                        .and(hasReferenced(payload, identifierProvider.getName()))
                        .and(r -> r.getEOpposite().getLowerBound() > 0))
                .collect(toList());

        checkArgument(referencesWithIdentifierWhichAreNotUpdatable.size() == 0,
                format("There is references (%s) which cannot updated " +
                                "without the violation of constraint on payload: %s ",
                        Joiner.on(",").join(referencesWithIdentifierWhichAreNotUpdatable), payload));

    }

    public void checkAssociationCannotBeEmbedded(List<EReference> references, Payload payload) {
        // Associations cannot be embedded entities (???)
        List<EReference> associationReferencesWithoutIdentifier = references.stream()
                .filter(isContainment
                        .negate()
                        .and(hasEmbedded(payload, identifierProvider.getName())))
                .collect(toList());

        checkArgument(associationReferencesWithoutIdentifier.size() == 0,
                format("There is associative references (%s) which haven't got existing entities on payload: %s ",
                        Joiner.on(",").join(associationReferencesWithoutIdentifier), payload));
    }

    public AsmModelAdapter getModelAdapter() {
        return queryFactory.getModelAdapter();
    }


    public Object getTransferObjectValueAsEntityValueFromPayload(Payload payload, EAttribute transferAttribute, EAttribute entityAttribute) {
        final Object value = payload.get(transferAttribute.getName());

        Optional<Unit> transferAttributeUnit = getModelAdapter().getUnit(transferAttribute);
        Optional<Unit> entityAttributeUnit = getModelAdapter().getUnit(entityAttribute);

        if (value != null && transferAttributeUnit.isPresent() && entityAttributeUnit != null) {
            BigDecimal decimal = entityAttributeUnit.get().getRateDivisor()
                    .multiply(transferAttributeUnit.get().getRateDividend())
                    .multiply(new BigDecimal(value.toString()))
                    .divide(entityAttributeUnit.get().getRateDividend()
                                    .multiply(transferAttributeUnit.get().getRateDivisor()),
                            MEASURE_CONVERTING_SCALE,
                            RoundingMode.HALF_UP);

            if (getModelAdapter().isInteger(entityAttribute.getEAttributeType())) {
                return decimal.toBigInteger();
            } else {
                return decimal;
            }
        } else {
            return value;
        }
    }


}

