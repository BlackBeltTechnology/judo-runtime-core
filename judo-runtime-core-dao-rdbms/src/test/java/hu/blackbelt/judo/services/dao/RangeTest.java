package hu.blackbelt.judo.services.dao;

import com.google.common.collect.ImmutableList;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.accesspoint.ActorType;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.NamespaceElement;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.ModelBuilder;
import hu.blackbelt.judo.meta.esm.operation.OperationType;
import hu.blackbelt.judo.meta.esm.operation.util.builder.OperationBuilders;
import hu.blackbelt.judo.meta.esm.structure.*;
import hu.blackbelt.judo.meta.esm.structure.util.builder.EntityTypeBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.MappingBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.OneWayRelationMemberBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.TwoWayRelationMemberBuilder;
import hu.blackbelt.judo.meta.esm.type.BooleanType;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.meta.esm.ui.util.builder.UiBuilders;
import hu.blackbelt.judo.services.core.exception.FeedbackItem;
import hu.blackbelt.judo.services.core.exception.ValidationException;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.hamcrest.collection.IsCollectionWithSize;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import java.util.*;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.esm.accesspoint.util.builder.AccesspointBuilders.newAccessBuilder;
import static hu.blackbelt.judo.meta.esm.accesspoint.util.builder.AccesspointBuilders.newActorTypeBuilder;
import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.structure.MemberType.DERIVED;
import static hu.blackbelt.judo.meta.esm.structure.MemberType.STORED;
import static hu.blackbelt.judo.meta.esm.structure.RelationKind.AGGREGATION;
import static hu.blackbelt.judo.meta.esm.structure.RelationKind.ASSOCIATION;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class RangeTest {

    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getFruitEsmModel() {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();

        final Model model = newModelBuilder().withName(getModelName()).build();

        final EntityType fruit = newEntityTypeBuilder()
                .withName("Fruit")
                .withAbstract_(true)
                .withAttributes(newDataMemberBuilder()
                        .withName("variety")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(fruit)
                .withMapping(newMappingBuilder().withTarget(fruit).build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("range")
                        .withTarget(fruit)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Apple!filter(a | a.variety == 'GALA')")
                        .build())
                .build();

        final EntityType apple = newEntityTypeBuilder()
                .withName("Apple")
                .withGeneralizations(newGeneralizationBuilder().withTarget(fruit).build())
                .build();
        apple.setMapping(newMappingBuilder().withTarget(apple).build());

        final EntityType pear = newEntityTypeBuilder()
                .withName("Pear")
                .withGeneralizations(newGeneralizationBuilder().withTarget(fruit).build())
                .build();
        pear.setMapping(newMappingBuilder().withTarget(pear).build());

        final EntityType basket = newEntityTypeBuilder()
                .withName("Basket")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("breakfast")
                        .withTarget(fruit)
                        .withLower(0).withUpper(1)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.STORED)
                        .withRangeType(RangeType.DERIVED)
                        .withRangeExpression(getModelName() + "::Apple")
                        .build())
                .build();
        basket.setMapping(newMappingBuilder().withTarget(basket).build());

        final TransferObjectType shelf = newTransferObjectTypeBuilder()
                .withName("Shelf")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("fruits")
                        .withTarget(fruit)
                        .withLower(0).withUpper(-1)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.TRANSIENT)
                        .withRangeType(RangeType.DERIVED)
                        .withRangeExpression(getModelName() + "::Pear")
                        .build())
                .withForm(UiBuilders.newTransferObjectFormBuilder().withName("form").build())
                .withTable(UiBuilders.newTransferObjectTableBuilder().withName("table").build())
                .build();

        final EntityType storeFront = newEntityTypeBuilder()
                .withName("StoreFront")
                .withOperations(OperationBuilders.newOperationBuilder().withName("decorateShelf")
                        .withInput(OperationBuilders.newParameterBuilder().withName("input").withTarget(shelf).withLower(1).withUpper(1).build())
                        .withOperationType(OperationType.INSTANCE)
                        .withBody("//todo")
                        .withBinding("decorateShelf")
                        .build())
                .build();
        storeFront.setMapping(newMappingBuilder().withTarget(storeFront).build());

        final TransferObjectType storeFrontDTO = newTransferObjectTypeBuilder()
                .withName("Window")
                .withOperations(OperationBuilders.newOperationBuilder().withName("decorateShelf")
                        .withInput(OperationBuilders.newParameterBuilder().withName("input").withTarget(shelf).withLower(1).withUpper(1).build())
                        .withOperationType(OperationType.MAPPED)
                        .withBinding("decorateShelf")
                        .build())
                .build();
        storeFrontDTO.setMapping(newMappingBuilder().withTarget(storeFront).build());

        ActorType actor = newActorTypeBuilder().withName("Actor").withAccesses(
                newAccessBuilder().withName("baskets").withTarget(basket).withUpper(-1).withTargetDefinedCRUD(false).withCreateable(true).withUpdateable(true).withDeleteable(true).build(),
                newAccessBuilder().withName("windows").withTarget(storeFrontDTO).withUpper(-1).withTargetDefinedCRUD(false).withCreateable(false).withUpdateable(false).withDeleteable(false).build()
        ).build();

        model.getElements().addAll(Arrays.asList(
                stringType, integerType, booleanType, fruit, apple, pear, basket, shelf, actor, storeFront, storeFrontDTO
        ));
        return model;
    }


    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    public void testRange(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getFruitEsmModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass appleType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Apple").get();
        final EClass pearType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Pear").get();
        final EClass basketType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Basket").get();
        final EClass fruitType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Fruit").get();
        final EClass shelfType = daoFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".Shelf").get();
        final EReference breakfastInBasket = basketType.getEAllReferences().stream().filter(r -> "breakfast".equals(r.getName())).findAny().get();
        final EReference rangeInFruit = fruitType.getEAllReferences().stream().filter(r -> "range".equals(r.getName())).findAny().get();
        final EReference fruitsOnShelf = shelfType.getEAllReferences().stream().filter(r -> "fruits".equals(r.getName())).findAny().get();

        final Payload galaApple = daoFixture.getDao().create(appleType, map(
                "variety", "GALA"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        log.debug("Saved GALA apple: {}", galaApple);
        final UUID galaAppleId = galaApple.getAs(UUID.class, daoFixture.getUuid().getName());

        final Payload pinkLadyApple = daoFixture.getDao().create(appleType, map(
                "variety", "PINK_LADY"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        log.debug("Saved PINK LADY apple: {}", pinkLadyApple);
        final UUID pinkLadyAppleId = pinkLadyApple.getAs(UUID.class, daoFixture.getUuid().getName());

        final Payload williamsPear = daoFixture.getDao().create(pearType, map(
                "variety", "WILLIAMS"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        log.debug("Saved WILLIAMS pear: {}", williamsPear);
        final UUID williamsPearId = williamsPear.getAs(UUID.class, daoFixture.getUuid().getName());

        final Collection<Payload> rangeOfBreakfast = daoFixture.getDao().getRangeOf(breakfastInBasket, null, DAO.QueryCustomizer.<UUID>builder()
                .withoutFeatures(true)
                .build());
        log.debug("Range of breakfast: {}", rangeOfBreakfast);
        final Set<UUID> rangeOfBreakfastIds = rangeOfBreakfast.stream().map(i -> i.getAs(UUID.class, daoFixture.getUuid().getName())).collect(Collectors.toSet());
        assertThat(rangeOfBreakfastIds, equalTo(ImmutableSet.of(galaAppleId, pinkLadyAppleId)));

        final UUID firstRangeOption = rangeOfBreakfast.iterator().next().getAs(UUID.class, daoFixture.getUuid().getName());

        final Payload basket = daoFixture.getDao().create(basketType, map(
                "breakfast", map(daoFixture.getIdProvider().getName(), firstRangeOption)
        ), null);
        log.debug("Saved basket: {}", basket);
        final UUID basketId = basket.getAs(UUID.class, daoFixture.getUuid().getName());
        assertThat(basket.getAsPayload("breakfast").getAs(UUID.class, daoFixture.getUuid().getName()), equalTo(firstRangeOption));

        List<Payload> range = daoFixture.getDao().getAllReferencedInstancesOf(rangeInFruit, rangeInFruit.getEReferenceType());
        log.debug("Range: {}", range);
        final Set<UUID> rangeIds = range.stream().map(i -> i.getAs(UUID.class, daoFixture.getUuid().getName())).collect(Collectors.toSet());
        assertThat(rangeIds, equalTo(ImmutableSet.of(galaAppleId)));

        final Collection<Payload> rangeOfFruits = daoFixture.getDao().getRangeOf(fruitsOnShelf, null, null);
        log.debug("Range of fruits: {}", rangeOfFruits);
        final Set<UUID> rangeOfFruitsIds = rangeOfFruits.stream().map(i -> i.getAs(UUID.class, daoFixture.getUuid().getName())).collect(Collectors.toSet());
        assertThat(rangeOfFruitsIds, equalTo(ImmutableSet.of(williamsPearId)));
    }

    @Test
    public void testDeletedReference(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getFruitEsmModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass appleType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Apple").get();
        final EClass basketType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Basket").get();

        final Payload galaApple = daoFixture.getDao().create(appleType, map(
                "variety", "GALA"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        log.debug("Saved GALA apple: {}", galaApple);
        final UUID galaAppleId = galaApple.getAs(UUID.class, daoFixture.getUuid().getName());

        daoFixture.getDao().delete(appleType, galaAppleId);

        try {
            final Payload basket = daoFixture.getDao().create(basketType, map(
                    "breakfast", map(daoFixture.getIdProvider().getName(), galaAppleId)
            ), null);
            fail();
        } catch (ValidationException ex) {
            final Collection<FeedbackItem> feedbackItems = ex.getFeedbackItems();
            assertThat(feedbackItems, hasSize(1));
            final FeedbackItem feedbackItem = feedbackItems.iterator().next();
            assertThat(feedbackItem.getCode(), equalTo("ENTITY_NOT_FOUND"));
            assertThat(feedbackItem.getLevel(), equalTo(FeedbackItem.Level.ERROR));
            assertThat(feedbackItem.getDetails().get(daoFixture.getIdProvider().getName()), equalTo(galaAppleId));
        }
    }

    @Test
    public void testRangeOfContainments(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();

        final EntityType wheel = newEntityTypeBuilder()
                .withName("Wheel")
                .withAttributes(newDataMemberBuilder()
                        .withName("produced")
                        .withDataType(integerType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(wheel).withMapping(newMappingBuilder().withTarget(wheel).build()).build();

        final EntityType car = newEntityTypeBuilder()
                .withName("Car")
                .withAttributes(newDataMemberBuilder()
                        .withName("licensePlate")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("color")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(car)
                .withMapping(newMappingBuilder().withTarget(car).build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("wheels")
                        .withTarget(wheel)
                        .withLower(4).withUpper(5)
                        .withMemberType(MemberType.STORED)
                        .withRelationKind(RelationKind.COMPOSITION)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("spareWheel")
                        .withTarget(wheel)
                        .withLower(0).withUpper(1)
                        .withMemberType(MemberType.STORED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withRangeType(RangeType.DERIVED)
                        .withRangeExpression("self.wheels")
                        .build())
                .build();

        final ActorType actor = newActorTypeBuilder()
                .withName("Actor")
                .withAccesses(newAccessBuilder()
                        .withName("cars")
                        .withLower(0).withUpper(-1)
                        .withTarget(car)
                        .withCreateable(true).withUpdateable(true).withDeleteable(true)
                        .build())
                .build();

        final Model model = newModelBuilder()
                .withName(getModelName())
                .withElements(stringType, integerType, booleanType, car, wheel, actor)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass carType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Car").get();
        final EReference spareWheelOfCarType = daoFixture.getAsmUtils().resolveReference(DTO_PACKAGE + ".Car#spareWheel").get();

        final Payload car1 = daoFixture.getDao().create(carType, map(
                "licensePlate", "ABC123",
                "color", "red",
                "wheels", Arrays.asList(
                        map("produced", 2018),
                        map("produced", 2019),
                        map("produced", 2020),
                        map("produced", 2021),
                        map("produced", 2015)
                )
        ), null);
        log.debug("Saved car ABC-123: {}", car1);

        car1.put("color", "green");
        final Collection<Payload> rangeOfShapeWheel1 = daoFixture.getDao().getRangeOf(spareWheelOfCarType, car1, null);
        log.debug("Range of spare wheel for car ABC-123: {}", rangeOfShapeWheel1);
        assertThat(rangeOfShapeWheel1.stream().map(w -> w.get("produced")).collect(Collectors.toSet()), equalTo(ImmutableSet.of(2018, 2019, 2020, 2021, 2015)));

        final Collection<Payload> rangeOfShapeWheel2 = daoFixture.getDao().getRangeOf(spareWheelOfCarType, map(
                "color", "black",
                "wheels", Arrays.asList(
                        map("produced", 2005),
                        map("produced", 2006),
                        map("produced", 2007),
                        map("produced", 2008),
                        map("produced", 1995)
                )
        ), null);
        log.debug("Range of spare wheel for new car: {}", rangeOfShapeWheel2);
        assertThat(rangeOfShapeWheel2.stream().map(w -> w.get("produced")).collect(Collectors.toSet()), equalTo(ImmutableSet.of(2005, 2006, 2007, 2008, 1995)));
    }

    @Test
    public void testRangeOnTwoWayRelationExpression(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final Set<NamespaceElement> namespaceElements = new HashSet<>();
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();

        namespaceElements.add(stringType);

        final EntityType planet = EntityTypeBuilder.create()
                .withName("Planet")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("planetNameOverCreature")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(DERIVED)
                        .withGetterExpression("self.creatures!any().planet.name")
                        .build())
                .build();
        planet.setMapping(MappingBuilder.create().withTarget(planet).build());
        namespaceElements.add(planet);

        final EntityType creature = EntityTypeBuilder.create()
                .withName("Creature")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("planetName")
                        .withDataType(stringType)
                        .withMemberType(DERIVED)
                        .withGetterExpression("self.planet.name")
                        .build())
                .build();
        creature.setMapping(MappingBuilder.create().withTarget(creature).build());
        namespaceElements.add(creature);

        final TwoWayRelationMember toPlanet = TwoWayRelationMemberBuilder.create()
                .withName("planet")
                .withTarget(planet)
                .withLower(0)
                .withUpper(1)
                .withMemberType(STORED)
                .withRelationKind(ASSOCIATION)
                .build();

        final TwoWayRelationMember toCreatures = TwoWayRelationMemberBuilder.create()
                .withName("creatures")
                .withTarget(creature)
                .withLower(0)
                .withUpper(-1)
                .withMemberType(STORED)
                .withRelationKind(ASSOCIATION)
                .build();

        final OneWayRelationMember toVisitors = OneWayRelationMemberBuilder.create()
                .withName("visitors")
                .withTarget(creature)
                .withLower(0)
                .withUpper(-1)
                .withMemberType(STORED)
                .withRelationKind(ASSOCIATION)
                .withRangeType(RangeType.DERIVED)
                .withRangeExpression("M::Creature!filter(c | c.planet!isUndefined())")
                .build();

        final OneWayRelationMember availableVisitors = OneWayRelationMemberBuilder.create()
                .withName("availableVisitors")
                .withTarget(creature)
                .withLower(0)
                .withUpper(-1)
                .withMemberType(DERIVED)
                .withRelationKind(AGGREGATION)
                .withGetterExpression("M::Creature!filter(c | c.planet.name == \"Venus\")")
//                .withGetterExpression("M::Creature!filter(c | c.planet!isUndefined())")
                .build();

        toPlanet.setPartner(toCreatures);
        toCreatures.setPartner(toPlanet);

        planet.getRelations().add(toCreatures);
        creature.getRelations().add(toPlanet);
        planet.getRelations().add(toVisitors);
        planet.getRelations().add(availableVisitors);

        final ActorType actor = newActorTypeBuilder()
                .withName("Actor")
                .withAccesses(newAccessBuilder()
                        .withName("planets")
                        .withLower(0).withUpper(-1)
                        .withTarget(planet)
                        .withCreateable(true).withUpdateable(true).withDeleteable(true)
                        .build())
                .build();
        namespaceElements.add(actor);

        final Model model = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        final EClass planetEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Planet").get();
        final EClass creatureEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Creature").get();
        final EReference referenceToPlanet =  daoFixture.getAsmUtils().resolveReference(DTO_PACKAGE + ".Creature#planet").get();
        final EReference referenceToCreature =  daoFixture.getAsmUtils().resolveReference(DTO_PACKAGE + ".Planet#creatures").get();
        final EReference referenceSelectableCreature =  daoFixture.getAsmUtils().resolveReference(DTO_PACKAGE + ".Planet#visitors").get();
        final EReference referenceAvailableCreature =  daoFixture.getAsmUtils().resolveReference(DTO_PACKAGE + ".Planet#availableVisitors").get();

        final Payload planetPayload = daoFixture.getDao().create(
                planetEClass, Payload.map("name", "Venus"), null);
        log.debug("{} created with payload: {}", planetEClass.getName(), planetPayload);

        final Payload creaturePayload = daoFixture.getDao().create(
                creatureEClass, Payload.map("name", "Alien"), null);
        log.debug("{} created with payload: {}", creatureEClass.getName(), creaturePayload);

        final Payload creatureWithoutPlanetPayload = daoFixture.getDao().create(
                creatureEClass, Payload.map("name", "Visitor"), null);
        log.debug("{} created with payload: {}", creatureEClass.getName(), creatureWithoutPlanetPayload);

        daoFixture.getDao().addReferences(referenceToCreature,
                planetPayload.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName()),
                ImmutableList.of(creaturePayload.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())));

        Collection<Payload> availableCreatures = daoFixture.getDao().getByIdentifier(planetEClass,
                planetPayload.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).get().getAs(Collection.class, "availableVisitors");

        Collection<Payload> selectableCreatures = daoFixture.getDao().getRangeOf(referenceSelectableCreature, creaturePayload,
                null);


        assertThat(selectableCreatures.stream().map(w -> w.get("name")).collect(Collectors.toSet()), equalTo(ImmutableSet.of("Visitor")));
        assertThat(availableCreatures.stream().map(w -> w.get("name")).collect(Collectors.toSet()), equalTo(ImmutableSet.of("Alien")));

    }

}
