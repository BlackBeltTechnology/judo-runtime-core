package hu.blackbelt.judo.runtime.core.dao.rdbms;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.structure.*;
import hu.blackbelt.judo.meta.esm.type.BooleanType;
import hu.blackbelt.judo.meta.esm.type.EnumerationType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceSingetonExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class CastTest {

    public static final String MODEL_NAME = "C";

    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getEsmModel() {

        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();

        final Model model = NamespaceBuilders.newModelBuilder().withName(getModelName()).build();

        final DataMember nameOfFruit = newDataMemberBuilder()
                .withName("name")
                .withDataType(stringType)
                .withRequired(true)
                .withMemberType(MemberType.STORED)
                .build();
        final DataMember colorCodeOfFruit = newDataMemberBuilder()
                .withName("colorCode")
                .withDataType(stringType)
                .withRequired(false)
                .withMemberType(MemberType.STORED)
                .build();
        final EntityType fruit = newEntityTypeBuilder()
                .withName("Fruit")
                .withAbstract_(true)
                .withAttributes(nameOfFruit)
                .withAttributes(colorCodeOfFruit)
                .withAttributes(newDataMemberBuilder()
                        .withName("appleKind")
                        .withDataType(booleanType)
                        .withRequired(true)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self!kindOf(" + getModelName() + "::Apple)")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("appleKind2")
                        .withDataType(booleanType)
                        .withRequired(true)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self!asType(" + getModelName() + "::Apple)!isDefined()")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("appleType")
                        .withDataType(booleanType)
                        .withRequired(true)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self!typeOf(" + getModelName() + "::Apple)")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("pearKind")
                        .withDataType(booleanType)
                        .withRequired(true)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self!kindOf(" + getModelName() + "::Pear)")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("pearKind2")
                        .withDataType(booleanType)
                        .withRequired(true)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self!asType(" + getModelName() + "::Pear)!isDefined()")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("pearType")
                        .withDataType(booleanType)
                        .withRequired(true)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self!typeOf(" + getModelName() + "::Pear)")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("fruitKind")
                        .withDataType(booleanType)
                        .withRequired(true)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self!kindOf(" + getModelName() + "::Fruit)")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("fruitType")
                        .withDataType(booleanType)
                        .withRequired(true)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self!typeOf(" + getModelName() + "::Fruit)")
                        .build())
                .build();
        fruit.setMapping(newMappingBuilder().withTarget(fruit).build());

        final EnumerationType appleVariety = newEnumerationTypeBuilder()
                .withName("AppleVariety")
                .withMembers(newEnumerationMemberBuilder()
                        .withName("PINK_LADY")
                        .withOrdinal(0)
                        .build())
                .withMembers(newEnumerationMemberBuilder()
                        .withName("GALA")
                        .withOrdinal(1)
                        .build())
                .withMembers(newEnumerationMemberBuilder()
                        .withName("RED_DELICIOUS")
                        .withOrdinal(2)
                        .build())
                .withMembers(newEnumerationMemberBuilder()
                        .withName("JONAGOLD")
                        .withOrdinal(3)
                        .build())
                .build();

        final DataMember varietyOfApple = newDataMemberBuilder()
                .withName("variety")
                .withDataType(appleVariety)
                .withRequired(true)
                .withMemberType(MemberType.STORED)
                .build();
        final EntityType apple = newEntityTypeBuilder()
                .withName("Apple")
                .withGeneralizations(newGeneralizationBuilder()
                        .withTarget(fruit)
                        .build())
                .withAttributes(varietyOfApple)
                .build();
        apple.setMapping(newMappingBuilder().withTarget(apple).build());

        final EnumerationType pearVariety = newEnumerationTypeBuilder()
                .withName("PearVariety")
                .withMembers(newEnumerationMemberBuilder()
                        .withName("WILLIAMS")
                        .withOrdinal(0)
                        .build())
                .withMembers(newEnumerationMemberBuilder()
                        .withName("ANJOU")
                        .withOrdinal(1)
                        .build())
                .withMembers(newEnumerationMemberBuilder()
                        .withName("BOSC")
                        .withOrdinal(2)
                        .build())
                .build();

        final EntityType pear = newEntityTypeBuilder()
                .withName("Pear")
                .withGeneralizations(newGeneralizationBuilder()
                        .withTarget(fruit)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("variety")
                        .withDataType(pearVariety)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        pear.setMapping(newMappingBuilder().withTarget(pear).build());

        final TransferObjectType pinkLadyApple = newTransferObjectTypeBuilder()
                .withMapping(newMappingBuilder()
                        .withTarget(apple)
                        .withFilter("self.variety == " + getModelName() + "::AppleVariety#PINK_LADY")
                        .build())
                .withName("PinkLadyApple")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.MAPPED)
                        .withBinding(nameOfFruit)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("color")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.MAPPED)
                        .withBinding(colorCodeOfFruit)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("variety")
                        .withDataType(appleVariety)
                        .withRequired(true)
                        .withMemberType(MemberType.MAPPED)
                        .withBinding(varietyOfApple)
                        .build())
                .build();

        final TransferObjectType store = newTransferObjectTypeBuilder()
                .withName("Store")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("allGalaApples")
                        .withTarget(apple)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Fruit!asCollection(" + getModelName() + "::Apple)!filter(a | a.variety == " + getModelName() + "::AppleVariety#GALA)")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("allWilliamsPears")
                        .withTarget(fruit)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Fruit!filter(f | f!asType(" + getModelName() + "::Pear).variety == " + getModelName() + "::PearVariety#WILLIAMS)")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("pinkLadyApples")
                        .withTarget(pinkLadyApple)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Apple")
                        .build())
                .build();

        model.getElements().addAll(Arrays.asList(
                stringType, booleanType, appleVariety, pearVariety, fruit, apple, pear, pinkLadyApple, store
        ));
        return model;
    }

    RdbmsDaoFixture daoFixture;

    @BeforeEach
    public void setup(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
        this.daoFixture = daoFixture;
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }


    private enum AppleVariety {
        PINK_LADY, GALA, RED_DELICIOUS, JONAGOLD
    }

    private enum PearVariety {
        WILLIAMS, ANJOU, BOSC
    }

    private UUID createApple(final AppleVariety appleVariety) {
        final EClass appleType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Apple").get();

        final Payload apple = daoFixture.getDao().create(appleType, map(
                "name", "Apple",
                "variety", appleVariety.ordinal()
                ),
                DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build()
        );

        final UUID appleId = apple.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        log.debug("Apple {} created with ID: {}", appleVariety, appleId);

        final Optional<Payload> metadata = daoFixture.getDao().getMetadata(appleType, appleId);
        assertTrue(metadata.isPresent());
        assertThat(metadata.get().getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName()), equalTo(appleId));
        assertThat(metadata.get().getAs(String.class, StatementExecutor.ENTITY_TYPE_MAP_KEY), equalTo(AsmUtils.getClassifierFQName(daoFixture.getAsmUtils().getMappedEntityType(appleType).get())));

        return appleId;
    }

    private UUID createPear(final PearVariety pearVariety) {
        final EClass pearType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Pear").get();

        final Payload pear = daoFixture.getDao().create(pearType, map(
                "name", "Pear",
                "variety", pearVariety.ordinal()
                ),
                DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build()
        );

        final UUID pearId = pear.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        log.debug("Pear {} created with ID: {}", pearVariety, pearId);

        final Optional<Payload> metadata = daoFixture.getDao().getMetadata(pearType, pearId);
        assertTrue(metadata.isPresent());
        assertThat(metadata.get().getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName()), equalTo(pearId));
        assertThat(metadata.get().getAs(String.class, StatementExecutor.ENTITY_TYPE_MAP_KEY), equalTo(AsmUtils.getClassifierFQName(daoFixture.getAsmUtils().getMappedEntityType(pearType).get())));

        return pearId;
    }

    private static final Map<AppleVariety, Integer> numberOfApplesByVarieties = ImmutableMap.of(
            AppleVariety.PINK_LADY, 2,
            AppleVariety.GALA, 4,
            AppleVariety.RED_DELICIOUS, 3,
            AppleVariety.JONAGOLD, 1
    );

    private static final Map<PearVariety, Integer> numberOfPearsByVarieties = ImmutableMap.of(
            PearVariety.WILLIAMS, 2,
            PearVariety.ANJOU, 4,
            PearVariety.BOSC, 5
    );

    @Test
    public void testCasting() {
        daoFixture.beginTransaction();
        final EClass fruitType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Fruit").get();
        final EClass appleType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Apple").get();
        final EClass pinkLadyAppleType = daoFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".PinkLadyApple").get();
        final EClass storeType = daoFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".Store").get();
        final EReference allGalaApplesRelation = storeType.getEAllReferences().stream().filter(r -> "allGalaApples".equals(r.getName())).findAny().get();
        final EReference allWilliamsPearRelation = storeType.getEAllReferences().stream().filter(r -> "allWilliamsPears".equals(r.getName())).findAny().get();
        final EReference pinkLadyApplesRelation = storeType.getEAllReferences().stream().filter(r -> "pinkLadyApples".equals(r.getName())).findAny().get();

        final Multimap<AppleVariety, UUID> applesByVarieties = HashMultimap.create();
        final Multimap<PearVariety, UUID> pearsByVarieties = HashMultimap.create();

        numberOfApplesByVarieties.forEach((variety, count) -> {
            for (int i = 0; i < count; i++) {
                applesByVarieties.put(variety, createApple(variety));
            }
        });

        final Optional<Payload> notExistingAppleMetadata = daoFixture.getDao().getMetadata(appleType, UUID.randomUUID());
        assertFalse(notExistingAppleMetadata.isPresent());

        numberOfPearsByVarieties.forEach((variety, count) -> {
            for (int i = 0; i < count; i++) {
                pearsByVarieties.put(variety, createPear(variety));
            }
        });

        final List<Payload> allGalaApples = daoFixture.getDao().getAllReferencedInstancesOf(allGalaApplesRelation, appleType);
        log.debug("All Gala apples: {}", allGalaApples);
        allGalaApples.forEach(galaApple -> {
            assertThat(galaApple.getAs(Boolean.class, "appleType"), equalTo(Boolean.TRUE));
            assertThat(galaApple.getAs(Boolean.class, "appleKind"), equalTo(Boolean.TRUE));
            assertThat(galaApple.getAs(Boolean.class, "appleKind2"), equalTo(Boolean.TRUE));
            assertThat(galaApple.getAs(Boolean.class, "fruitType"), equalTo(Boolean.FALSE));
            assertThat(galaApple.getAs(Boolean.class, "fruitKind"), equalTo(Boolean.TRUE));
            assertThat(galaApple.getAs(Boolean.class, "pearType"), equalTo(Boolean.FALSE));
            assertThat(galaApple.getAs(Boolean.class, "pearKind"), equalTo(Boolean.FALSE));
            assertThat(galaApple.getAs(Boolean.class, "pearKind2"), equalTo(Boolean.FALSE));

            assertThat(galaApple.getAs(Integer.class, "variety"), equalTo(AppleVariety.GALA.ordinal()));
        });
        final Set<UUID> allGalaApplesIds = allGalaApples.stream()
                .map(c -> c.getAs(UUID.class, daoFixture.getUuid().getName()))
                .collect(Collectors.toSet());
        assertThat(allGalaApplesIds, equalTo(applesByVarieties.get(AppleVariety.GALA)));

        final List<Payload> allWilliamsPears = daoFixture.getDao().getAllReferencedInstancesOf(allWilliamsPearRelation, fruitType);
        log.debug("All Williams pears: {}", allWilliamsPears);
        allWilliamsPears.forEach(williamsPear -> {
            assertThat(williamsPear.getAs(Boolean.class, "appleType"), equalTo(Boolean.FALSE));
            assertThat(williamsPear.getAs(Boolean.class, "appleKind"), equalTo(Boolean.FALSE));
            assertThat(williamsPear.getAs(Boolean.class, "appleKind2"), equalTo(Boolean.FALSE));
            assertThat(williamsPear.getAs(Boolean.class, "fruitType"), equalTo(Boolean.FALSE));
            assertThat(williamsPear.getAs(Boolean.class, "fruitKind"), equalTo(Boolean.TRUE));
            assertThat(williamsPear.getAs(Boolean.class, "pearType"), equalTo(Boolean.TRUE));
            assertThat(williamsPear.getAs(Boolean.class, "pearKind"), equalTo(Boolean.TRUE));
            assertThat(williamsPear.getAs(Boolean.class, "pearKind2"), equalTo(Boolean.TRUE));

            assertThat(williamsPear.getAs(Integer.class, "variety"), nullValue());
        });
        final Set<UUID> allWilliamsPearIds = allWilliamsPears.stream()
                .map(c -> c.getAs(UUID.class, daoFixture.getUuid().getName()))
                .collect(Collectors.toSet());
        assertThat(allWilliamsPearIds, equalTo(pearsByVarieties.get(PearVariety.WILLIAMS)));

        final List<Payload> allPinkLadyApples = daoFixture.getDao().getAllOf(pinkLadyAppleType);
        log.debug("All Pink Lady apples: {}", allPinkLadyApples);
        final Set<UUID> allPinkLadyAppleIds = allPinkLadyApples.stream()
                .map(c -> c.getAs(UUID.class, daoFixture.getUuid().getName()))
                .collect(Collectors.toSet());
        assertThat(allPinkLadyAppleIds, equalTo(applesByVarieties.get(AppleVariety.PINK_LADY)));

        final Payload newPinkLadyApple = daoFixture.getDao().create(pinkLadyAppleType, map(
                "name", "Apple",
                "variety", AppleVariety.PINK_LADY.ordinal(),
                "color", "red"
                ),
                DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build()
        );

        final UUID newPinkLadyAppleId = newPinkLadyApple.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());
        log.debug("New Pink Lady Apple created with ID: {}", newPinkLadyAppleId);
        applesByVarieties.put(AppleVariety.PINK_LADY, newPinkLadyAppleId);

        final List<Payload> pinkLadyApples = daoFixture.getDao().getAllReferencedInstancesOf(pinkLadyApplesRelation, pinkLadyAppleType);
        log.debug("Pink Lady apples: {}", pinkLadyApples);

        assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao().create(pinkLadyAppleType, map(
                "name", "Apple",
                "variety", AppleVariety.GALA.ordinal()
                ),
                DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build()
        ));

        final Payload updatedPinkLadyApple = daoFixture.getDao().update(pinkLadyAppleType, map(
                daoFixture.getIdProvider().getName(), newPinkLadyAppleId,
                "name", "Apple",
                "variety", AppleVariety.PINK_LADY.ordinal(),
                "color", "pink"
                ), null
        );
        log.debug("Updated Pink Lady apple: {}", updatedPinkLadyApple);
        assertThat(updatedPinkLadyApple.getAs(String.class, "color"), equalTo("pink"));

        daoFixture.commitTransaction();

        daoFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao().update(pinkLadyAppleType, map(
                daoFixture.getIdProvider().getName(), newPinkLadyAppleId,
                "name", "Apple",
                "variety", AppleVariety.GALA.ordinal(),
                "color", "pink"
                ), null
        ));
        daoFixture.rollbackTransaction();


        final Optional<Payload> unchangedPinkLadyApple = daoFixture.getDao().getByIdentifier(pinkLadyAppleType, newPinkLadyAppleId);
        assertTrue(unchangedPinkLadyApple.isPresent());
        assertThat(unchangedPinkLadyApple.get(), equalTo(updatedPinkLadyApple));
    }
}
