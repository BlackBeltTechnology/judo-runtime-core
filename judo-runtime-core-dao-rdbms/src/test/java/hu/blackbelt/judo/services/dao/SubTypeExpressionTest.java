package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.meta.esm.structure.TransferObjectType;
import hu.blackbelt.judo.meta.esm.type.BooleanType;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import java.util.*;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class SubTypeExpressionTest {

    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getEsmModel() {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();

        final Model model = NamespaceBuilders.newModelBuilder().withName(getModelName()).build();

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
        fruit.setMapping(newMappingBuilder().withTarget(fruit).build());

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

        final TransferObjectType store = newTransferObjectTypeBuilder()
                .withName("Store")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("allApples")
                        .withTarget(fruit)
                        .withLower(0).withUpper(-1)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(getModelName() + "::Apple")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("galaApples")
                        .withTarget(fruit)
                        .withLower(0).withUpper(-1)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(getModelName() + "::Apple!filter(a | a.variety == 'GALA')")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("galaFruits")
                        .withTarget(fruit)
                        .withLower(0).withUpper(-1)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(getModelName() + "::Fruit!filter(f | f.variety == 'GALA')")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("galaAppleFruits")
                        .withTarget(fruit)
                        .withLower(0).withUpper(-1)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(getModelName() + "::Fruit!asCollection(" + getModelName() + "::Apple)!filter(a | a.variety == 'GALA')")
                        .build())
                .build();

        model.getElements().addAll(Arrays.asList(
                stringType, integerType, booleanType, fruit, apple, pear, store
        ));
        return model;
    }


    @BeforeEach
    public void setup(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    public void testRange(RdbmsDaoFixture daoFixture) {
        final EClass fruitType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Fruit").get();
        final EClass appleType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Apple").get();
        final EClass pearType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Pear").get();
        final EClass storeType = daoFixture.getAsmUtils().getClassByFQName(getModelName() + ".Store").get();
        final EReference allApplesReference = storeType.getEAllReferences().stream().filter(r -> "allApples".equals(r.getName())).findAny().get();
        final EReference galaApplesReference = storeType.getEAllReferences().stream().filter(r -> "galaApples".equals(r.getName())).findAny().get();
        final EReference galaFruitsReference = storeType.getEAllReferences().stream().filter(r -> "galaFruits".equals(r.getName())).findAny().get();
        final EReference galaAppleFruitsReference = storeType.getEAllReferences().stream().filter(r -> "galaAppleFruits".equals(r.getName())).findAny().get();

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

        final List<Payload> allApples = daoFixture.getDao().getAllReferencedInstancesOf(allApplesReference, fruitType);
        log.debug("All apples: {}", allApples);
        final Set<UUID> allAppleIds = allApples.stream().map(i -> i.getAs(UUID.class, daoFixture.getUuid().getName())).collect(Collectors.toSet());
        assertThat(allAppleIds, equalTo(ImmutableSet.of(galaAppleId, pinkLadyAppleId)));

        final List<Payload> galaApples = daoFixture.getDao().getAllReferencedInstancesOf(galaApplesReference, fruitType);
        log.debug("GALA apples: {}", galaApples);
        final Set<UUID> galaAppleIds = galaApples.stream().map(i -> i.getAs(UUID.class, daoFixture.getUuid().getName())).collect(Collectors.toSet());
        assertThat(galaAppleIds, equalTo(ImmutableSet.of(galaAppleId)));

        final List<Payload> galaFruits = daoFixture.getDao().getAllReferencedInstancesOf(galaFruitsReference, fruitType);
        log.debug("GALA fruits: {}", galaFruits);
        final Set<UUID> galaFruitIds = galaApples.stream().map(i -> i.getAs(UUID.class, daoFixture.getUuid().getName())).collect(Collectors.toSet());
        assertThat(galaFruitIds, equalTo(ImmutableSet.of(galaAppleId)));

        final List<Payload> galaAppleFruits = daoFixture.getDao().getAllReferencedInstancesOf(galaAppleFruitsReference, fruitType);
        log.debug("Fruits that are apples and variety is GALA: {}", galaAppleFruits);
        final Set<UUID> galaAppleFruitIds = galaApples.stream().map(i -> i.getAs(UUID.class, daoFixture.getUuid().getName())).collect(Collectors.toSet());
        assertThat(galaAppleFruitIds, equalTo(ImmutableSet.of(galaAppleId)));
    }
}
