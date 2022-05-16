package hu.blackbelt.judo.runtime.core.dao.rdbms;

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
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
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

@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
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
    public void setup(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(getEsmModel(), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");
    }

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.dropDatabase();
    }

    @Test
    public void testRange(JudoRuntimeFixture runtimeFixture) {
        final EClass fruitType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Fruit").get();
        final EClass appleType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Apple").get();
        final EClass pearType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Pear").get();
        final EClass storeType = runtimeFixture.getAsmUtils().getClassByFQName(getModelName() + ".Store").get();
        final EReference allApplesReference = storeType.getEAllReferences().stream().filter(r -> "allApples".equals(r.getName())).findAny().get();
        final EReference galaApplesReference = storeType.getEAllReferences().stream().filter(r -> "galaApples".equals(r.getName())).findAny().get();
        final EReference galaFruitsReference = storeType.getEAllReferences().stream().filter(r -> "galaFruits".equals(r.getName())).findAny().get();
        final EReference galaAppleFruitsReference = storeType.getEAllReferences().stream().filter(r -> "galaAppleFruits".equals(r.getName())).findAny().get();

        final Payload galaApple = runtimeFixture.getDao().create(appleType, map(
                "variety", "GALA"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        log.debug("Saved GALA apple: {}", galaApple);
        final UUID galaAppleId = galaApple.getAs(UUID.class, runtimeFixture.getIdProvider().getName());

        final Payload pinkLadyApple = runtimeFixture.getDao().create(appleType, map(
                "variety", "PINK_LADY"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        log.debug("Saved PINK LADY apple: {}", pinkLadyApple);
        final UUID pinkLadyAppleId = pinkLadyApple.getAs(UUID.class, runtimeFixture.getIdProvider().getName());

        final Payload williamsPear = runtimeFixture.getDao().create(pearType, map(
                "variety", "WILLIAMS"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        log.debug("Saved WILLIAMS pear: {}", williamsPear);
        final UUID williamsPearId = williamsPear.getAs(UUID.class, runtimeFixture.getIdProvider().getName());

        final List<Payload> allApples = runtimeFixture.getDao().getAllReferencedInstancesOf(allApplesReference, fruitType);
        log.debug("All apples: {}", allApples);
        final Set<UUID> allAppleIds = allApples.stream().map(i -> i.getAs(UUID.class, runtimeFixture.getIdProvider().getName())).collect(Collectors.toSet());
        assertThat(allAppleIds, equalTo(ImmutableSet.of(galaAppleId, pinkLadyAppleId)));

        final List<Payload> galaApples = runtimeFixture.getDao().getAllReferencedInstancesOf(galaApplesReference, fruitType);
        log.debug("GALA apples: {}", galaApples);
        final Set<UUID> galaAppleIds = galaApples.stream().map(i -> i.getAs(UUID.class, runtimeFixture.getIdProvider().getName())).collect(Collectors.toSet());
        assertThat(galaAppleIds, equalTo(ImmutableSet.of(galaAppleId)));

        final List<Payload> galaFruits = runtimeFixture.getDao().getAllReferencedInstancesOf(galaFruitsReference, fruitType);
        log.debug("GALA fruits: {}", galaFruits);
        final Set<UUID> galaFruitIds = galaApples.stream().map(i -> i.getAs(UUID.class, runtimeFixture.getIdProvider().getName())).collect(Collectors.toSet());
        assertThat(galaFruitIds, equalTo(ImmutableSet.of(galaAppleId)));

        final List<Payload> galaAppleFruits = runtimeFixture.getDao().getAllReferencedInstancesOf(galaAppleFruitsReference, fruitType);
        log.debug("Fruits that are apples and variety is GALA: {}", galaAppleFruits);
        final Set<UUID> galaAppleFruitIds = galaApples.stream().map(i -> i.getAs(UUID.class, runtimeFixture.getIdProvider().getName())).collect(Collectors.toSet());
        assertThat(galaAppleFruitIds, equalTo(ImmutableSet.of(galaAppleId)));
    }
}
