package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.esm.accesspoint.AccessType;
import hu.blackbelt.judo.meta.esm.accesspoint.ActorType;
import hu.blackbelt.judo.meta.esm.accesspoint.util.builder.AccesspointBuilders;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.structure.*;
import hu.blackbelt.judo.meta.esm.type.BooleanType;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.esm.accesspoint.util.builder.AccesspointBuilders.newAccessBuilder;
import static hu.blackbelt.judo.meta.esm.accesspoint.util.builder.AccesspointBuilders.newActorTypeBuilder;
import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newPackageBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class StaticDataTest {

    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected String getModelName() {
        return MODEL_NAME;
    }

    @AfterEach
    public void teardown(final RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    void testSimpleFilter(final RdbmsDaoFixture daoFixture, final RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();
        final NumericType doubleType = newNumericTypeBuilder().withName("Double").withPrecision(15).withScale(4).build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();

        final DataMember nameOfItem = newDataMemberBuilder()
                .withName("name")
                .withMemberType(MemberType.STORED)
                .withDataType(stringType)
                .withRequired(true)
                .withIdentifier(true)
                .build();
        final EntityType item = newEntityTypeBuilder()
                .withName("Item")
                .withAttributes(nameOfItem)
                .build();
        useEntityType(item)
                .withMapping(newMappingBuilder().withTarget(item).build())
                .build();

        final TransferObjectType tester = newTransferObjectTypeBuilder()
                .withName("Tester")
                .withAttributes(newDataMemberBuilder()
                        .withName("simpleArithmeticResult")
                        .withMemberType(MemberType.DERIVED)
                        .withDataType(integerType)
                        .withGetterExpression("1 + 2")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("count")
                        .withMemberType(MemberType.DERIVED)
                        .withDataType(integerType)
                        .withGetterExpression(MODEL_NAME + "::" + "entities::Item!count()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("itemA")
                        .withLower(0).withUpper(1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!filter(i | i.name == 'a')!any()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("itemAny")
                        .withLower(0).withUpper(1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!filter(i | i.name!length() == 1)!sort(j | j.name DESC)!any()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("itemMax")
                        .withLower(0).withUpper(1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!filter(i | i.name!length() == 1)!head(j | j.name DESC)")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("anyItem")
                        .withLower(0).withUpper(1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!any()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("itemX")
                        .withLower(0).withUpper(1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!filter(i | i.name == 'x')!any()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("items")
                        .withLower(0).withUpper(-1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!sort(i | i.name)")
                        .build())
                .build();

        final TransferObjectType unmapped1 = newTransferObjectTypeBuilder()
                .withName("Unmapped1")
                .withAttributes(newDataMemberBuilder()
                        .withName("attribute1")
                        .withMemberType(MemberType.DERIVED)
                        .withDataType(integerType)
                        .withGetterExpression("1*2+3*4")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("count")
                        .withMemberType(MemberType.DERIVED)
                        .withDataType(integerType)
                        .withGetterExpression(MODEL_NAME + "::" + "entities::Item!count()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("itemAny")
                        .withLower(0).withUpper(1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!filter(i | i.name!length() == 1)!sort(j | j.name DESC)!any()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("itemMax")
                        .withLower(0).withUpper(1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!filter(i | i.name!length() == 1)!tail(j | j.name)")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("anyItem")
                        .withLower(0).withUpper(1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!any()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("itemA")
                        .withLower(0).withUpper(1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!filter(i | i.name == 'a')!any()")
                        .build())
                .build();
        final TransferObjectType unmapped2 = newTransferObjectTypeBuilder()
                .withGeneralizations(newGeneralizationBuilder()
                        .withTarget(unmapped1)
                        .build())
                .withName("Unmapped2")
                .withAttributes(newDataMemberBuilder()
                        .withName("attribute2")
                        .withMemberType(MemberType.DERIVED)
                        .withDataType(integerType)
                        .withGetterExpression("5*6-7*8")
                        .build())
                .build();

        final TransferObjectType mapped1 = newTransferObjectTypeBuilder()
                .withName("Mapped1")
                .withMapping(newMappingBuilder()
                        .withTarget(item)
                        .build())
                .withGeneralizations(newGeneralizationBuilder()
                        .withTarget(unmapped2)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.MAPPED)
                        .withRequired(true)
                        .withBinding(nameOfItem)
                        .build())
                .build();

        final TransferObjectType mapped2 = newTransferObjectTypeBuilder()
                .withName("Mapped2")
                .withMapping(newMappingBuilder()
                        .withTarget(item)
                        .build())
                .withGeneralizations(newGeneralizationBuilder()
                        .withTarget(unmapped2)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.MAPPED)
                        .withRequired(true)
                        .withBinding(nameOfItem)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("mapped1List")
                        .withTarget(mapped1)
                        .withLower(0).withUpper(-1)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!filter(i | i.name!matches('[bc]'))!sort(i | i.name)")
                        .build())
                .build();

        final EntityType entity = newEntityTypeBuilder()
                .withName("Entity")
                .withAttributes(newDataMemberBuilder()
                        .withName("simpleArithmeticResult")
                        .withMemberType(MemberType.DERIVED)
                        .withDataType(integerType)
                        .withGetterExpression("1 + 2")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("count")
                        .withMemberType(MemberType.DERIVED)
                        .withDataType(integerType)
                        .withGetterExpression(MODEL_NAME + "::" + "entities::Item!count()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("itemA")
                        .withLower(0).withUpper(1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!filter(i | i.name == 'a')!any()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("itemAny")
                        .withLower(0).withUpper(1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!filter(i | i.name!length() == 1)!sort(j | j.name DESC)!any()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("itemMax")
                        .withLower(0).withUpper(1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!filter(i | i.name!length() == 1)!head(j | j.name DESC)")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("anyItem")
                        .withLower(0).withUpper(1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!any()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("itemX")
                        .withLower(0).withUpper(1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!filter(i | i.name == 'x')!any()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("items")
                        .withLower(0).withUpper(-1)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::entities::Item!sort(i | i.name)")
                        .build())
                .build();

//        // not supported
//        useEntityType(item)
//                .withGeneralizations(newGeneralizationBuilder()
//                        .withTarget(unmapped2)
//                        .build())
//                .build();

        final Model model = newModelBuilder()
                .withName(getModelName())
                .withElements(newPackageBuilder()
                        .withName("type")
                        .withElements(Arrays.asList(stringType, integerType, doubleType, booleanType))
                        .build())
                .withElements(newPackageBuilder()
                        .withName("entities")
                        .withElements(Arrays.asList(item, entity))
                        .build())
                .withElements(newPackageBuilder()
                        .withName("dto")
                        .withElements(tester, unmapped1, unmapped2, mapped1, mapped2)
                        .build())
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass testerType = daoFixture.getAsmUtils().all(EClass.class)
                .filter(c -> (MODEL_NAME + ".dto.Tester").equals(AsmUtils.getClassifierFQName(c)))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Tester type not found in ASM model"));
        final EClass itemType = daoFixture.getAsmUtils().all(EClass.class)
                .filter(c -> (DTO_PACKAGE + ".entities.Item").equals(AsmUtils.getClassifierFQName(c)))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Item type not found in ASM model"));
        final EAttribute simpleArithmeticResultAttribute = testerType.getEAllAttributes().stream()
                .filter(a -> "simpleArithmeticResult".equals(a.getName()))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Attribute 'simpleArithmeticResult' not found in ASM model"));
        final EAttribute countAttribute = testerType.getEAllAttributes().stream()
                .filter(a -> "count".equals(a.getName()))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Attribute 'count' not found in ASM model"));
        final EClass mapped1Type = daoFixture.getAsmUtils().all(EClass.class)
                .filter(c -> (MODEL_NAME + ".dto.Mapped1").equals(AsmUtils.getClassifierFQName(c)))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Mapped1 type not found in ASM model"));
        final EAttribute nameOfMapped1Attribute = mapped1Type.getEAllAttributes().stream()
                .filter(a -> "name".equals(a.getName()))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Attribute 'name' not found in ASM model"));
        final EClass mapped2Type = daoFixture.getAsmUtils().all(EClass.class)
                .filter(c -> (MODEL_NAME + ".dto.Mapped2").equals(AsmUtils.getClassifierFQName(c)))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Mapped2 type not found in ASM model"));
        final EAttribute nameOfMapped2Attribute = mapped2Type.getEAllAttributes().stream()
                .filter(a -> "name".equals(a.getName()))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Attribute 'name' not found in ASM model"));
        final EClass unmapped2Type = daoFixture.getAsmUtils().all(EClass.class)
                .filter(c -> (MODEL_NAME + ".dto.Unmapped2").equals(AsmUtils.getClassifierFQName(c)))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Unmapped2 type not found in ASM model"));
        final EClass entityType = daoFixture.getAsmUtils().all(EClass.class)
                .filter(c -> (DTO_PACKAGE + ".entities.Entity").equals(AsmUtils.getClassifierFQName(c)))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Entity type not found in ASM model"));

        final Payload a = daoFixture.getDao().create(itemType, Payload.map("name", "a"), null);
        log.debug("Item A created successfully: {}", a);
        final Payload b = daoFixture.getDao().create(itemType, Payload.map("name", "b"), null);
        log.debug("Item B created successfully: {}", b);
        final Payload c = daoFixture.getDao().create(itemType, Payload.map("name", "c"), null);
        log.debug("Item C created successfully: {}", c);
        final Payload d = daoFixture.getDao().create(itemType, Payload.map("name", "d"), null);
        log.debug("Item D created successfully: {}", d);

        final Payload simpleArithmeticResult = daoFixture.getDao().getStaticData(simpleArithmeticResultAttribute);
        log.debug("simpleArithmeticResult: {}", simpleArithmeticResult);

        assertThat(simpleArithmeticResult.entrySet(), hasSize(1));
        assertThat(simpleArithmeticResult.get(simpleArithmeticResultAttribute.getName()), equalTo(3));

        final Payload count = daoFixture.getDao().getStaticData(countAttribute);
        log.debug("count: {}", count);

        assertThat(count.entrySet(), hasSize(1));
        assertThat(count.get(countAttribute.getName()), equalTo(4));

        final Payload testerResult = daoFixture.getDao().getStaticFeatures(testerType);
        log.debug("Tester: {}", testerResult);
        assertThat(testerResult.get(simpleArithmeticResultAttribute.getName()), equalTo(3));
        assertThat(testerResult.get(countAttribute.getName()), equalTo(4));
        assertThat(testerResult.get("itemA"), equalTo(a));
        assertThat(testerResult.get("itemAny"), notNullValue());
        assertThat(testerResult.get("itemMax"), equalTo(d));
        assertThat(testerResult.get("anyItem"), notNullValue());
        assertThat(testerResult.get("itemX"), nullValue());
        assertThat(testerResult.get("items"), equalTo(Arrays.asList(a, b, c, d)));

        final List<Payload> mapped1Result = daoFixture.getDao().search(mapped1Type, DAO.QueryCustomizer.<UUID>builder()
                .orderBy(DAO.OrderBy.builder()
                        .attribute(nameOfMapped1Attribute)
                        .descending(true)
                        .build())
                .build());
        log.debug("Mapped1: {}", mapped1Result);
        mapped1Result.stream().forEach(result -> checkStaticFeatures(result, a, d));
        assertThat(mapped1Result, hasSize(4));
        assertThat(mapped1Result.get(0).getAs(String.class, "name"), equalTo("d"));
        assertThat(mapped1Result.get(1).getAs(String.class, "name"), equalTo("c"));
        assertThat(mapped1Result.get(2).getAs(String.class, "name"), equalTo("b"));
        assertThat(mapped1Result.get(3).getAs(String.class, "name"), equalTo("a"));

        final List<Payload> mapped2Result = daoFixture.getDao().search(mapped2Type, DAO.QueryCustomizer.<UUID>builder()
                .filter("this.name!matches('[ad]')")
                .orderBy(DAO.OrderBy.builder()
                        .attribute(nameOfMapped2Attribute)
                        .descending(false)
                        .build())
                .build());
        log.debug("Mapped2: {}", mapped2Result);
        mapped2Result.stream().forEach(result -> checkStaticFeatures(result, a, d));
        assertThat(mapped2Result, hasSize(2));
        assertThat(mapped2Result.get(0).getAs(String.class, "name"), equalTo("a"));
        assertThat(mapped2Result.get(1).getAs(String.class, "name"), equalTo("d"));

        final Payload unmapped2Result = daoFixture.getDao().getStaticFeatures(unmapped2Type);
        log.debug("Unmapped2: {}", unmapped2Result);
        checkStaticFeatures(unmapped2Result, a, d);

        final Payload entityResult = daoFixture.getDao().create(entityType, Payload.empty(), null);
        log.debug("Entity: {}", entityResult);
        assertThat(entityResult.get(simpleArithmeticResultAttribute.getName()), equalTo(3));
        assertThat(entityResult.get(countAttribute.getName()), equalTo(4));
        assertThat(entityResult.get("itemA"), equalTo(a));
        assertThat(entityResult.get("itemAny"), notNullValue());
        assertThat(entityResult.get("itemMax"), equalTo(d));
        assertThat(entityResult.get("anyItem"), notNullValue());
        assertThat(entityResult.get("itemX"), nullValue());
        assertThat(entityResult.get("items"), equalTo(Arrays.asList(a, b, c, d)));
    }

    @Test
    void testStaticRelationOfEntity(final RdbmsDaoFixture daoFixture, final RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();

        final EntityType referenced = newEntityTypeBuilder()
                .withName("Referenced")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(referenced).withMapping(newMappingBuilder().withTarget(referenced).build()).build();

        final DataMember nameOfReferrer = newDataMemberBuilder()
                .withName("name")
                .withDataType(stringType)
                .withMemberType(MemberType.STORED)
                .build();
        final EntityType referrer = newEntityTypeBuilder()
                .withName("Referrer")
                .withAttributes(nameOfReferrer)
                .build();
        useEntityType(referrer).withMapping(newMappingBuilder().withTarget(referrer).build()).build();

        final TransferObjectType tester = newTransferObjectTypeBuilder()
                .withName("Tester")
                .withMapping(newMappingBuilder().withTarget(referrer).build())
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.MAPPED)
                        .withBinding(nameOfReferrer)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("self")
                        .withLower(0).withUpper(1)
                        .withTarget(referrer)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("referencedList")
                        .withLower(0).withUpper(-1)
                        .withTarget(referenced)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::Referenced")
                        .build())
                .build();

        final Model model = newModelBuilder()
                .withName(getModelName())
                .withElements(stringType, referenced, referrer, tester)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass referencedType = (EClass) daoFixture.getAsmUtils().resolve(DTO_PACKAGE + ".Referenced").get();
        final EClass referrerType = (EClass) daoFixture.getAsmUtils().resolve(DTO_PACKAGE + ".Referrer").get();
        final EClass testerType = (EClass) daoFixture.getAsmUtils().resolve(MODEL_NAME + ".Tester").get();

        final Payload a = daoFixture.getDao().create(referencedType, Payload.map("name", "A"), null);
        final UUID aId = a.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());
        final Payload b = daoFixture.getDao().create(referencedType, Payload.map("name", "B"), null);
        final UUID bId = b.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final Payload x = daoFixture.getDao().create(referrerType, Payload.map("name", "X"), null);
        final UUID xId = x.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());
        final Payload y = daoFixture.getDao().create(referrerType, Payload.map("name", "Y"), null);
        final UUID yId = y.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final List<Payload> referencedList = daoFixture.getDao().search(testerType, DAO.QueryCustomizer.<UUID>builder()
                        .orderBy(DAO.OrderBy.builder()
                                .attribute(testerType.getEAllAttributes().stream().filter(attr -> "name".equals(attr.getName())).findAny().get())
                                .descending(true)
                                .build())
                        .seek(DAO.Seek.builder()
                                .limit(10)
                                .build())
                .build());
        log.debug("Referenced list: {}", referencedList);
        final Set<UUID> xy = referencedList.stream().map(p -> p.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toSet());
        assertThat(xy, equalTo(ImmutableSet.of(xId, yId)));

        referencedList.forEach(item -> {
            final Set<UUID> ab = item.getAsCollectionPayload("referencedList").stream().map(p -> p.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toSet());
            assertThat(ab, equalTo(ImmutableSet.of(aId, bId)));
        });
    }

    private void checkStaticFeatures(final Payload payload, final Payload a, final Payload d) {
        assertThat(payload.get("attribute1"), equalTo(14));
        assertThat(payload.get("attribute2"), equalTo(-26));
        assertThat(payload.get("count"), equalTo(4));
        assertThat(payload.get("itemA"), equalTo(a));
        assertThat(payload.get("itemAny"), notNullValue());
        assertThat(payload.get("itemMax"), equalTo(d));
        assertThat(payload.get("anyItem"), notNullValue());
    }
}
