package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.structure.*;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;

import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newNumericTypeBuilder;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class DefaultsTest {

    public static final String MODEL_NAME = "M";

    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    public void testDefaultValues(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(16).withScale(0).build();

        final Model model = NamespaceBuilders.newModelBuilder().withName(MODEL_NAME).build();

        final DataMember numberOfTester = newDataMemberBuilder()
                .withName("number")
                .withMemberType(MemberType.STORED)
                .withDataType(integerType)
                .withDefaultExpression("M::Tester!count()")
                .build();
        final DataMember stringOfTester = newDataMemberBuilder()
                .withName("string")
                .withMemberType(MemberType.STORED)
                .withDataType(stringType)
                .withDefaultExpression("'text'")
                .build();
        final EntityType tester = newEntityTypeBuilder()
                .withName("Tester")
                .withAttributes(numberOfTester)
                .withAttributes(stringOfTester)
                .build();
        final OneWayRelationMember selectedOfTester = newOneWayRelationMemberBuilder()
                .withName("selected")
                .withMemberType(MemberType.STORED)
                .withTarget(tester)
                .withLower(0).withUpper(1)
                .withRelationKind(RelationKind.AGGREGATION)
                .withDefaultExpression("M::Tester!head(t | t.number)")
                .build();
        useEntityType(tester)
                .withMapping(newMappingBuilder().withTarget(tester).build())
                .withRelations(selectedOfTester)
                .build();

        final TransferObjectType testerDTO1 = newTransferObjectTypeBuilder()
                .withMapping(newMappingBuilder().withTarget(tester).build())
                .withName("TesterDTO1")
                .withAttributes(newDataMemberBuilder()
                        .withName("n")
                        .withMemberType(MemberType.MAPPED)
                        .withDataType(integerType)
                        .withBinding(numberOfTester)
                        .withDefaultExpression("M::Tester!min(t | t.number) - 1")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("selected")
                        .withMemberType(MemberType.MAPPED)
                        .withTarget(tester)
                        .withLower(0).withUpper(1)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withBinding(selectedOfTester)
                        .build())
                .build();

        final TransferObjectType testerDTO2 = newTransferObjectTypeBuilder()
                .withMapping(newMappingBuilder().withTarget(tester).build())
                .withName("TesterDTO2")
                .build();

        model.getElements().addAll(Arrays.asList(
                stringType, integerType,
                tester, testerDTO1, testerDTO2
        ));
        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass testerType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Tester").get();
        final EClass testerDTO1Type = daoFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".TesterDTO1").get();
        final EClass testerDTO2Type = daoFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".TesterDTO2").get();

        final Payload tester10 = daoFixture.getDao().create(testerType, Payload.map("number", 10L), null);
        log.debug("Tester 10: {}", tester10);
        assertThat(tester10.get("string"), equalTo("text"));
        assertFalse(tester10.containsKey("_number_default_M_Tester"));
        assertFalse(tester10.containsKey("_string_default_M_Tester"));
        assertFalse(tester10.containsKey("_selected_default_M_Tester"));
        final Payload tester5 = daoFixture.getDao().create(testerType, Payload.map("number", 5L), null);
        log.debug("Tester 5: {}", tester5);
        final Payload testerDefaults = daoFixture.getDao().create(testerType, Payload.empty(), null);
        log.debug("Tester defaults: {}", testerDefaults);
        assertThat(testerDefaults.get("string"), equalTo("text"));
        assertThat(testerDefaults.get("number"), equalTo(2L));
        assertFalse(testerDefaults.containsKey("_number_default_M_Tester"));
        assertFalse(testerDefaults.containsKey("_string_default_M_Tester"));
        assertFalse(testerDefaults.containsKey("_selected_default_M_Tester"));
        assertThat(testerDefaults.getAsPayload("selected").getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName()), equalTo(tester5.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())));

        final Payload defaults = daoFixture.getDao().getDefaultsOf(testerType);
        log.debug("Defaults: {}", defaults);

        final Payload expectedDefaults = Payload.map(
                "number", 3L,
                "string", "text",
                "selected", testerDefaults
        );

        assertThat(defaults, equalTo(expectedDefaults));

        final Payload dto1Defaults = daoFixture.getDao().getDefaultsOf(testerDTO1Type);
        log.debug("DTO1 defaults: {}", dto1Defaults);

        final Payload expectedDTO1Defaults = Payload.map(
                "n", 1L,
                "selected", testerDefaults
        );

        assertThat(dto1Defaults, equalTo(expectedDTO1Defaults));

        final Payload testerDto1Defaults = daoFixture.getDao().create(testerDTO1Type, Payload.empty(), null);
        log.debug("Tester DTO1: {}", testerDto1Defaults);

        assertThat(testerDto1Defaults.get("n"), equalTo(1L));
        assertThat(testerDto1Defaults.get("selected"), notNullValue());
        assertFalse(testerDto1Defaults.containsKey("_n_default_M_TesterDTO1"));
        assertThat(testerDto1Defaults.getAsPayload("selected").getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName()), equalTo(testerDefaults.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())));

        final Payload testerDto1DefaultsEntity = daoFixture.getDao().getByIdentifier(testerType, testerDto1Defaults.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).get();
        log.debug("Tester DTO1 entity: {}", testerDto1DefaultsEntity);

        assertThat(testerDto1DefaultsEntity.get("number"), equalTo(1L));
        assertThat(testerDto1DefaultsEntity.get("string"), equalTo("text"));
        assertThat(testerDto1DefaultsEntity.get("selected"), notNullValue());
        assertFalse(testerDto1DefaultsEntity.containsKey("_number_default_M_Tester"));
        assertFalse(testerDto1DefaultsEntity.containsKey("_string_default_M_Tester"));
        assertFalse(testerDto1DefaultsEntity.containsKey("_selected_default_M_Tester"));
        assertThat(testerDto1DefaultsEntity.getAsPayload("selected").getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName()), equalTo(testerDefaults.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())));

        final Payload dto2Defaults = daoFixture.getDao().getDefaultsOf(testerDTO2Type);
        log.debug("DTO2 defaults: {}", dto2Defaults);

        final Payload expectedDTO2Defaults = Payload.empty();

        assertThat(dto2Defaults, equalTo(expectedDTO2Defaults));

        final Payload testerDto2Defaults = daoFixture.getDao().create(testerDTO2Type, Payload.empty(), null);
        log.debug("Tester DTO2: {}", testerDto2Defaults);
        assertFalse(testerDto2Defaults.containsKey("_number_default_M_Tester"));
        assertFalse(testerDto2Defaults.containsKey("_string_default_M_Tester"));
        assertFalse(testerDto2Defaults.containsKey("_selected_default_M_Tester"));

        final Payload testerDto2DefaultsEntity = daoFixture.getDao().getByIdentifier(testerType, testerDto2Defaults.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).get();
        log.debug("Tester DTO2 entity: {}", testerDto2DefaultsEntity);

        assertThat(testerDto2DefaultsEntity.get("number"), equalTo(4L));
        assertThat(testerDto2DefaultsEntity.get("string"), equalTo("text"));
        assertThat(testerDto2DefaultsEntity.get("selected"), notNullValue());
        assertThat(testerDto2DefaultsEntity.getAsPayload("selected").getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName()), equalTo(testerDto1DefaultsEntity.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())));
    }

    @Test
    public void testDefaultsOfNonExposedRequired(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(16).withScale(0).build();

        final Model model = NamespaceBuilders.newModelBuilder().withName(MODEL_NAME).build();

        final EntityType category = newEntityTypeBuilder()
                .withName("Category")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(category).withMapping(newMappingBuilder().withTarget(category).build()).build();

        final DataMember quantityOfOrderItem = newDataMemberBuilder()
                .withName("quantityquantityquantityquantityquantityquantityquantityquantityquantityquantity")
                .withMemberType(MemberType.STORED)
                .withDataType(integerType)
                .withRequired(true)
                .withDefaultExpression("1")
                .build();
        final OneWayRelationMember categoryOfOrderItem = newOneWayRelationMemberBuilder()
                .withName("category")
                .withMemberType(MemberType.STORED)
                .withTarget(category)
                .withLower(1).withUpper(1)
                .withRelationKind(RelationKind.AGGREGATION)
                .withDefaultExpression("M::Category!head(c | c.name)")
                .build();
        final EntityType orderItem = newEntityTypeBuilder()
                .withName("OrderItem")
                .withAttributes(quantityOfOrderItem)
                .withRelations(categoryOfOrderItem)
                .build();
        useEntityType(orderItem)
                .withMapping(newMappingBuilder().withTarget(orderItem).build())
                .build();

        final TransferObjectType orderItemDTO = newTransferObjectTypeBuilder()
                .withMapping(newMappingBuilder().withTarget(orderItem).build())
                .withName("OrderItemDTO")
                .build();

        model.getElements().addAll(Arrays.asList(
                stringType, integerType,
                orderItem, category, orderItemDTO
        ));
        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass categoryType = (EClass) daoFixture.getAsmUtils().resolve(DTO_PACKAGE + ".Category").get();
        final EClass orderItemType = (EClass) daoFixture.getAsmUtils().resolve(DTO_PACKAGE + ".OrderItem").get();
        final EClass orderItemDTOType = (EClass) daoFixture.getAsmUtils().resolve(MODEL_NAME + ".OrderItemDTO").get();

        daoFixture.getDao().create(categoryType, Payload.map(
                "name", "Category"
        ), null);
        final Payload orderItem1 = daoFixture.getDao().create(orderItemDTOType, Payload.empty(), null);
        final Payload orderItem1Loaded = daoFixture.getDao().getByIdentifier(orderItemType, orderItem1.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).get();

        assertThat(orderItem1Loaded.get("quantityquantityquantityquantityquantityquantityquantityquantityquantityquantity"), equalTo(1L));
        assertThat(orderItem1Loaded.getAsPayload("category").get("name"), equalTo("Category"));
    }

    @Test
    public void testDefaultsOfUnmappedTransferObjectType(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(16).withScale(0).build();

        final Model model = NamespaceBuilders.newModelBuilder().withName(MODEL_NAME).build();

        final EntityType ball = newEntityTypeBuilder()
                .withName("Ball")
                .build();
        useEntityType(ball).withMapping(newMappingBuilder().withTarget(ball).build()).build();

        final TransferObjectType tester = newTransferObjectTypeBuilder()
                .withName("Tester")
                .withAttributes(newDataMemberBuilder()
                        .withName("constantDefault")
                        .withDataType(integerType)
                        .withMemberType(MemberType.TRANSIENT)
                        .withDefaultExpression("1")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("additionDefault")
                        .withDataType(integerType)
                        .withMemberType(MemberType.TRANSIENT)
                        .withDefaultExpression("1 + 2")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("selectedBall")
                        .withLower(0).withUpper(1)
                        .withTarget(ball)
                        .withMemberType(MemberType.TRANSIENT)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withDefaultExpression(MODEL_NAME + "::Ball!any()")
                        .build())
                .build();

        model.getElements().addAll(Arrays.asList(
                stringType, integerType, ball, tester
        ));
        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass ballType = (EClass) daoFixture.getAsmUtils().resolve(DTO_PACKAGE + ".Ball").get();
        final EClass testerType = (EClass) daoFixture.getAsmUtils().resolve(MODEL_NAME + ".Tester").get();

        final Payload ball1 = daoFixture.getDao().create(ballType, Payload.empty(), null);
        final Payload testerDefaults = daoFixture.getDao().getDefaultsOf(testerType);

        assertThat(testerDefaults.getAs(Long.class, "constantDefault"), equalTo(1L));
        assertThat(testerDefaults.getAs(Long.class, "additionDefault"), equalTo(3L));
        assertThat(testerDefaults.getAsPayload("selectedBall"), equalTo(ball1));
    }
}
