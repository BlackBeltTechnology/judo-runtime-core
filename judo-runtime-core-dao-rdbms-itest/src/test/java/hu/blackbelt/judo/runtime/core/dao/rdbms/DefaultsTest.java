package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.structure.*;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.junit.jupiter.api.AfterEach;
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

@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class DefaultsTest {

    public static final String MODEL_NAME = "M";

    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
    }

    @Test
    public void testDefaultValues(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
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
        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        final EClass testerType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Tester").get();
        final EClass testerDTO1Type = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".TesterDTO1").get();
        final EClass testerDTO2Type = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".TesterDTO2").get();

        final Payload tester10 = runtimeFixture.getDao().create(testerType, Payload.map("number", 10L), null);
        log.debug("Tester 10: {}", tester10);
        assertThat(tester10.get("string"), equalTo("text"));
        assertFalse(tester10.containsKey("_number_default_M_Tester"));
        assertFalse(tester10.containsKey("_string_default_M_Tester"));
        assertFalse(tester10.containsKey("_selected_default_M_Tester"));
        final Payload tester5 = runtimeFixture.getDao().create(testerType, Payload.map("number", 5L), null);
        log.debug("Tester 5: {}", tester5);
        final Payload testerDefaults = runtimeFixture.getDao().create(testerType, Payload.empty(), null);
        log.debug("Tester defaults: {}", testerDefaults);
        assertThat(testerDefaults.get("string"), equalTo("text"));
        assertThat(testerDefaults.get("number"), equalTo(2L));
        assertFalse(testerDefaults.containsKey("_number_default_M_Tester"));
        assertFalse(testerDefaults.containsKey("_string_default_M_Tester"));
        assertFalse(testerDefaults.containsKey("_selected_default_M_Tester"));
        assertThat(testerDefaults.getAsPayload("selected").getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()), equalTo(tester5.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName())));

        final Payload defaults = runtimeFixture.getDao().getDefaultsOf(testerType);
        log.debug("Defaults: {}", defaults);

        final Payload expectedDefaults = Payload.map(
                "number", 3L,
                "string", "text",
                "selected", testerDefaults
        );

        assertThat(defaults, equalTo(expectedDefaults));

        final Payload dto1Defaults = runtimeFixture.getDao().getDefaultsOf(testerDTO1Type);
        log.debug("DTO1 defaults: {}", dto1Defaults);

        final Payload expectedDTO1Defaults = Payload.map(
                "n", 1L,
                "selected", testerDefaults
        );

        assertThat(dto1Defaults, equalTo(expectedDTO1Defaults));

        final Payload testerDto1Defaults = runtimeFixture.getDao().create(testerDTO1Type, Payload.empty(), null);
        log.debug("Tester DTO1: {}", testerDto1Defaults);

        assertThat(testerDto1Defaults.get("n"), equalTo(1L));
        assertThat(testerDto1Defaults.get("selected"), notNullValue());
        assertFalse(testerDto1Defaults.containsKey("_n_default_M_TesterDTO1"));
        assertThat(testerDto1Defaults.getAsPayload("selected").getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()), equalTo(testerDefaults.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName())));

        final Payload testerDto1DefaultsEntity = runtimeFixture.getDao().getByIdentifier(testerType, testerDto1Defaults.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName())).get();
        log.debug("Tester DTO1 entity: {}", testerDto1DefaultsEntity);

        assertThat(testerDto1DefaultsEntity.get("number"), equalTo(1L));
        assertThat(testerDto1DefaultsEntity.get("string"), equalTo("text"));
        assertThat(testerDto1DefaultsEntity.get("selected"), notNullValue());
        assertFalse(testerDto1DefaultsEntity.containsKey("_number_default_M_Tester"));
        assertFalse(testerDto1DefaultsEntity.containsKey("_string_default_M_Tester"));
        assertFalse(testerDto1DefaultsEntity.containsKey("_selected_default_M_Tester"));
        assertThat(testerDto1DefaultsEntity.getAsPayload("selected").getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()), equalTo(testerDefaults.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName())));

        final Payload dto2Defaults = runtimeFixture.getDao().getDefaultsOf(testerDTO2Type);
        log.debug("DTO2 defaults: {}", dto2Defaults);

        final Payload expectedDTO2Defaults = Payload.empty();

        assertThat(dto2Defaults, equalTo(expectedDTO2Defaults));

        final Payload testerDto2Defaults = runtimeFixture.getDao().create(testerDTO2Type, Payload.empty(), null);
        log.debug("Tester DTO2: {}", testerDto2Defaults);
        assertFalse(testerDto2Defaults.containsKey("_number_default_M_Tester"));
        assertFalse(testerDto2Defaults.containsKey("_string_default_M_Tester"));
        assertFalse(testerDto2Defaults.containsKey("_selected_default_M_Tester"));

        final Payload testerDto2DefaultsEntity = runtimeFixture.getDao().getByIdentifier(testerType, testerDto2Defaults.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName())).get();
        log.debug("Tester DTO2 entity: {}", testerDto2DefaultsEntity);

        assertThat(testerDto2DefaultsEntity.get("number"), equalTo(4L));
        assertThat(testerDto2DefaultsEntity.get("string"), equalTo("text"));
        assertThat(testerDto2DefaultsEntity.get("selected"), notNullValue());
        assertThat(testerDto2DefaultsEntity.getAsPayload("selected").getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()), equalTo(testerDto1DefaultsEntity.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName())));
    }

    @Test
    public void testDefaultsOfNonExposedRequired(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
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
        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        final EClass categoryType = (EClass) runtimeFixture.getAsmUtils().resolve(DTO_PACKAGE + ".Category").get();
        final EClass orderItemType = (EClass) runtimeFixture.getAsmUtils().resolve(DTO_PACKAGE + ".OrderItem").get();
        final EClass orderItemDTOType = (EClass) runtimeFixture.getAsmUtils().resolve(MODEL_NAME + ".OrderItemDTO").get();

        runtimeFixture.getDao().create(categoryType, Payload.map(
                "name", "Category"
        ), null);
        final Payload orderItem1 = runtimeFixture.getDao().create(orderItemDTOType, Payload.empty(), null);
        final Payload orderItem1Loaded = runtimeFixture.getDao().getByIdentifier(orderItemType, orderItem1.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName())).get();

        assertThat(orderItem1Loaded.get("quantityquantityquantityquantityquantityquantityquantityquantityquantityquantity"), equalTo(1L));
        assertThat(orderItem1Loaded.getAsPayload("category").get("name"), equalTo("Category"));
    }

    @Test
    public void testDefaultsOfUnmappedTransferObjectType(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
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
        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        final EClass ballType = (EClass) runtimeFixture.getAsmUtils().resolve(DTO_PACKAGE + ".Ball").get();
        final EClass testerType = (EClass) runtimeFixture.getAsmUtils().resolve(MODEL_NAME + ".Tester").get();

        final Payload ball1 = runtimeFixture.getDao().create(ballType, Payload.empty(), null);
        final Payload testerDefaults = runtimeFixture.getDao().getDefaultsOf(testerType);

        assertThat(testerDefaults.getAs(Long.class, "constantDefault"), equalTo(1L));
        assertThat(testerDefaults.getAs(Long.class, "additionDefault"), equalTo(3L));
        assertThat(testerDefaults.getAsPayload("selectedBall"), equalTo(ball1));
    }
}
