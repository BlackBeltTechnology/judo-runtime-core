package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.psm.data.Attribute;
import hu.blackbelt.judo.meta.psm.data.Containment;
import hu.blackbelt.judo.meta.psm.data.EntityType;
import hu.blackbelt.judo.meta.psm.data.util.builder.DataBuilders;
import hu.blackbelt.judo.meta.psm.derived.DataProperty;
import hu.blackbelt.judo.meta.psm.derived.ExpressionDialect;
import hu.blackbelt.judo.meta.psm.namespace.Model;
import hu.blackbelt.judo.meta.psm.service.MappedTransferObjectType;
import hu.blackbelt.judo.meta.psm.type.NumericType;
import hu.blackbelt.judo.meta.psm.type.StringType;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.psm.data.util.builder.DataBuilders.*;
import static hu.blackbelt.judo.meta.psm.derived.util.builder.DerivedBuilders.newDataExpressionTypeBuilder;
import static hu.blackbelt.judo.meta.psm.derived.util.builder.DerivedBuilders.newDataPropertyBuilder;
import static hu.blackbelt.judo.meta.psm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.psm.service.util.builder.ServiceBuilders.*;
import static hu.blackbelt.judo.meta.psm.type.util.builder.TypeBuilders.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class CountTest {
    public static final String MODEL_NAME = "CountTest";

    public static final String A_ENTITY = "A";
    public static final String B_ENTITY = "B";
    public static final String C_ENTITY = "C";
    public static final String A_DTO = "ADTO";
    public static final String A_ADDITION_DTO = "AAdditionDTO";
    public static final String B_DTO = "BDTO";
    public static final String C_DTO = "CDTO";

    public static final String B1 = "b1";
    public static final String B2 = "b2";
    public static final String CS = "cs";

    public static final String COUNT_C1 = "countC1";
    public static final String COUNT_C2 = "countC2";
    public static final String COUNT_C1_PLUS_ONE = "countC1Plus1";

    public static final String NAME = "name";

    protected Model getPsmModel() {

        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(10).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(18).withScale(0).build();

        final Attribute nameOfA = newAttributeBuilder()
                .withName(NAME)
                .withDataType(stringType)
                .withRequired(true)
                .build();
        final Attribute nameOfB = newAttributeBuilder()
                .withName(NAME)
                .withDataType(stringType)
                .withRequired(true)
                .build();
        final Attribute nameOfC = newAttributeBuilder()
                .withName(NAME)
                .withDataType(stringType)
                .withRequired(true)
                .build();

        final DataProperty countC1 = newDataPropertyBuilder()
                .withName(COUNT_C1)
                .withDataType(integerType)
                .withRequired(true)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withDialect(ExpressionDialect.JQL)
                        .withExpression("self.b1.cs!count()")
                        .build())
                .build();
        final DataProperty countC1PlusOne = newDataPropertyBuilder()
                .withName(COUNT_C1_PLUS_ONE)
                .withDataType(integerType)
                .withRequired(true)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withDialect(ExpressionDialect.JQL)
                        .withExpression("self.b1.cs!count() + 1")
                        .build())
                .build();
        final DataProperty countC2 = newDataPropertyBuilder()
                .withName(COUNT_C2)
                .withDataType(integerType)
                .withRequired(true)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withDialect(ExpressionDialect.JQL)
                        .withExpression("self.b2.cs!count()")
                        .build())
                .build();

        final EntityType a = newEntityTypeBuilder()
                .withName(A_ENTITY)
                .withAttributes(Arrays.asList(nameOfA))
                .withDataProperties(Arrays.asList(countC1, countC2, countC1PlusOne))
                .build();
        final EntityType b = newEntityTypeBuilder()
                .withName(B_ENTITY)
                .withAttributes(Arrays.asList(nameOfB))
                .build();
        final EntityType c = newEntityTypeBuilder()
                .withName(C_ENTITY)
                .withAttributes(Arrays.asList(nameOfC))
                .build();

        final Containment b1 = DataBuilders.newContainmentBuilder()
                .withName(B1)
                .withTarget(b)
                .withCardinality(newCardinalityBuilder().withLower(0).withUpper(1).build())
                .build();
        final Containment b2 = DataBuilders.newContainmentBuilder()
                .withName(B2)
                .withTarget(b)
                .withCardinality(newCardinalityBuilder().withLower(0).withUpper(1).build())
                .build();
        final Containment cs = DataBuilders.newContainmentBuilder()
                .withName(CS)
                .withTarget(c)
                .withCardinality(newCardinalityBuilder().withLower(0).withUpper(-1).build())
                .build();

        useEntityType(a).withRelations(Arrays.asList(b1, b2)).build();
        useEntityType(b).withRelations(cs).build();

        final MappedTransferObjectType aDTO = newMappedTransferObjectTypeBuilder()
                .withName(A_DTO)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withRequired(true)
                        .withBinding(nameOfA)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(COUNT_C1)
                        .withDataType(integerType)
                        .withRequired(true)
                        .withBinding(countC1)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(COUNT_C2)
                        .withDataType(integerType)
                        .withRequired(true)
                        .withBinding(countC2)
                        .build())
                .withEntityType(a)
                .build();
        final MappedTransferObjectType aAdditionDTO = newMappedTransferObjectTypeBuilder()
                .withName(A_ADDITION_DTO)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(COUNT_C1_PLUS_ONE)
                        .withDataType(integerType)
                        .withRequired(true)
                        .withBinding(countC1PlusOne)
                        .build())
                .withEntityType(a)
                .build();
        final MappedTransferObjectType bDTO = newMappedTransferObjectTypeBuilder()
                .withName(B_DTO)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withRequired(true)
                        .withBinding(nameOfB)
                        .build())
                .withEntityType(b)
                .build();
        final MappedTransferObjectType cDTO = newMappedTransferObjectTypeBuilder()
                .withName(C_DTO)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withRequired(true)
                        .withBinding(nameOfC)
                        .build())
                .withEntityType(c)
                .build();

        useMappedTransferObjectType(aDTO)
                .withRelations(newTransferObjectRelationBuilder()
                        .withName(B1)
                        .withCardinality(newCardinalityBuilder().withLower(0).withUpper(1).build())
                        .withTarget(bDTO)
                        .withEmbedded(true).withEmbeddedCreate(true)
                        .withBinding(b1)
                        .build())
                .build();
        useMappedTransferObjectType(aDTO)
                .withRelations(newTransferObjectRelationBuilder()
                        .withName(B2)
                        .withCardinality(newCardinalityBuilder().withLower(0).withUpper(1).build())
                        .withTarget(bDTO)
                        .withEmbedded(true).withEmbeddedCreate(true)
                        .withBinding(b2)
                        .build())
                .build();
        useMappedTransferObjectType(bDTO)
                .withRelations(newTransferObjectRelationBuilder()
                        .withName(CS)
                        .withCardinality(newCardinalityBuilder().withLower(0).withUpper(-1).build())
                        .withTarget(cDTO)
                        .withEmbedded(true).withEmbeddedCreate(true)
                        .withBinding(cs)
                        .build())
                .build();

        Model model = newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(Arrays.asList(stringType, integerType, a, b, c, aDTO, bDTO, cDTO, aAdditionDTO)).build();
        return model;
    }

    @BeforeEach
    public void setup(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(getPsmModel(), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");
    }

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
    }

    @Test
    void testCount(JudoRuntimeFixture runtimeFixture) {

        final Payload a1 = map(
                NAME, "a1",
                B1, map(NAME, "b1"));

        final Payload a2 = map(
                NAME, "a2",
                B1, map(NAME, "b1",
                        CS, Arrays.asList(
                                map(NAME, "c1"),
                                map(NAME, "c2")
                        )
                ));

        final EClass aDto = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + "." + A_DTO).get();

        final Payload saved1 = runtimeFixture.getDao().create(aDto, a1, null);
        final Payload saved2 = runtimeFixture.getDao().create(aDto, a2, null);

        final UUID id1 = saved1.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        final UUID id2 = saved2.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());

        log.debug("Entity #1: {}", saved1);
        log.debug("Entity #2: {}", saved2);

        assertEquals(Long.valueOf(0L), saved1.getAs(Long.class, COUNT_C1));
        assertEquals(Long.valueOf(0L), saved1.getAs(Long.class, COUNT_C2));
        assertEquals(Long.valueOf(2L), saved2.getAs(Long.class, COUNT_C1));
        assertEquals(Long.valueOf(0L), saved2.getAs(Long.class, COUNT_C2));

        runtimeFixture.getDao().delete(aDto, id1);
        runtimeFixture.getDao().delete(aDto, id2);
    }

    @Test
    void testAddingNumberToCount(JudoRuntimeFixture runtimeFixture) {
        final Payload a1 = map(
                NAME, "a1",
                B1, map(NAME, "b1",
                        CS, Arrays.asList(
                                map(NAME, "c1"),
                                map(NAME, "c2")
                        )
                ));

        final EClass aDto = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + "." + A_DTO).get();
        final EClass aAdditionDto = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + "." + A_ADDITION_DTO).get();

        final Payload saved = runtimeFixture.getDao().create(aDto, a1, DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        log.debug("Saved: {}", saved);
        final UUID id = saved.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());

        final Payload loaded = runtimeFixture.getDao().getByIdentifier(aAdditionDto, id).get();
        log.debug("Loaded: {}", loaded);

        assertEquals(Long.valueOf(3L), loaded.getAs(Long.class, COUNT_C1_PLUS_ONE));

        runtimeFixture.getDao().delete(aDto, id);
    }
}
