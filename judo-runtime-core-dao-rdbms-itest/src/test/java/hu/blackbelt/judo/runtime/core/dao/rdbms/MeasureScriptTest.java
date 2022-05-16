package hu.blackbelt.judo.runtime.core.dao.rdbms;


import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.measure.Measure;
import hu.blackbelt.judo.meta.esm.measure.MeasuredType;
import hu.blackbelt.judo.meta.esm.measure.Unit;
import hu.blackbelt.judo.meta.esm.measure.util.builder.MeasureBuilder;
import hu.blackbelt.judo.meta.esm.measure.util.builder.MeasuredTypeBuilder;
import hu.blackbelt.judo.meta.esm.measure.util.builder.UnitBuilder;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.NamespaceElement;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.ModelBuilder;
import hu.blackbelt.judo.meta.esm.operation.util.builder.OperationBuilder;
import hu.blackbelt.judo.meta.esm.operation.util.builder.ParameterBuilder;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.util.builder.DataMemberBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.EntityTypeBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.MappingBuilder;
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
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.esm.operation.OperationType.STATIC;
import static hu.blackbelt.judo.meta.esm.structure.MemberType.STORED;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class MeasureScriptTest {
    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
    }

    @Test
    public void testMeasureScriptTest(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        final Set<NamespaceElement> namespaceElements = new HashSet<>();

        final Unit dm = UnitBuilder.create().withName("decimeter").withSymbol("dm").withRateDividend(1).withRateDivisor(10).build();
        final Unit m = UnitBuilder.create().withName("meter").withSymbol("m").withRateDividend(1).withRateDivisor(1).build();
        final Unit km = UnitBuilder.create().withName("kilometer").withSymbol("km").withRateDividend(1000).withRateDivisor(1).build();

        final Measure measure = MeasureBuilder.create().withName("Length").withUnits(dm, m, km).build();
        namespaceElements.add(measure);

        final MeasuredType decimeter = MeasuredTypeBuilder.create().withName("lengthInDecimeter").withPrecision(10).withScale(3).withStoreUnit(dm).build();
        final MeasuredType meter = MeasuredTypeBuilder.create().withName("lengthInMeter").withPrecision(10).withScale(3).withStoreUnit(m).build();
        final MeasuredType kilometer = MeasuredTypeBuilder.create().withName("lengthInKilometer").withPrecision(10).withScale(3).withStoreUnit(km).build();
        namespaceElements.addAll(ImmutableSet.of(decimeter, meter, kilometer));

        final EntityType testEntity = EntityTypeBuilder.create()
                .withName("Test")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("lengthIn_dm")
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(decimeter)
                                .build())
                .build();
        testEntity.setMapping(MappingBuilder.create().withTarget(testEntity).build());
        namespaceElements.add(testEntity);

        testEntity.getOperations().add(
                OperationBuilder.create()
                        .withName("testOperation")
                        .withBinding("")
                        .withStateful(false)
                        .withCustomImplementation(false)
                        .withOperationType(STATIC)
                        .withBody("return M::Test!filter(t | t.lengthIn_dm > 1[km])")
                        .withOutput(
                                ParameterBuilder.create()
                                        .withName("output")
                                        .withLower(0)
                                        .withUpper(-1)
                                        .withTarget(testEntity)
                                        .build())
                        .build());

        final Model model = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "Dao is not initialized");

        final EClass testEntityEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Test").get();

        final UUID testEntityID1 = runtimeFixture.getDao()
                .create(testEntityEClass, Payload.map("lengthIn_dm", 0), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with id: {}", testEntityEClass.getName(), testEntityID1);

        final UUID testEntityID2 = runtimeFixture.getDao()
                .create(testEntityEClass, Payload.map("lengthIn_dm", 1), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with id: {}", testEntityEClass.getName(), testEntityID2);

        final UUID testEntityID3 = runtimeFixture.getDao()
                .create(testEntityEClass, Payload.map("lengthIn_dm", 1000), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with id: {}", testEntityEClass.getName(), testEntityID3);

        final UUID testEntityID4 = runtimeFixture.getDao()
                .create(testEntityEClass, Payload.map("lengthIn_dm", 15000), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with id: {}", testEntityEClass.getName(), testEntityID4);

        final Set<UUID> testOperationOutputIDs = runtimeFixture.getOperationImplementations()
                .get("testOperation").apply(Payload.empty())
                .getAsCollectionPayload("output").stream()
                .map(p -> p.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()))
                .collect(Collectors.toSet());

        assertThat(testOperationOutputIDs, equalTo(ImmutableSet.of(testEntityID4)));
    }

}
