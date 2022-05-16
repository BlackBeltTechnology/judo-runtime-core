package hu.blackbelt.judo.runtime.core.dao.rdbms.script;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.*;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class ReturnedUnmappedTOTest {

    private static final String DTO = "demo._default_transferobjecttypes.entities.";
    public static final String OUTPUT = "output";
    public static final String ID_KEY = "__identifier";

    public static Payload run(JudoRuntimeFixture fixture, String operationName, Payload exchange) {
        Function<Payload, Payload> operationImplementation =
                operationName != null ? fixture.getOperationImplementations().get(operationName) :
                        fixture.getOperationImplementations().values().iterator().next();
        Payload result;
        try {
            result = operationImplementation.apply(exchange);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static Payload run(JudoRuntimeFixture fixture) {
        return run(fixture, null, null);
    }

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        if (runtimeFixture.isInitialized()) {
            runtimeFixture.dropDatabase();
        }
    }


    @Test
    public void test(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();

        builder.addEntity("Target")
                .withAttribute("String", "name");

        builder.addUnmappedTransferObject("Tester")
                .withStaticNavigation("Target", "t", "demo::entities::Target", cardinality(0, -1));

        builder.addUnboundOperation("operation")
                .withBody("var demo::services::Tester t = new demo::services::Tester()\n" +
                          "var demo::entities::Target t1 = new demo::entities::Target(name = 't1')\n" +
                          "var demo::entities::Target t2 = new demo::entities::Target(name = 't2')\n" +
                          "return t")
                .withOutput("Tester", cardinality(1, 1));

        runtimeFixture.init(builder.build(), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        EClass targetEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "Target").orElseThrow();

        DAO<UUID> dao = runtimeFixture.getDao();

        testData(targetEClass, dao, run(runtimeFixture, "operation", Payload.empty()).getAsPayload(OUTPUT), 2);

        dao.create(targetEClass, Payload.map("name", "target"), DAO.QueryCustomizer.<UUID>builder().build());
        dao.create(targetEClass, Payload.map("name", "target1"), DAO.QueryCustomizer.<UUID>builder().build());

        testData(targetEClass, dao, run(runtimeFixture, "operation", Payload.empty()).getAsPayload(OUTPUT), 6);
    }

    private void testData(EClass targetEClass, DAO<UUID> dao, Payload result, int size) {
        Set<UUID> targetSet = dao.getAllOf(targetEClass).stream()
                .map(p -> p.getAs(UUID.class, ID_KEY))
                .collect(Collectors.toSet());
        assertThat(targetSet.size(), equalTo(size));
        Set<UUID> resultIDSet = result.getAsCollectionPayload("t").stream()
                .map(p -> p.getAs(UUID.class, ID_KEY))
                .collect(Collectors.toSet());
        assertThat(resultIDSet, equalTo(targetSet));
    }
}
