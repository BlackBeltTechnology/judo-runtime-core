package hu.blackbelt.judo.runtime.core.dao.rdbms.script;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.function.Function;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class WhitespacesInOperationParametersTest {

    private static final String DTO = "demo._default_transferobjecttypes.entities.";
    public static final String OUTPUT = "output";
    public static final String ID_KEY = "__identifier";

    public static Payload run(RdbmsDaoFixture fixture, String operationName, Payload exchange) {
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

    public static Payload run(RdbmsDaoFixture fixture) {
        return run(fixture, null, null);
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        if (daoFixture.isInitialized()) {
            daoFixture.dropDatabase();
        }
    }

    @Test
    public void test(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();

        builder.addEntity("Tester")
               .withAttribute("String", "name");

        builder.addBoundOperation("Tester", "operation")
               .withInput("Tester", "input", cardinality(1, 1))
               .withBody("return mutable input")
               .withOutput("Tester", cardinality(1, 1));

        Map<String, String> operationDescriptors = Map.of(
                "btb", "(t)",
                "btsb", "(t )",
                "bstb", "( t)",
                "btssb", "(t  )",
                "bstsb", "( t )",
                "bsstb", "(  t)"
        );
        operationDescriptors.forEach((operationName, operationParameter) -> {
            String operationBody = "var demo::entities::Tester t = new demo::entities::Tester(name = 't')\n" +
                                   "return t.operation" + operationParameter;
            builder.addUnboundOperation(operationName)
                   .withBody(operationBody)
                   .withOutput("Tester", cardinality(1, 1));
            log.debug(String.format("Testing operation:\n%s", operationBody));
        });

        daoFixture.init(builder.build(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        operationDescriptors.forEach((operationName, operationParameter) -> {
            Payload operationResult = run(daoFixture, operationName, Payload.empty());
            assertNotNull(operationResult);
            Payload output = operationResult.getAsPayload(OUTPUT);
            assertNotNull(output);
            assertEquals("t", output.getAs(String.class, "name"));
        });

    }
}
