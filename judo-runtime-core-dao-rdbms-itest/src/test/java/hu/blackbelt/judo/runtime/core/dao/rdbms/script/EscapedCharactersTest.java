package hu.blackbelt.judo.runtime.core.dao.rdbms.script;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Function;
import java.util.stream.Stream;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class EscapedCharactersTest {

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

    private static Stream<String> testEscapeCharacterSource() {
        return Stream.of("\\b", "\\t", "\\n", "\\f", "\\r", "\\\"", "\\'", "\\\\");
    }

    @ParameterizedTest
    @MethodSource("testEscapeCharacterSource")
    public void testEscapeCharacter(String string, JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();

        builder.addEntity("Tester")
                .withAttribute("String", "s");
        builder.addUnboundOperation("init")
                .withBody(String.format("return new demo::entities::Tester(s = \"%s\")", string))
                .withOutput("Tester", cardinality(1, 1));

        fixture.init(builder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture, "init", Payload.empty());
    }
}
