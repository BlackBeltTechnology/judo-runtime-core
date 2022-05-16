package hu.blackbelt.judo.runtime.core.dao.rdbms.script;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.*;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class DecimalTargetedIntegerDivisionTest {

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
    public void testIntegerDivisionInPractice(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();

        builder.addEntity("Tester")
                .withAttribute("String", "name")
                .withAttribute("Boolean", "bool")
                .withProperty("Double", "trueTargetScale",
                              "self.targets!count() > 0" +
                              "     ? self.targets!filter(t | t.bool)!count() / self.targets!count()" +
                              "     : 0")
                .withRelation("Tester", "targets", cardinality(0, -1));

        runtimeFixture.setValidateModels(true);
        runtimeFixture.init(builder.build(), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "Dao initialized");

        EClass testerEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "Tester").orElseThrow();
        EReference targetsEReference = testerEClass.getEAllReferences().stream()
                .filter(r -> "targets".equals(r.getName()))
                .findAny().orElseThrow();

        DAO<UUID> dao = runtimeFixture.getDao();

        UUID mainTesterID = dao.create(testerEClass, Payload.map("name", "MainTester"), DAO.QueryCustomizer.<UUID>builder().build())
                .getAs(UUID.class, ID_KEY);

        Function<String, UUID> createTarget = name ->
                dao.createNavigationInstanceAt(mainTesterID, targetsEReference, Payload.map("name", name, "bool", false),
                                               DAO.QueryCustomizer.<UUID>builder().build())
                        .getAs(UUID.class, ID_KEY);
        Consumer<UUID> flipTargetsBoolValue = id -> dao.update(testerEClass, Payload.map(ID_KEY, id, "bool", true),
                                                     DAO.QueryCustomizer.<UUID>builder().build());

        UUID target1 = createTarget.apply("target1");
        UUID target2 = createTarget.apply("target2");
        UUID target3 = createTarget.apply("target3");

        String assertMessage = "%1$f <= %2$f && %2$f <= %3$f";

        Double trueTargetScale = dao.getByIdentifier(testerEClass, mainTesterID).orElseThrow().getAs(Double.class, "trueTargetScale");
        assertEquals(0., trueTargetScale);

        flipTargetsBoolValue.accept(target1);

        trueTargetScale = dao.getByIdentifier(testerEClass, mainTesterID).orElseThrow().getAs(Double.class, "trueTargetScale");
        assertTrue(0.3 <= trueTargetScale && trueTargetScale <= 0.4, String.format(assertMessage, 0.3, trueTargetScale, 0.4));

        flipTargetsBoolValue.accept(target2);

        trueTargetScale = dao.getByIdentifier(testerEClass, mainTesterID).orElseThrow().getAs(Double.class, "trueTargetScale");
        assertTrue(0.6 <= trueTargetScale && trueTargetScale <= 0.7, String.format(assertMessage, 0.6, trueTargetScale, 0.7));

        flipTargetsBoolValue.accept(target3);

        trueTargetScale = dao.getByIdentifier(testerEClass, mainTesterID).orElseThrow().getAs(Double.class, "trueTargetScale");
        assertEquals(1., trueTargetScale);
    }

    @Test
    public void testGenericIntegerDivision(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();

        builder.addEntity("Tester")
                .withAttribute("Double", "td") // terminable
                .withAttribute("Double", "ntd") // non-terminable
                .withProperty("Double", "_td", "1 / 4") // terminable
                .withProperty("Double", "_ntd", "1 / 9"); // non-terminable

        builder.addBoundOperation("Tester", "dOp")
                .withBody("var demo::types::Double td = 1 / 4\n" +
                          "var demo::types::Double ntd = 1 / 9\n" +
                          "return new demo::entities::Tester(td = td, ntd = ntd)")
                .withOutput("Tester", cardinality(1, 1));

        runtimeFixture.setValidateModels(true);
        runtimeFixture.init(builder.build(), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "Dao initialized");

        AsmUtils asmUtils = runtimeFixture.getAsmUtils();
        EClass testerEClass = asmUtils.getClassByFQName(DTO + "Tester").orElseThrow();

        DAO<UUID> dao = runtimeFixture.getDao();

        UUID testerID = dao.create(testerEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder().build())
                .getAs(UUID.class, ID_KEY);

        Payload dOpResult = runtimeFixture.getOperationImplementations().get("dOp")
                .apply(Payload.map(ID_KEY, testerID))
                .getAsPayload(OUTPUT);

        Double ntd = dOpResult.getAs(Double.class, "ntd");
        Double _ntd = dOpResult.getAs(Double.class, "_ntd");

        String assertMessage = "%1$f <= %2$f && %2$f <= %3$f";

        assertEquals(0.25, dOpResult.getAs(Double.class, "td"));
        assertTrue(0.11 <= ntd && ntd <= 0.12, String.format(assertMessage, 0.11, ntd, 0.12));
        assertEquals(0.25, dOpResult.getAs(Double.class, "_td"));
        assertTrue(0.11 <= _ntd && _ntd <= 0.12, String.format(assertMessage, 0.11, _ntd, 0.12));
    }

    @Test
    public void testDoubleDivision(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();

        builder.addEntity("Tester")
                .withAttribute("Double", "a")
                .withAttribute("Double", "b")
                .withProperty("Double", "ntd", "self.a / self.b");

        builder.addBoundOperation("Tester", "dOp")
                .withBody("return new demo::entities::Tester(a = 1, b = 9)")
                .withOutput("Tester", cardinality(1, 1));

        runtimeFixture.setValidateModels(true);
        runtimeFixture.init(builder.build(), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "Dao initialized");

        Double ntd = run(runtimeFixture, "dOp", Payload.empty()).getAsPayload(OUTPUT).getAs(Double.class, "ntd");
        assertTrue(0.11 <= ntd && ntd <= 0.12, String.format("%1$f <= %2$f && %2$f <= %3$f", 0.11, ntd, 0.12));

    }

}
