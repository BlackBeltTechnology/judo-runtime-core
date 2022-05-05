package hu.blackbelt.judo.services.dao.script;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.fixture.*;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class FQNameTest {

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
    public void testUsingNonFQNames(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();

        builder.addEntity("AncientTester");
        builder.addEntity("Tester")
                .withSuperType("AncientTester")
                .withAttribute("String", "name");
        builder.addEntity("TestResult")
                .withRelation("Tester", "singleDeclarationWithValue", cardinality(0, 1))
                .withRelation("Tester", "multiDeclarationWithValue", cardinality(0, -1))
                .withRelation("Tester", "multiDeclarationWithValue1", cardinality(0, -1))
                .withRelation("Tester", "lambdaWithVariableSameAsType", cardinality(0, 1))
                .withRelation("Tester", "lambdaWithVariableSameAsTypeOther", cardinality(0, -1))
                .withRelation("AncientTester", "cast", cardinality(0, 1));

        builder.addBoundOperation("Tester", "operation")
                .withOutput("TestResult", cardinality(1, 1))
                .withBody("var TestResult tc = new TestResult()\n\n" +

                          "var Tester t\n\n" +

                          "var Tester[] ts\n\n" +

                          "var Tester t1 = new Tester(name = 'apple')\n" +
                          "tc.singleDeclarationWithValue = t1\n\n" +

                          "var Tester[] ts1 = new Tester[] {}\n\n" +

                          "var Tester[] ts2 = new Tester[] { new Tester(name = 'pear') }\n" +
                          "tc.multiDeclarationWithValue = ts2\n\n" +

                          "var Tester[] ts3 = new Tester[] { new Tester(name = 'peach'), new Tester(name = 'strawberry') }\n" +
                          "tc.multiDeclarationWithValue1 = ts3\n\n" +

                          "var Tester t2 = Tester!filter(Tester | Tester.name == 'apple')!any()\n" +
                          "tc.lambdaWithVariableSameAsType = t2\n\n" +

                          "var Tester t3 = new Tester(name = 'cherry')\n" +
                          "var AncientTester at = t3 as AncientTester\n" +
                          "tc.cast = at\n\n" +

                          "var Tester Tester = new Tester(name = 'grape')\n" +
                          "var demo::entities::Tester[] ts4 = demo::entities::Tester!filter(tt | tt.name!length() == Tester.name!length())\n" +
                          "tc.lambdaWithVariableSameAsTypeOther = ts4\n\n" +

                          "return tc"
                );

        builder.addUnboundOperation("op")
                .withOutput("TestResult", cardinality(1, 1))
                .withBody("var demo::entities::Tester t = new demo::entities::Tester()\n" +
                          "return t.operation()");

        daoFixture.init(builder.build(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao initialized");

        UUID testResultID = run(daoFixture, "op", Payload.empty())
                .getAsPayload(OUTPUT)
                .getAs(UUID.class, ID_KEY);

        AsmUtils asmUtils = daoFixture.getAsmUtils();

        EClass testerEClass = asmUtils.getClassByFQName(DTO + "Tester").get();
        EClass testResultEClass = asmUtils.getClassByFQName(DTO + "TestResult").get();

        Map<String, EReference> testResultEReferences = testResultEClass.getEAllReferences().stream()
                .collect(Collectors.toMap(r -> r.getName(), r -> r));

        DAO<UUID> dao = daoFixture.getDao();

        List<Payload> singleDeclarationWithValue =
                dao.getNavigationResultAt(testResultID, testResultEReferences.get("singleDeclarationWithValue"));
        assertThat(singleDeclarationWithValue.size(), equalTo(1));
        assertThat(singleDeclarationWithValue.get(0).getAs(String.class, "name"), equalTo("apple"));

        List<Payload> multiDeclarationWithValue =
                dao.getNavigationResultAt(testResultID, testResultEReferences.get("multiDeclarationWithValue"));
        assertThat(multiDeclarationWithValue.size(), equalTo(1));
        assertThat(multiDeclarationWithValue.get(0).getAs(String.class, "name"), equalTo("pear"));

        List<Payload> multiDeclarationWithValue1 =
                dao.getNavigationResultAt(testResultID, testResultEReferences.get("multiDeclarationWithValue1"));
        assertThat(multiDeclarationWithValue1.size(), equalTo(2));
        assertThat(multiDeclarationWithValue1.stream()
                           .map(p -> p.getAs(String.class, "name"))
                           .collect(Collectors.toList()),
                   hasItems("peach", "strawberry"));

        List<Payload> lambdaWithVariableSameAsType =
                dao.getNavigationResultAt(testResultID, testResultEReferences.get("lambdaWithVariableSameAsType"));
        assertThat(lambdaWithVariableSameAsType.size(), equalTo(1));
        assertThat(lambdaWithVariableSameAsType.get(0).getAs(String.class, "name"), equalTo("apple"));

        List<Payload> lambdaWithVariableSameAsTypeOther =
                dao.getNavigationResultAt(testResultID, testResultEReferences.get("lambdaWithVariableSameAsTypeOther"));
        assertThat(lambdaWithVariableSameAsTypeOther.size(), equalTo(3));
        assertThat(lambdaWithVariableSameAsTypeOther.stream()
                           .map(p -> p.getAs(String.class, "name"))
                           .collect(Collectors.toList()),
                   hasItems("grape", "apple", "peach"));

        List<Payload> cast = dao.getNavigationResultAt(testResultID, testResultEReferences.get("cast"));
        assertThat(cast.size(), equalTo(1));
        Payload castResult = cast.get(0);
        assertThat(castResult.getAs(String.class, "name"), nullValue()); // AncientTester
        UUID castResultID = castResult.getAs(UUID.class, ID_KEY);
        assertThat(dao.getByIdentifier(testerEClass, castResultID).get().getAs(String.class, "name"),
                   equalTo("cherry"));
    }

}
