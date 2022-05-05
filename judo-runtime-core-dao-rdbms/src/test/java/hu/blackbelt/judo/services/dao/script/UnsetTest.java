package hu.blackbelt.judo.services.dao.script;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.fixture.*;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.UUID;
import java.util.function.Function;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class UnsetTest {

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
    public void testUnset(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();

        builder.addEntity("Tester")
                .withAttribute("String", "name")
                .withRelation("Tester", "t", cardinality(0, 1));

        builder.addUnboundOperation("operation")
                .withInput("Tester", "input", cardinality(0, 1))
                .withBody("var demo::entities::Tester tester = mutable input\n" +
                          "if (tester.t!isDefined()) {\n" +
                          "  unset tester.t\n" +
                          "  tester.name = input.t.name\n" +
                          "}\n" +
                          "return tester")
                .withOutput("Tester", cardinality(0, 1));

        daoFixture.init(builder.build(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao initialized");

        EClass testerEClass = daoFixture.getAsmUtils().getClassByFQName(DTO + "Tester").orElseThrow();
        EReference tEReference = testerEClass.getEAllReferences().stream().filter(r -> "t".equals(r.getName())).findAny().orElseThrow();

        DAO<UUID> dao = daoFixture.getDao();

        UUID testerMainID = dao.create(testerEClass, Payload.map("name", "TesterMain"), DAO.QueryCustomizer.<UUID>builder().build())
                .getAs(UUID.class, ID_KEY);
        dao.createNavigationInstanceAt(testerMainID, tEReference, Payload.map("name", "TesterTarget"), DAO.QueryCustomizer.<UUID>builder().build());

        Payload emptyResult = run(daoFixture, "operation", Payload.empty()).getAsPayload(OUTPUT);
        assertThat(emptyResult, equalTo(Payload.empty()));

        assertThat(dao.getByIdentifier(testerEClass, testerMainID).orElseThrow().getAs(String.class, "name"), equalTo("TesterMain"));
        Payload result = run(daoFixture, "operation", Payload.map("input", Payload.map(ID_KEY, testerMainID))).getAsPayload(OUTPUT);
        assertThat(result.getAsPayload("t"), nullValue());
        assertThat(result.getAs(String.class, "name"), nullValue());
        assertThat(dao.getByIdentifier(testerEClass, testerMainID).orElseThrow().getAs(String.class, "name"), nullValue());

    }

}
