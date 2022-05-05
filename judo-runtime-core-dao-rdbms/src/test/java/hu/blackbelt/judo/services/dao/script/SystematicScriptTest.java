package hu.blackbelt.judo.services.dao.script;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.ScriptTestEntityBuilder;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.ScriptTestMappedTransferObjectBuilder;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.ScriptTestUnmappedTransferObjectBuilder;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceSingetonExtension;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator.SubjectCollectionCreator;
import hu.blackbelt.judo.services.dao.script.subject.creator.impl.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class SystematicScriptTest {

    private final static Logger logger = LoggerFactory.getLogger(SystematicScriptTest.class);

    public static final String OUTPUT = "output";

    // PREP //////////////////////////////////////////

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
        daoFixture.dropDatabase();
    }

    /**
     * <p>Creates fixture with testname and subject name then builds model with
     * given unbound operation.</p>
     * <p>Assertions can be run after fixture is returned.</p>
     * <p>Database must be purged after use.</p>
     * <p>In the scrip the following can be used to automatically replaced:</p>
     * <ul>
     *     <li>&lt&lt PREP >> (without spaces)</li>
     *     <li>&lt&lt RETURNTYPE >> (without spaces)</li>
     *     <li>&lt&lt USE >> (without spaces)</li>
     *     <li>&lt&lt OBJECT >> (without spaces)</li>
     * </ul>
     *
     * @param sc                     Subject that will be tested
     * @param unboundOperationBody   Script
     * @param hasReturn               is return statement included in script
     * @param rdbmsDatasourceFixture test parameter
     * @param fixture                test parameter
     * @return Payload
     * @see RdbmsDaoFixture#dropDatabase()
     */
    private Payload testSubject(SubjectCreator sc, String unboundOperationBody, boolean hasReturn, RdbmsDatasourceFixture rdbmsDatasourceFixture, RdbmsDaoFixture fixture) {
        PsmTestModelBuilder modelBuilder = sc.getPsmTestModelBuilder();

        String objectType = sc.getReturnType().replace("[]", "");
        String script = unboundOperationBody
                .replace("<<PREP>>", sc.getPrep())
                .replace("<<RETURNTYPE>>", sc.getReturnType())
                .replace("<<USE>>", sc.getUse())
                .replace("<<OBJECT>>", objectType);

        logger.debug("Return type: {} ", sc.getReturnType());
        logger.debug("Return type (EMF): {} ", parseScriptTypeToEmfType(sc.getReturnType()));
        logger.debug("Script:\n {}", script);

        PsmTestModelBuilder.ScriptTestOperationBuilder operation =
                modelBuilder.addUnboundOperation("init").withBody(script);
        if (hasReturn) {
            operation.withOutput(parseScriptTypeToEmfType(objectType), cardinality(0, sc.getReturnType().endsWith("[]") ? -1 : 1)
            );
        }

        fixture.init(modelBuilder.build(), rdbmsDatasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        return run(fixture);
    }

    /**
     * Adds new attribute to given subjects entity, mapped to or unmapped to.
     *
     * @param sc            Subject
     * @param elementName   Entity, mapped to- or unmapped to's name
     * @param type          Attribute type
     * @param attributeName Attribute name
     */
    private static void addAttribute(final SubjectCreator sc,
                                     final String elementName,
                                     final String type,
                                     final String attributeName) {
        final Optional<ScriptTestEntityBuilder> entity =
                sc.getPsmTestModelBuilder().getEntity(elementName);

        final Optional<ScriptTestMappedTransferObjectBuilder> mappedTransferObject =
                sc.getPsmTestModelBuilder().getMappedTransferObject(elementName);
        final Optional<ScriptTestEntityBuilder> entityFromTO =
                sc.getPsmTestModelBuilder().getEntityFromTO(elementName);

        final Optional<ScriptTestUnmappedTransferObjectBuilder> unmappedTransferObject =
                sc.getPsmTestModelBuilder().getUnmappedTransferObject(elementName);

        if (entity.isPresent()) {
            entity.get().withAttribute(type, attributeName);
        } else if (mappedTransferObject.isPresent() && entityFromTO.isPresent()) {
            mappedTransferObject.get().withAttribute(type, attributeName);
            entityFromTO.get().withAttribute(type, attributeName);
        } else if (unmappedTransferObject.isPresent()) {
            unmappedTransferObject.get().withAttribute(type, attributeName);
        } else {
            fail("Unable to find builder for " + elementName);
        }
    }

    private String parseScriptTypeToEmfType(String scriptType) {
        return scriptType.split("::")[2].replace("[]", "");
    }

    // TESTS /////////////////////////////////////////

    private static Stream<SubjectCreator> testAssignToVariableParameter() {
        final List<SubjectCreator> creators = Subjects.getObjects();
        creators.addAll(Subjects.getCollections());
        return creators.stream();
    }

    @ParameterizedTest
    @MethodSource("testAssignToVariableParameter")
    public void testAssignCollectionToVariable(SubjectCreator sc, RdbmsDaoFixture fixture, RdbmsDatasourceFixture rdbmsDatasourceFixture) {
        log.info("Subject: {}", sc.getName());
        final String script = "\n" +
                "<<PREP>>\n" +
                "\n" +
                "var <<RETURNTYPE>> _e = <<USE>>\n" +
                "\n" +
                (sc.getReturnType().contains("demo::types") ? "" : "return _e\n");

        if (sc instanceof SubjectCollectionCreator) {
            assertEquals(
                    ((SubjectCollectionCreator) sc).getCollectionSize(),
                    testSubject(sc, script, true, rdbmsDatasourceFixture, fixture)
                            .getAsCollectionPayload(OUTPUT).size()
            );
        } else {
            if (sc.getReturnType().contains("demo::types"))
                testSubject(sc, script, false, rdbmsDatasourceFixture, fixture);
            else
                assertNotNull(testSubject(sc, script, true, rdbmsDatasourceFixture, fixture).getAsPayload(OUTPUT));
        }
    }

    private static Stream<SubjectCollectionCreator> testCollectionsInForStatementParameter() {
        return Subjects.getCollections().stream().filter(sc ->
                !(sc instanceof SubjectNewMC || sc instanceof SubjectNewUC));
    }

    @ParameterizedTest
    @MethodSource("testCollectionsInForStatementParameter")
    public void testCollectionsInForStatement(SubjectCollectionCreator sc, RdbmsDaoFixture fixture, RdbmsDatasourceFixture rdbmsDatasourceFixture) {
        log.info("Subject: {}", sc.getName());
        final String script = "\n" +
                "<<PREP>>\n" +
                "\n" +
                "for(_e in <<USE>>) {  }\n";

        testSubject(sc, script, false, rdbmsDatasourceFixture, fixture);
    }

    private static Stream<SubjectCreator> testDeleteParameter() {
        return Stream.concat(Subjects.getObjects().stream().filter(sc ->
                !(sc instanceof SubjectNewM ||
                  sc instanceof SubjectNewU ||
                  sc instanceof SubjectBoundAttributeM2P ||
                  sc instanceof SubjectUnboundAttributeM2P ||
                  sc instanceof SubjectUnboundAttributeU2P ||
                  sc instanceof SubjectVariableP ||
                  sc instanceof SubjectRoundP)),
                Subjects.getCollections().stream().filter(sc ->
                        !(sc instanceof SubjectNewMC ||
                            sc instanceof SubjectNewUC)));
    }

    @ParameterizedTest
    @MethodSource("testDeleteParameter")
    public void testDelete(SubjectCreator sc, RdbmsDaoFixture fixture, RdbmsDatasourceFixture rdbmsDatasourceFixture) {
        log.info("Subject: {}", sc.getName());
        final String script = "\n" +
                "<<PREP>>\n" +
                "\n" +
                "delete <<USE>>\n";

        testSubject(sc, script, false, rdbmsDatasourceFixture, fixture);
    }

    private static Stream<SubjectCollectionCreator> testAddObjectToCollectionParameter() {
        return Subjects.getCollections().stream().filter(sc ->
                !(sc instanceof SubjectSelectAllMC ||
                        sc instanceof SubjectBoundRelationMC2MC ||
                        sc instanceof SubjectUnboundRelationMC2MC ||
                        sc instanceof  SubjectUnboundRelationUC2MC ||
                        sc instanceof  SubjectUnboundRelationMC2UC ||
                        sc instanceof  SubjectUnboundRelationUC2UC ||
                        sc instanceof SubjectNewMC ||
                        sc instanceof SubjectNewUC ||
                        sc instanceof SubjectSortedMC2MC ||
                        sc instanceof SubjectOperationMC ||
                        sc instanceof SubjectOperationUC));
    }

    @ParameterizedTest
    @MethodSource("testAddObjectToCollectionParameter")
    public void testAddObjectToCollection(SubjectCollectionCreator sc, RdbmsDaoFixture fixture, RdbmsDatasourceFixture rdbmsDatasourceFixture) {
        log.info("Subject: {}", sc.getName());
        final String script = "\n" +
                "<<PREP>>\n" +
                "\n" +
                "var <<OBJECT>> _o = new <<OBJECT>>()\n" +
                "var <<OBJECT>> _o1 = new <<OBJECT>>()\n" +
                "\n" +
                "<<USE>> += _o\n" +
                "<<USE>> += _o1\n" +
                "<<USE>> += new <<OBJECT>>()\n" +
                "\n" +
                "return <<USE>>\n";

        assertEquals(
                sc.getCollectionSize() + 3,
                testSubject(sc, script, true, rdbmsDatasourceFixture, fixture)
                        .getAsCollectionPayload(OUTPUT).size()
        );
    }

    private static Stream<SubjectCollectionCreator> testAddAllToCollectionParameter() {
        return Subjects.getCollections().stream().filter(sc ->
                !(sc instanceof SubjectSelectAllMC ||
                        sc instanceof SubjectBoundRelationMC2MC ||
                        sc instanceof SubjectUnboundRelationMC2MC ||
                        sc instanceof  SubjectUnboundRelationUC2MC ||
                        sc instanceof  SubjectUnboundRelationMC2UC ||
                        sc instanceof  SubjectUnboundRelationUC2UC ||
                        sc instanceof SubjectNewMC ||
                        sc instanceof SubjectNewUC ||
                        sc instanceof SubjectSortedMC2MC ||
                        sc instanceof SubjectOperationMC ||
                        sc instanceof SubjectOperationUC));
    }

    @ParameterizedTest
    @MethodSource("testAddAllToCollectionParameter")
    public void testAddAllToCollection(SubjectCollectionCreator sc, RdbmsDaoFixture fixture, RdbmsDatasourceFixture rdbmsDatasourceFixture) {
        log.info("Subject: {}", sc.getName());
        final String script = "\n" +
                "<<PREP>>\n" +
                "\n" +
                "<<USE>> = new <<RETURNTYPE>>{\n" +
                "\tnew <<OBJECT>>(),\n" +
                "\tnew <<OBJECT>>(),\n" +
                "\tnew <<OBJECT>>()\n" +
                "}\n" +
                "\n" +
                "return <<USE>>\n";

        assertEquals(3,
                testSubject(sc, script, true, rdbmsDatasourceFixture, fixture)
                        .getAsCollectionPayload(OUTPUT).size()
        );
    }

    private static Stream<SubjectCollectionCreator> testRemoveFromCollectionParameter() {
        return Subjects.getCollections().stream().filter(sc ->
                !(sc instanceof SubjectSelectAllMC ||
                        sc instanceof SubjectBoundRelationMC2MC ||
                        sc instanceof SubjectUnboundRelationMC2MC ||
                        sc instanceof  SubjectUnboundRelationUC2MC ||
                        sc instanceof  SubjectUnboundRelationMC2UC ||
                        sc instanceof  SubjectUnboundRelationUC2UC ||
                        sc instanceof SubjectNewMC ||
                        sc instanceof SubjectNewUC ||
                        sc instanceof SubjectSortedMC2MC ||
                        sc instanceof SubjectOperationMC ||
                        sc instanceof SubjectOperationUC
                ));

    }

    @ParameterizedTest
    @MethodSource("testRemoveFromCollectionParameter")
    public void testRemoveFromCollection(SubjectCollectionCreator sc, RdbmsDaoFixture fixture, RdbmsDatasourceFixture rdbmsDatasourceFixture) {
        log.info("Subject: {}", sc.getName());
        final String script = "\n" +
                "<<PREP>>\n" +
                "\n" +
                "var <<OBJECT>> _o = new <<OBJECT>>()\n" +
                "var <<OBJECT>> _o1 = new <<OBJECT>>()\n" +
                "\n" +
                "<<USE>> += _o\n" +
                "<<USE>> += _o1\n" +
                "\n" +
                "<<USE>> -= _o\n" +
                "<<USE>> -= _o1\n" +
                "\n" +
                "return <<USE>>\n";

        assertEquals(
                sc.getCollectionSize(),
                testSubject(sc, script, true, rdbmsDatasourceFixture, fixture)
                        .getAsCollectionPayload(OUTPUT).size()
        );
    }

    private static class TestClass {
        private final SubjectCreator sc;
        private final boolean isReturn;

        private TestClass(SubjectCreator sc, boolean isReturn) {
            this.sc = sc;
            this.isReturn = isReturn;
        }

        @Override
        public String toString() {
            return sc.toString();
        }
    }

    private static Stream<TestClass> testSetAttributeParameter() {
        return Subjects.getObjects().stream()
                .filter(sc ->
                        !(sc instanceof SubjectNewU ||
                                sc instanceof SubjectNewM ||
                                sc instanceof SubjectRoundP ||
                                sc instanceof SubjectVariableP ||
                                sc instanceof SubjectUnboundAttributeU2P ||
                                sc instanceof SubjectUnboundAttributeM2P ||
                                sc instanceof SubjectBoundAttributeM2P))
                .map(sc -> new TestClass(sc,
                        !(sc instanceof SubjectOperationU ||
                          sc instanceof SubjectOperationM ||
                          sc instanceof SubjectAnyUC2U || // TODO: Ask Joe
                          sc instanceof SubjectUnboundRelationU2U || // TODO: Ask Joe
                          sc instanceof SubjectUnboundRelationM2U || // TODO: Ask Joe
                          sc instanceof SubjectUnboundRelationU2M || // TODO: Ask Joe
                          sc instanceof SubjectVariableU))); // TODO: Ask Joe
    }

    @ParameterizedTest
    @MethodSource("testSetAttributeParameter")
    public void testSetAttribute(TestClass tc, RdbmsDaoFixture fixture, RdbmsDatasourceFixture rdbmsDatasourceFixture) {
        log.info("Subject: {}", tc.sc.getName());
        final String script = "\n" +
                "<<PREP>>\n" +
                "\n" +
                "var demo::types::Double _o = 3.14\n" +
                "\n" +
                "<<USE>>.test_attribute = _o\n" +
                "<<USE>>.test_attribute = 3.14\n" +
                "\n" +
                (tc.isReturn ? "return <<USE>>\n" : "");

        addAttribute(tc.sc, parseScriptTypeToEmfType(tc.sc.getReturnType()), "Double", "test_attribute");

        final Payload payload = testSubject(tc.sc, script, tc.isReturn, rdbmsDatasourceFixture, fixture);

        if (tc.isReturn)
            assertEquals(3.14, (double) payload.getAsPayload(OUTPUT).getAs(Double.class, "test_attribute"));
    }

    private static Stream<SubjectCreator> testUnsetAttributeParameter() {
        return Subjects.getObjects().stream().filter(sc ->
                !(sc instanceof SubjectNewU ||
                        sc instanceof SubjectNewM ||
                        sc instanceof SubjectRoundP ||
                        sc instanceof SubjectVariableP ||
                        sc instanceof SubjectUnboundAttributeU2P ||
                        sc instanceof SubjectUnboundAttributeM2P ||
                        sc instanceof SubjectBoundAttributeM2P ||
                        sc instanceof SubjectAnyMC2M ||
                        sc instanceof SubjectAnyUC2U ||
                        sc instanceof SubjectOperationM ||
                        sc instanceof SubjectOperationU));
    }

    @ParameterizedTest
    @MethodSource("testUnsetAttributeParameter")
    public void testUnsetAttribute(SubjectCreator sc, RdbmsDaoFixture fixture, RdbmsDatasourceFixture rdbmsDatasourceFixture) {
        log.info("Subject: {}", sc.getName());
        final String script = "\n" +
                "<<PREP>>\n" +
                "\n" +
                "var demo::types::Double _o = 3.14\n" +
                "\n" +
                "<<USE>>.test_attribute = _o\n" +
                "<<USE>>.test_attribute = 3.14\n" +
                "\n" +
                "unset <<USE>>.test_attribute\n";

        addAttribute(sc, parseScriptTypeToEmfType(sc.getReturnType()), "Double", "test_attribute");

        testSubject(sc, script, false, rdbmsDatasourceFixture, fixture);
    }

    private static Stream<SubjectCreator> testSetSingleRelationParameter() {
        return Subjects.getObjects().stream().filter(sc ->
                (sc instanceof SubjectBoundRelationM2M ||
                        sc instanceof SubjectUnboundRelationM2M ||
                        sc instanceof SubjectUnboundRelationM2U ||
                        sc instanceof SubjectUnboundRelationU2M ||
                        sc instanceof SubjectUnboundRelationU2U));
    }

    @ParameterizedTest
    @MethodSource("testSetSingleRelationParameter")
    public void testSetSingleRelation(SubjectCreator sc, RdbmsDaoFixture fixture, RdbmsDatasourceFixture rdbmsDatasourceFixture) {
        log.info("Subject: {}", sc.getName());
        final String script = "\n" +
                "<<PREP>>\n" +
                "\n" +
                "var <<OBJECT>> _o = new <<OBJECT>>()\n" +
                "\n" +
                "<<USE>> = _o\n" +
                "<<USE>> = new <<OBJECT>>()\n" +
                "\n" +
                "return <<USE>>\n";

        assertNotNull(testSubject(sc, script, true, rdbmsDatasourceFixture, fixture).getAsPayload(OUTPUT));
    }

    private static Stream<SubjectCreator> testUnsetSingleRelationParameter() {
        return Subjects.getObjects().stream().filter(sc ->
                (sc instanceof SubjectBoundRelationM2M ||
                        sc instanceof SubjectUnboundRelationM2M ||
                        sc instanceof SubjectUnboundRelationM2U ||
                        sc instanceof SubjectUnboundRelationU2M ||
                        sc instanceof SubjectUnboundRelationU2U));
    }

    @ParameterizedTest
    @MethodSource("testUnsetSingleRelationParameter")
    public void testUnsetSingleRelation(SubjectCreator sc, RdbmsDaoFixture fixture, RdbmsDatasourceFixture rdbmsDatasourceFixture) {
        final String script = "\n" +
                "<<PREP>>\n" +
                "\n" +
                "var <<OBJECT>> _o = new <<OBJECT>>()\n" +
                "\n" +
                "<<USE>> = _o\n" +
                "<<USE>> = new <<OBJECT>>()\n" +
                "\n" +
                "unset <<USE>>\n";

        testSubject(sc, script, false, rdbmsDatasourceFixture, fixture);
    }

    private static Stream<SubjectCollectionCreator> testSetMultiRelationParameter() {
        return Subjects.getCollections().stream().filter(sc ->
                (sc instanceof SubjectBoundRelationM2MC ||
                        sc instanceof SubjectUnboundRelationM2MC ||
                        sc instanceof SubjectUnboundRelationM2UC ||
                        sc instanceof SubjectUnboundRelationU2MC ||
                        sc instanceof SubjectUnboundRelationU2UC));
    }

    @ParameterizedTest
    @MethodSource("testSetMultiRelationParameter")
    public void testSetMultiRelation(SubjectCollectionCreator sc, RdbmsDaoFixture fixture, RdbmsDatasourceFixture rdbmsDatasourceFixture) {
        log.info("Subject: {}", sc.getName());
        final String script = "\n" +
                "<<PREP>>\n" +
                "\n" +
                "var <<OBJECT>> _o = new <<OBJECT>>()\n" +
                "var <<OBJECT>> _o1 = new <<OBJECT>>()\n" +
                "\n" +
                "<<USE>> += _o\n" +
                "<<USE>> += _o1\n" +
                "\n" +
                "return <<USE>>\n";

        assertEquals(
                sc.getCollectionSize() + 2,
                testSubject(sc, script, true, rdbmsDatasourceFixture, fixture)
                        .getAsCollectionPayload(OUTPUT).size()
        );
    }

    private static Stream<SubjectCollectionCreator> testUnsetMultiRelationParameter() {
        return Subjects.getCollections().stream().filter(sc ->
                (sc instanceof SubjectBoundRelationM2MC ||
                        sc instanceof SubjectUnboundRelationM2MC ||
                        sc instanceof SubjectUnboundRelationM2UC ||
                        sc instanceof SubjectUnboundRelationU2MC ||
                        sc instanceof SubjectUnboundRelationU2UC));
    }

    @ParameterizedTest
    @MethodSource("testUnsetMultiRelationParameter")
    public void testUnsetMultiRelation(SubjectCollectionCreator sc, RdbmsDaoFixture fixture, RdbmsDatasourceFixture rdbmsDatasourceFixture) {
        log.info("Subject: {}", sc.getName());
        final String script = "\n" +
                "<<PREP>>\n" +
                "\n" +
                "<<USE>> += new <<OBJECT>>()\n" +
                "<<USE>> += new <<OBJECT>>()\n" +
                "\n" +
                "unset <<USE>>\n" +
                "\n" +
                "return <<USE>>\n";

        assertEquals(
                0,
                testSubject(sc, script, true, rdbmsDatasourceFixture, fixture)
                        .getAsCollectionPayload(OUTPUT).size()
        );
    }

    private static Stream<SubjectCreator> testOperationParameterParameter() {
        final List<SubjectCreator> creators = Subjects.getObjects();
        creators.addAll(Subjects.getCollections());
        return creators.stream().filter(sc ->
                !(     sc instanceof SubjectVariableP ||
                        sc instanceof SubjectRoundP ||
                        sc instanceof SubjectBoundAttributeM2P ||
                        sc instanceof SubjectUnboundAttributeM2P ||
                        sc instanceof SubjectUnboundAttributeU2P));

    }

    @ParameterizedTest
    @MethodSource("testOperationParameterParameter")
    public void testOperationParameter(SubjectCreator sc, RdbmsDaoFixture fixture, RdbmsDatasourceFixture rdbmsDatasourceFixture) {
        log.info("Subject: {}", sc.getName());
        final boolean isCollection = sc instanceof SubjectCollectionCreator;
        final String script = "\n" +
                "<<PREP>>\n" +
                "\n" +
                "return demo::services::UnboundServices.operation(<<USE>>)\n";

        final String name = parseScriptTypeToEmfType(sc.getReturnType());
        sc.getPsmTestModelBuilder().addUnboundOperation("operation")
                .withInput(name, "_e", cardinality(1, isCollection ? -1 : 1))
                .withBody("var " + sc.getReturnType() + " _e1 = mutable _e\n" +
                        "\n" +
                        "return _e1\n")
                .withOutput(parseScriptTypeToEmfType(sc.getReturnType()), cardinality(0, isCollection ? -1 : 1));

        logger.debug("Parameter: \n\t" + name + (isCollection ? "[]" : "") + " _e");
        logger.debug("Body: \n\tvar " + sc.getReturnType() + " _e1 = _e\n");

        if (isCollection) {
            assertEquals(
                    ((SubjectCollectionCreator) sc).getCollectionSize(),
                    testSubject(sc, script, true, rdbmsDatasourceFixture, fixture)
                            .getAsCollectionPayload(OUTPUT).size()
            );
        } else {
            assertNotNull(testSubject(sc, script, true, rdbmsDatasourceFixture, fixture).getAsPayload(OUTPUT));
        }
    }

    private static Stream<SubjectCreator> testUseInObjectCreationParameter() {
        final List<SubjectCreator> creators = Subjects.getObjects();
        creators.addAll(Subjects.getCollections());
        return creators.stream();

    }

    @ParameterizedTest
    @MethodSource("testUseInObjectCreationParameter")
    public void testUseInObjectCreation(SubjectCreator sc, RdbmsDaoFixture fixture, RdbmsDatasourceFixture rdbmsDatasourceFixture) {
        log.info("Subject: {}", sc.getName());
        String TEST_UTO_NAME = "TestUTO";
        String TEST_RELATION_OR_ATTRIBUTE_NAME = "_test";
        String script = "\n" +
                "<<PREP>>\n" +
                "\n" +
                "var demo::services::" + TEST_UTO_NAME + " _e = new demo::services::" + TEST_UTO_NAME +
                "(" + TEST_RELATION_OR_ATTRIBUTE_NAME + " = <<USE>>)\n";

        String name = parseScriptTypeToEmfType(sc.getReturnType());
        ScriptTestUnmappedTransferObjectBuilder unmappedTransferObjectBuilder =
                sc.getPsmTestModelBuilder().addUnmappedTransferObject(TEST_UTO_NAME);
        if (sc instanceof SubjectRoundP ||
                sc instanceof SubjectVariableP ||
                sc instanceof SubjectBoundAttributeM2P ||
                sc instanceof SubjectUnboundAttributeM2P ||
                sc instanceof SubjectUnboundAttributeU2P) {
            unmappedTransferObjectBuilder.withAttribute(name, TEST_RELATION_OR_ATTRIBUTE_NAME);
        } else {
            boolean isCollection = sc instanceof SubjectCollectionCreator;
            unmappedTransferObjectBuilder
                    .withRelation(name, TEST_RELATION_OR_ATTRIBUTE_NAME, cardinality(0, isCollection ? -1 : 1));
        }
        testSubject(sc, script, false, rdbmsDatasourceFixture, fixture);
    }

// SUBJECTS //////////////////////////////////////

    private interface Subjects {

        // MC - Mapped Transfer Object Collection
        // UC - Unmapped Transfer Object Collection
        static List<SubjectCollectionCreator> getCollections() {
            return new ArrayList<>(asList(
                    new SubjectSelectAllMC(),
                    new SubjectSortedMC2MC(),
                    new SubjectBoundRelationMC2MC(),
                    new SubjectUnboundRelationMC2MC(),
                    new SubjectUnboundRelationUC2MC(),
                    new SubjectBoundRelationM2MC(),
                    new SubjectUnboundRelationM2MC(),
                    new SubjectUnboundRelationU2MC(),
                    new SubjectOperationMC(),
                    new SubjectVariableMC(),
                    new SubjectNewMC(),
                    //new SubjectSortedUC2UC(), TODO -> default sorting is not supported, TODO not default sort (TODOTODO sort by ...)
                    new SubjectUnboundRelationMC2UC(),
                    new SubjectUnboundRelationUC2UC(),
                    new SubjectUnboundRelationM2UC(),
                    new SubjectUnboundRelationU2UC(),
                    new SubjectOperationUC(),
                    new SubjectVariableUC(),
                    new SubjectNewUC()
            ));
        }

        // M - Mapped Transfer Object
        // U - Unmapped Transfer Object
        // P - Primitive
        static List<SubjectCreator> getObjects() {
            return new ArrayList<>(asList(
                    new SubjectAnyMC2M(),
                    new SubjectBoundRelationM2M(),
                    new SubjectUnboundRelationM2M(),
                    new SubjectUnboundRelationU2M(),
                    new SubjectOperationM(),
                    new SubjectOperationU(),
                    new SubjectVariableM(),
                    new SubjectNewM(),
                    new SubjectAnyUC2U(),
                    new SubjectUnboundRelationU2U(),
                    new SubjectUnboundRelationM2U(),
                    new SubjectVariableU(),
                    new SubjectNewU(),
                    new SubjectBoundAttributeM2P(),
                    new SubjectUnboundAttributeM2P(),
                    new SubjectUnboundAttributeU2P(),
                    new SubjectVariableP(),
                    new SubjectRoundP()
            ));
        }
    }

}
