package hu.blackbelt.judo.runtime.core.dao.rdbms.script;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.ModelBuilder;
import hu.blackbelt.judo.meta.esm.structure.*;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collection;
import java.util.function.Function;

import static hu.blackbelt.judo.meta.esm.operation.OperationType.STATIC;
import static hu.blackbelt.judo.meta.esm.operation.util.builder.OperationBuilders.newOperationBuilder;
import static hu.blackbelt.judo.meta.esm.operation.util.builder.OperationBuilders.newParameterBuilder;
import static hu.blackbelt.judo.meta.esm.structure.MemberType.*;
import static hu.blackbelt.judo.meta.esm.structure.RelationKind.AGGREGATION;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class TransientFeatureSetWithoutReloadTest {

    private static final String DTO = "demo._default_transferobjecttypes.entities.";
    public static final String OUTPUT = "output";
    public static final String ID_KEY = "__identifier";

    private static final StringType STRING_TYPE = TypeBuilders.newStringTypeBuilder().withName("String").withMaxLength(255).build();

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
    @Disabled
    public void test(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        // target
        TransferObjectType target = newTransferObjectTypeBuilder()
                .withName("Target")
                .withAttributes(createNameAttribute(TRANSIENT))
                .build();

        // tester entity
        DataMember nameAttribute = createNameAttribute(STORED);

        EntityType tester = newEntityTypeBuilder()
                .withName("Tester")
                .withAttributes(nameAttribute)
                .build();
        tester.setMapping(newMappingBuilder().withTarget(tester).build());

        // tester mapped to
        TransferObjectType mappedTester = newTransferObjectTypeBuilder()
                .withName("MappedTester")
                .withMappedEntity(tester)
                .withAttributes(createTransientAttribute(),
                                newDataMemberBuilder()
                                        .withName("name")
                                        .withRequired(true)
                                        .withDataType(STRING_TYPE)
                                        .withMemberType(MAPPED)
                                        .withBinding(nameAttribute)
                                        .build())
                .withRelations(createTransientRelationTo(target, "transientRelationSingle"),
                               createTransientRelationTo(target, "transientRelationMulti"))
                .build();

        // tester unmapped to
        TransferObjectType unmappedTester = newTransferObjectTypeBuilder()
                .withName("UnmappedTester")
                .withAttributes(createNameAttribute(TRANSIENT), createTransientAttribute())
                .withRelations(createTransientRelationTo(target, "transientRelationSingle"),
                               createTransientRelationTo(target, "transientRelationMulti"))
                .build();

        // test holder
        TransferObjectType testHolder = newTransferObjectTypeBuilder()
                .withName("TestHolder")
                .withRelations(createTransientRelationTo(mappedTester, "mt"),
                               createTransientRelationTo(unmappedTester, "ut"))
                .build();

        // test operation
        mappedTester.getOperations().add(
                newOperationBuilder()
                        .withBinding("")
                        .withName("operation")
                        .withOperationType(STATIC)
                        .withCustomImplementation(false)
                        .withOutput(newParameterBuilder()
                                            .withName("output")
                                            .withTarget(testHolder)
                                            .withLower(1).withUpper(1)
                                            .build())
                        .withBody("var MappedTester mt = new MappedTester(name = 'mt')\n" +
                                  "mt.transientAttribute = 'ta'\n" +
                                  "mt.transientRelationSingle = new Target(name = 't')\n" +
                                  "mt.transientRelationMulti += new Target(name = 't1')\n\n" +

                                  "var UnmappedTester ut = new UnmappedTester(name = 'ut')\n" +
                                  "ut.transientAttribute = 'ta'\n" +
                                  "ut.transientRelationSingle = new Target(name = 't')\n" +
                                  "ut.transientRelationMulti += new Target(name = 't1')\n\n" +

                                  "return new TestHolder(mt = mt, ut = ut)")
                        .build()
        );

        // model
        Model model = ModelBuilder.create()
                .withName("TestModel")
                .withElements(STRING_TYPE, tester, target, mappedTester, unmappedTester, testHolder)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        Payload result = daoFixture.getOperationImplementations()
                .get("operation")
                .apply(Payload.empty())
                .getAsPayload(OUTPUT);

        assertData(result, "mt");
        assertData(result, "ut");
    }

    private void assertData(Payload result, String parameter) {
        Payload to = result.getAsPayload(parameter);
        assertNotNull(to, String.format("Payload with name %s should not be null", parameter));
        assertEquals(parameter, to.getAs(String.class, "name"), String.format("%1$s's name in payload should be %1$s", parameter));
        assertEquals("ta", to.getAs(String.class, "transientAttribute"), parameter + "'s transient attribute's value should be: ta");
        Payload transientRelationSingle = to.getAsPayload("transientRelationSingle");
        assertNotNull(transientRelationSingle, "Transient single relation's payload should not be null in " + parameter);
        assertEquals("t", transientRelationSingle.getAs(String.class, "name"), "Single transient relation's target's name should be: t");
        Collection<Payload> transientRelationMulti = to.getAsCollectionPayload("transientRelationMulti");
        assertNotNull(transientRelationMulti, "Transient multi relation's collection payload should not be null");
        assertEquals(1, transientRelationMulti.size(), "Transient multi relation's collection payload's size should be: 1");
        assertEquals("t1", transientRelationMulti.iterator().next().getAs(String.class, "name"),
                     "Transient multi relation's target's name should be: t1");
    }

    private OneWayRelationMember createTransientRelationTo(TransferObjectType mappedTester, String name) {
        return newOneWayRelationMemberBuilder()
                .withName(name)
                .withTarget(mappedTester)
                .withLower(0).withUpper(name.endsWith("Multi") ? -1 : 1)
                .withMemberType(TRANSIENT)
                .withRelationKind(AGGREGATION)
                .build();
    }

    private DataMember createNameAttribute(MemberType memberType) {
        return newDataMemberBuilder()
                .withName("name")
                .withRequired(true)
                .withDataType(STRING_TYPE)
                .withMemberType(memberType)
                .build();
    }

    private DataMember createTransientAttribute() {
        return newDataMemberBuilder()
                .withDataType(STRING_TYPE)
                .withMemberType(TRANSIENT)
                .withName("transientAttribute")
                .build();
    }

}

