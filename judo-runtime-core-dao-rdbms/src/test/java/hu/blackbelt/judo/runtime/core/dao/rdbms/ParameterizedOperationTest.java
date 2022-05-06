package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.NamespaceElement;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.ModelBuilder;
import hu.blackbelt.judo.meta.esm.operation.OperationType;
import hu.blackbelt.judo.meta.esm.operation.util.builder.OperationBuilder;
import hu.blackbelt.judo.meta.esm.operation.util.builder.ParameterBuilder;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.TransferObjectType;
import hu.blackbelt.judo.meta.esm.structure.util.builder.DataMemberBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.EntityTypeBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.MappingBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.TransferObjectTypeBuilder;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.util.builder.NumericTypeBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static hu.blackbelt.judo.meta.esm.structure.MemberType.STORED;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class ParameterizedOperationTest {
    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    private Class<UUID> idProviderClass;
    private String idProviderName;

    @BeforeEach
    public void setup(RdbmsDaoFixture daoFixture) {
        idProviderClass = daoFixture.getIdProvider().getType();
        idProviderName = daoFixture.getIdProvider().getName();
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    @Disabled // TODO: JNG-1889
    public void testParameterizedOperation(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final String MODEL_NAME = "M";

        final NumericType integer = NumericTypeBuilder.create().withName("integerType").withPrecision(7).withScale(0).build();

        final List<NamespaceElement> namespaceElements = new ArrayList<>(Collections.singletonList(integer));

        final EntityType entityA = EntityTypeBuilder.create()
                .withName("A")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("non_required")
                                .withMemberType(STORED)
                                .withDataType(integer)
                                .withRequired(false)
                                .build(),
                        DataMemberBuilder.create()
                                .withName("required")
                                .withMemberType(STORED)
                                .withDataType(integer)
                                .withRequired(true)
                                .build())
                .build();
        entityA.setMapping(MappingBuilder.create().withTarget(entityA).build());
        namespaceElements.add(entityA);

        final TransferObjectType operationManager = TransferObjectTypeBuilder.create()
                .withName("OperationManager")
                .withOperations(
                        OperationBuilder.create()
                                .withName("increase_non_required_parameter")
                                .withStateful(true)
                                .withBinding("")
                                .withOperationType(OperationType.STATIC)
                                .withCustomImplementation(false)
                                .withInput(ParameterBuilder.create().withName("input").withTarget(entityA).withLower(1).withUpper(1))
                                .withOutput(ParameterBuilder.create().withName("output").withTarget(entityA).withLower(1).withUpper(1))
                                .withBody("" +
                                                  "var M::A i = mutable input\n" +
                                                  "i.non_required = i.non_required + 1\n" +
                                                  "return i\n")
                                .build())
                .build();
        namespaceElements.add(operationManager);

        final Model model = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();

        daoFixture.init(model, datasourceFixture);
        Assertions.assertTrue(daoFixture.isInitialized());

        final EClass aEClass = daoFixture.getAsmUtils().getClassByFQName("M._default_transferobjecttypes.A").get();

        final Payload aInstance =
                daoFixture.getDao().create(aEClass, Payload.map("required", 999), DAO.QueryCustomizer.<UUID>builder().build());
        log.debug("Instance of A (entity) created:\n" + aInstance);
        final UUID aID = aInstance.getAs(idProviderClass, idProviderName);

        final Payload operationCalledWithInstance = daoFixture.getOperationImplementations()
                .get("increase_non_required_parameter")
                .apply(Payload.map("input", Payload.map(idProviderName, aID, "required", 999)));
        log.debug("OPERATION RESULT:\n" + operationCalledWithInstance.toString());

        Payload output = operationCalledWithInstance.getAsPayload("output");
        assertNotNull(output);
        assertThat(output.getAs(idProviderClass, idProviderName), equalTo(aID));
        assertThat(output.getAs(Integer.class, "required"), equalTo(999));
        assertThat(output.getAs(Integer.class, "non_required"), nullValue());

        final Payload operationCalledWithNonExistent = daoFixture.getOperationImplementations()
                .get("increase_non_required_parameter")
                .apply(Payload.map("input", Payload.map("required", 348623)));
        log.debug("OPERATION RESULT:\n" + operationCalledWithNonExistent.toString());

        Payload output2 = operationCalledWithNonExistent.getAsPayload("output");
        assertNotNull(output2);
        assertThat(output2.getAs(Integer.class, "required"), equalTo(348623));
        assertThat(output2.getAs(Integer.class, "non_required"), nullValue());

    }

}
