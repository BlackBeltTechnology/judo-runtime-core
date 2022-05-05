package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.TransferObjectType;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newDataMemberBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newTransferObjectTypeBuilder;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newNumericTypeBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class SequenceTest {

    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    private static final String NEXT_VALUE = "next";

    protected String getModelName() {
        return MODEL_NAME;
    }

    @AfterEach
    public void teardown(final RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    void testSequence(final RdbmsDaoFixture daoFixture, final RdbmsDatasourceFixture datasourceFixture) {
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();
        final TransferObjectType tester = newTransferObjectTypeBuilder()
                .withName("Tester")
                .withAttributes(newDataMemberBuilder()
                        .withName(NEXT_VALUE)
                        .withDataType(integerType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("M::Integer!getVariable('SEQUENCE', 'Xárvíztűrő \\'tükörfúrógép\"')") // - HsqlDb sequence name must not start with underscore
                        .build())
                .build();

        final Model model = newModelBuilder()
                .withName(getModelName())
                .withElements(integerType, tester)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EAttribute nextOfTesterAttribute = daoFixture.getAsmUtils().resolveAttribute(MODEL_NAME + ".Tester#" + NEXT_VALUE).get();

        final Payload payload1 = daoFixture.getDao().getStaticData(nextOfTesterAttribute);
        assertThat(payload1, equalTo(Payload.map(NEXT_VALUE, (int) RdbmsDaoFixture.SEQUENCE_START)));

        final Payload payload2 = daoFixture.getDao().getStaticData(nextOfTesterAttribute);
        assertThat(payload2, equalTo(Payload.map(NEXT_VALUE, (int) (RdbmsDaoFixture.SEQUENCE_START + RdbmsDaoFixture.SEQUENCE_INCREMENT))));
    }
}
