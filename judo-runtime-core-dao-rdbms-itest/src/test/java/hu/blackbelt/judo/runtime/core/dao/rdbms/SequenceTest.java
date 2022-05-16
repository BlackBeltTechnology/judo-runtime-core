package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.TransferObjectType;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
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
@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class SequenceTest {

    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    private static final String NEXT_VALUE = "next";

    protected String getModelName() {
        return MODEL_NAME;
    }

    @AfterEach
    public void teardown(final JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
    }

    @Test
    void testSequence(final JudoRuntimeFixture runtimeFixture, final JudoDatasourceFixture datasourceFixture) {
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

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        final EAttribute nextOfTesterAttribute = runtimeFixture.getAsmUtils().resolveAttribute(MODEL_NAME + ".Tester#" + NEXT_VALUE).get();

        final Payload payload1 = runtimeFixture.getDao().getStaticData(nextOfTesterAttribute);
        assertThat(payload1, equalTo(Payload.map(NEXT_VALUE, (int) 1)));

        final Payload payload2 = runtimeFixture.getDao().getStaticData(nextOfTesterAttribute);
        assertThat(payload2, equalTo(Payload.map(NEXT_VALUE, (int) 2)));
    }
}
