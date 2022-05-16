package hu.blackbelt.judo.runtime.core.dao.rdbms.rdbms;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.psm.namespace.Model;
import hu.blackbelt.judo.meta.psm.runtime.PsmModel;
import hu.blackbelt.judo.meta.psm.support.PsmModelResourceSupport;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.model.northwind.Demo;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Slf4j
public class RdbmsResolverTest {

    public static final String DEMO_ENTITY_CATEGORY = "demo.entities.Category";

    protected String getModelName() {
        return "northwind";
    }

    protected Model getPsmModel() {
        PsmModel psmModel = new Demo().fullDemo();
        return PsmModelResourceSupport.psmModelResourceSupportBuilder().resourceSet(psmModel.getResourceSet()).uri(psmModel.getUri()).build().getStreamOf(Model.class).findFirst().get();
    }

    @BeforeAll
    public void setup(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(getPsmModel(), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");
    }

    @AfterAll
    public void teardown(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.dropDatabase();
    }

    @Test
    public void testSqlTableName(JudoRuntimeFixture runtimeFixture) {
        Optional<EClass> categoryInfoClass = runtimeFixture.getAsmUtils().getClassByFQName(DEMO_ENTITY_CATEGORY);
        assertThat(runtimeFixture.getRdbmsResolver().rdbmsTable(categoryInfoClass.get()).getSqlName(), equalTo("T_ENTITIES_CATEGORY"));
    }

    @Test
    public void testSqlColumnName(JudoRuntimeFixture runtimeFixture) {
        EAttribute categoryName = runtimeFixture.getAsmUtils().all(EAttribute.class)
                .filter(a -> AsmUtils.getAttributeFQName(a).equals("demo.entities.Category#categoryName"))
                .findFirst()
                .get();

        assertThat(runtimeFixture.getRdbmsResolver().rdbmsField(categoryName).getSqlName(), equalTo("C_CATEGORYNAME"));
    }

    @Test
    public void testSqlReferenceNameForNormalTableReference(JudoRuntimeFixture runtimeFixture) {
        EReference product = runtimeFixture.getAsmUtils().all(EReference.class)
                .filter(a -> AsmUtils.getReferenceFQName(a).equals("demo.entities.OrderDetail#product"))
                .findFirst()
                .get();

        assertThat(runtimeFixture.getRdbmsResolver().rdbmsField(product).getSqlName(), equalTo("C_PRODUCT_ID"));
    }

    @Test
    public void testSqlReferenceNameForJoinTableBidirectionalReference(JudoRuntimeFixture runtimeFixture) {
        EReference manufacturedProducts = runtimeFixture.getAsmUtils().all(EReference.class)
                .filter(a -> AsmUtils.getReferenceFQName(a).equals("demo.entities.Company#manufacturedProducts"))
                .findFirst()
                .get();

        EReference manufacturers = runtimeFixture.getAsmUtils().all(EReference.class)
                .filter(a -> AsmUtils.getReferenceFQName(a).equals("demo.entities.Product#manufacturers"))
                .findFirst()
                .get();

        assertThat(runtimeFixture.getRdbmsResolver().rdbmsJunctionField(manufacturedProducts).getSqlName(), equalTo("C_MANUFCTURDPRDCTS_ID"));
        assertThat(runtimeFixture.getRdbmsResolver().rdbmsJunctionField(manufacturers).getSqlName(), equalTo("C_MANUFACTURERS_ID"));

        assertThat(runtimeFixture.getRdbmsResolver().rdbmsJunctionOppositeField(manufacturedProducts).getSqlName(), equalTo("C_MANUFACTURERS_ID"));
        assertThat(runtimeFixture.getRdbmsResolver().rdbmsJunctionOppositeField(manufacturers).getSqlName(), equalTo("C_MANUFCTURDPRDCTS_ID"));
    }

    @Test
    public void testSqlReferenceNameForJoinTableUnidirectionalReference(JudoRuntimeFixture runtimeFixture) {
        EReference employees = runtimeFixture.getAsmUtils().all(EReference.class)
                .filter(a -> AsmUtils.getReferenceFQName(a).equals("demo.entities.PaymentList#employees"))
                .findFirst()
                .get();

        assertThat(runtimeFixture.getRdbmsResolver().rdbmsJunctionField(employees).getSqlName(), equalTo("C_EMPLOYEES_ID1"));
        assertThat(runtimeFixture.getRdbmsResolver().rdbmsJunctionOppositeField(employees).getSqlName(), equalTo("EMPLOYEES_T_ENTITIES_PAYMENTLIST_ID2"));
    }

    @Test
    public void testSqlNameForJoinTableBidirectionalReference(JudoRuntimeFixture runtimeFixture) {
        EReference manufacturedProducts = runtimeFixture.getAsmUtils().all(EReference.class)
                .filter(a -> AsmUtils.getReferenceFQName(a).equals("demo.entities.Company#manufacturedProducts"))
                .findFirst()
                .get();

        EReference manufacturers = runtimeFixture.getAsmUtils().all(EReference.class)
                .filter(a -> AsmUtils.getReferenceFQName(a).equals("demo.entities.Product#manufacturers"))
                .findFirst()
                .get();

        assertThat(runtimeFixture.getRdbmsResolver().rdbmsJunctionTable(manufacturedProducts).getSqlName(), equalTo("T_ENTITIS_PRODUCT_MANUFCTURDPRDCTS_ENTITIS_COMPANY_MANUFCTURRS"));
        assertThat(runtimeFixture.getRdbmsResolver().rdbmsJunctionTable(manufacturers).getSqlName(), equalTo("T_ENTITIS_PRODUCT_MANUFCTURDPRDCTS_ENTITIS_COMPANY_MANUFCTURRS"));
    }

    @Test
    public void testSqlNameForJoinTableUnidirectionalReference(JudoRuntimeFixture runtimeFixture) {
        EReference employees = runtimeFixture.getAsmUtils().all(EReference.class)
                .filter(a -> AsmUtils.getReferenceFQName(a).equals("demo.entities.PaymentList#employees"))
                .findFirst()
                .get();

        assertThat(runtimeFixture.getRdbmsResolver().rdbmsJunctionTable(employees).getSqlName(), equalTo("T_ENTITIES_PAYMENTLIST_EMPLOYEES"));
    }
}