package hu.blackbelt.judo.runtime.core.dao.rdbms.rdbms;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.psm.namespace.Model;
import hu.blackbelt.judo.meta.psm.runtime.PsmModel;
import hu.blackbelt.judo.meta.psm.support.PsmModelResourceSupport;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceSingetonExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceFixture;
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

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
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
    public void setup(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getPsmModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
    }

    @AfterAll
    public void teardown(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    public void testSqlTableName(RdbmsDaoFixture daoFixture) {
        Optional<EClass> categoryInfoClass = daoFixture.getAsmUtils().getClassByFQName(DEMO_ENTITY_CATEGORY);
        assertThat(daoFixture.getRdbmsResolver().rdbmsTable(categoryInfoClass.get()).getSqlName(), equalTo("T_ENTITIES_CATEGORY"));
    }

    @Test
    public void testSqlColumnName(RdbmsDaoFixture daoFixture) {
        EAttribute categoryName = daoFixture.getAsmUtils().all(EAttribute.class)
                .filter(a -> AsmUtils.getAttributeFQName(a).equals("demo.entities.Category#categoryName"))
                .findFirst()
                .get();

        assertThat(daoFixture.getRdbmsResolver().rdbmsField(categoryName).getSqlName(), equalTo("C_CATEGORYNAME"));
    }

    @Test
    public void testSqlReferenceNameForNormalTableReference(RdbmsDaoFixture daoFixture) {
        EReference product = daoFixture.getAsmUtils().all(EReference.class)
                .filter(a -> AsmUtils.getReferenceFQName(a).equals("demo.entities.OrderDetail#product"))
                .findFirst()
                .get();

        assertThat(daoFixture.getRdbmsResolver().rdbmsField(product).getSqlName(), equalTo("C_PRODUCT_ID"));
    }

    @Test
    public void testSqlReferenceNameForJoinTableBidirectionalReference(RdbmsDaoFixture daoFixture) {
        EReference manufacturedProducts = daoFixture.getAsmUtils().all(EReference.class)
                .filter(a -> AsmUtils.getReferenceFQName(a).equals("demo.entities.Company#manufacturedProducts"))
                .findFirst()
                .get();

        EReference manufacturers = daoFixture.getAsmUtils().all(EReference.class)
                .filter(a -> AsmUtils.getReferenceFQName(a).equals("demo.entities.Product#manufacturers"))
                .findFirst()
                .get();

        assertThat(daoFixture.getRdbmsResolver().rdbmsJunctionField(manufacturedProducts).getSqlName(), equalTo("C_MANUFCTURDPRDCTS_ID"));
        assertThat(daoFixture.getRdbmsResolver().rdbmsJunctionField(manufacturers).getSqlName(), equalTo("C_MANUFACTURERS_ID"));

        assertThat(daoFixture.getRdbmsResolver().rdbmsJunctionOppositeField(manufacturedProducts).getSqlName(), equalTo("C_MANUFACTURERS_ID"));
        assertThat(daoFixture.getRdbmsResolver().rdbmsJunctionOppositeField(manufacturers).getSqlName(), equalTo("C_MANUFCTURDPRDCTS_ID"));
    }

    @Test
    public void testSqlReferenceNameForJoinTableUnidirectionalReference(RdbmsDaoFixture daoFixture) {
        EReference employees = daoFixture.getAsmUtils().all(EReference.class)
                .filter(a -> AsmUtils.getReferenceFQName(a).equals("demo.entities.PaymentList#employees"))
                .findFirst()
                .get();

        assertThat(daoFixture.getRdbmsResolver().rdbmsJunctionField(employees).getSqlName(), equalTo("C_EMPLOYEES_ID1"));
        assertThat(daoFixture.getRdbmsResolver().rdbmsJunctionOppositeField(employees).getSqlName(), equalTo("EMPLOYEES_T_ENTITIES_PAYMENTLIST_ID2"));
    }

    @Test
    public void testSqlNameForJoinTableBidirectionalReference(RdbmsDaoFixture daoFixture) {
        EReference manufacturedProducts = daoFixture.getAsmUtils().all(EReference.class)
                .filter(a -> AsmUtils.getReferenceFQName(a).equals("demo.entities.Company#manufacturedProducts"))
                .findFirst()
                .get();

        EReference manufacturers = daoFixture.getAsmUtils().all(EReference.class)
                .filter(a -> AsmUtils.getReferenceFQName(a).equals("demo.entities.Product#manufacturers"))
                .findFirst()
                .get();

        assertThat(daoFixture.getRdbmsResolver().rdbmsJunctionTable(manufacturedProducts).getSqlName(), equalTo("T_ENTITIS_PRODUCT_MANUFCTURDPRDCTS_ENTITIS_COMPANY_MANUFCTURRS"));
        assertThat(daoFixture.getRdbmsResolver().rdbmsJunctionTable(manufacturers).getSqlName(), equalTo("T_ENTITIS_PRODUCT_MANUFCTURDPRDCTS_ENTITIS_COMPANY_MANUFCTURRS"));
    }

    @Test
    public void testSqlNameForJoinTableUnidirectionalReference(RdbmsDaoFixture daoFixture) {
        EReference employees = daoFixture.getAsmUtils().all(EReference.class)
                .filter(a -> AsmUtils.getReferenceFQName(a).equals("demo.entities.PaymentList#employees"))
                .findFirst()
                .get();

        assertThat(daoFixture.getRdbmsResolver().rdbmsJunctionTable(employees).getSqlName(), equalTo("T_ENTITIES_PAYMENTLIST_EMPLOYEES"));
    }
}