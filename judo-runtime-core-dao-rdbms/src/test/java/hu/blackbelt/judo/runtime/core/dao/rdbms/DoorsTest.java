package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.meta.esm.structure.TwoWayRelationMember;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.*;

import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newPackageBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newNumericTypeBuilder;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class DoorsTest {

    public static final String MODEL_NAME = "doors";

    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    @BeforeAll
    public static void setup() {
        System.setProperty("email", "test@employee");
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    public Model getDefaultModel() {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(16).withScale(0).build();

        final EntityType employee = newEntityTypeBuilder().withName("Employee").build();
        final EntityType position = newEntityTypeBuilder().withName("Position").build();
        final EntityType division = newEntityTypeBuilder().withName("Division").build();

        final TwoWayRelationMember positionsOfEmployee = newTwoWayRelationMemberBuilder()
                .withName("positions")
                .withLower(0).withUpper(-1)
                .withTarget(position)
                .withMemberType(MemberType.STORED)
                .withRelationKind(RelationKind.ASSOCIATION)
                .build();
        final TwoWayRelationMember employeesOfPosition = newTwoWayRelationMemberBuilder()
                .withName("employees")
                .withLower(0).withUpper(-1)
                .withTarget(employee)
                .withPartner(positionsOfEmployee)
                .withMemberType(MemberType.STORED)
                .withRelationKind(RelationKind.ASSOCIATION)
                .build();
        useTwoWayRelationMember(positionsOfEmployee)
                .withPartner(employeesOfPosition)
                .build();

        final TwoWayRelationMember positionsOfDivision = newTwoWayRelationMemberBuilder()
                .withName("positions")
                .withLower(0).withUpper(-1)
                .withTarget(position)
                .withMemberType(MemberType.STORED)
                .withRelationKind(RelationKind.ASSOCIATION)
                .build();
        final TwoWayRelationMember divisionOfPosition = newTwoWayRelationMemberBuilder()
                .withName("division")
                .withLower(0).withUpper(1)
                .withTarget(division)
                .withPartner(positionsOfDivision)
                .withMemberType(MemberType.STORED)
                .withRelationKind(RelationKind.ASSOCIATION)
                .build();
        useTwoWayRelationMember(positionsOfDivision)
                .withPartner(divisionOfPosition)
                .build();

        useEntityType(employee)
                .withMapping(newMappingBuilder().withTarget(employee).build())
                .withAttributes(newDataMemberBuilder()
                        .withName("email")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(positionsOfEmployee)
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("rangeOfDivisions")
                        .withLower(0).withUpper(-1)
                        .withTarget(division)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression("doors::entities::Employee!filter(e | e.email == doors::types::String!getVariable('ENVIRONMENT', 'email')).positions.division")
                        .build())
                .build();
        useEntityType(position)
                .withMapping(newMappingBuilder().withTarget(position).build())
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(employeesOfPosition, divisionOfPosition)
                .build();
        useEntityType(division)
                .withMapping(newMappingBuilder().withTarget(division).build())
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(positionsOfDivision)
                .build();

        return newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(newPackageBuilder()
                        .withName("types")
                        .withElements(stringType, integerType)
                        .build())
                .withElements(newPackageBuilder()
                        .withName("entities")
                        .withElements(employee, position, division)
                        .build())
                .build();
    }

    @Test
    public void testDefaultValues(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getDefaultModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass division = (EClass) daoFixture.getAsmUtils().resolve(DTO_PACKAGE + ".entities.Division").get();
        final EClass position = (EClass) daoFixture.getAsmUtils().resolve(DTO_PACKAGE + ".entities.Position").get();
        final EClass employee = (EClass) daoFixture.getAsmUtils().resolve(DTO_PACKAGE + ".entities.Employee").get();
        final EReference rangeOfDivisionsOfEmployee = daoFixture.getAsmUtils().resolveReference(DTO_PACKAGE + ".entities.Employee#rangeOfDivisions").get();
        final EAttribute nameOfDivision = daoFixture.getAsmUtils().resolveAttribute(DTO_PACKAGE + ".entities.Division#name").get();

        final Payload d1 = daoFixture.getDao().create(division, Payload.map(
                "name", "D1"
        ), null);
        final Payload d2 = daoFixture.getDao().create(division, Payload.map(
                "name", "D2"
        ), null);
        final Payload d3 = daoFixture.getDao().create(division, Payload.map(
                "name", "D3"
        ), null);

        final Payload p1 = daoFixture.getDao().create(position, Payload.map(
                "name", "P1",
                "division", d1
        ), null);
        final Payload p2 = daoFixture.getDao().create(position, Payload.map(
                "name", "P2",
                "division", d2
        ), null);
        daoFixture.getDao().create(position, Payload.map(
                "name", "P3",
                "division", d3
        ), null);

        final Payload testEmployee = daoFixture.getDao().create(employee, Payload.map(
                "email", "test@employee",
                "positions", Arrays.asList(p1, p2)
        ), null);
        final UUID testEmployeeId = testEmployee.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final Set<Payload> set1 = new HashSet<>(daoFixture.getDao().getNavigationResultAt(testEmployeeId, rangeOfDivisionsOfEmployee));
        final Set<Payload> set2 = new HashSet<>(daoFixture.getDao().searchNavigationResultAt(testEmployeeId, rangeOfDivisionsOfEmployee, DAO.QueryCustomizer.<UUID>builder()
                .seek(DAO.Seek.builder()
                        .limit(5)
                        .build())
                .orderBy(DAO.OrderBy.builder()
                        .attribute(nameOfDivision)
                        .descending(false)
                        .build())
                .build()));

        assertThat(set1, equalTo(set2));
    }
}
