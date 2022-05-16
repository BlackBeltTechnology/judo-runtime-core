package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class InheritanceTest {

    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected String getModelName() {
        return MODEL_NAME;
    }

    @AfterEach
    public void teardown(final JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
    }

    @Test
    void testCar(final JudoRuntimeFixture runtimeFixture, final JudoDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();

        final EntityType entity = newEntityTypeBuilder()
                .withName("Entity")
                .withAbstract_(true)
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(entity).withMapping(newMappingBuilder().withTarget(entity)).build();

        final EntityType company = newEntityTypeBuilder()
                .withName("Company")
                .withGeneralizations(newGeneralizationBuilder().withTarget(entity).build())
                .build();
        useEntityType(company).withMapping(newMappingBuilder().withTarget(company)).build();

        final EntityType vehicle = newEntityTypeBuilder()
                .withName("Vehicle")
                .withAbstract_(true)
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("manufacturer")
                        .withLower(0).withUpper(1)
                        .withTarget(company)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(vehicle).withMapping(newMappingBuilder().withTarget(vehicle)).build();

        final EntityType car = newEntityTypeBuilder()
                .withName("Car")
                .withGeneralizations(newGeneralizationBuilder().withTarget(vehicle).build())
                .withAttributes(newDataMemberBuilder()
                        .withName("licensePlate")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(car).withMapping(newMappingBuilder().withTarget(car)).build();

        final EntityType person = newEntityTypeBuilder()
                .withName("Person")
                .withGeneralizations(newGeneralizationBuilder().withTarget(entity).build())
                .withAttributes(newDataMemberBuilder()
                        .withName("carInfo")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.car.licensePlate + ' ' + self.car.manufacturer.name")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("car")
                        .withLower(0).withUpper(1)
                        .withTarget(car)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(person).withMapping(newMappingBuilder().withTarget(person)).build();

        final Model model = newModelBuilder()
                .withName(getModelName())
                .withElements(stringType, entity, company, vehicle, car, person)
                .build();

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        final EClass companyType = (EClass) runtimeFixture.getAsmUtils().resolve(DTO_PACKAGE + ".Company").get();
        final EClass carType = (EClass) runtimeFixture.getAsmUtils().resolve(DTO_PACKAGE + ".Car").get();
        final EClass personType = (EClass) runtimeFixture.getAsmUtils().resolve(DTO_PACKAGE + ".Person").get();

        final Payload bmw = runtimeFixture.getDao().create(companyType, Payload.map(
                "name", "BMW"
        ), null);
        final Payload vw = runtimeFixture.getDao().create(companyType, Payload.map(
                "name", "VW"
        ), null);

        final Payload car1 = runtimeFixture.getDao().create(carType, Payload.map(
                "licensePlate", "ABC-123",
                "manufacturer", bmw
        ), null);

        final Payload person1 = runtimeFixture.getDao().create(personType, Payload.map(
                "name", "Gipsz Jakab",
                "car", car1
        ), null);

        log.debug("Person1: {}", person1);
        assertThat(person1.get("carInfo"), equalTo("ABC-123 BMW"));
    }
}
