package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.structure.*;
import hu.blackbelt.judo.meta.esm.type.BooleanType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newBooleanTypeBuilder;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class SelfNavigationTest {

    public static final String MODEL_NAME = "S";

    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getEsmModel() {

        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();

        final Model model = NamespaceBuilders.newModelBuilder().withName(getModelName()).build();

        final DataMember nameOfEntityType = newDataMemberBuilder()
                .withName("name")
                .withDataType(stringType)
                .withRequired(true)
                .withMemberType(MemberType.STORED)
                .build();
        final EntityType entityType = newEntityTypeBuilder()
                .withName("E")
                .withAttributes(nameOfEntityType)
                .build();
        final OneWayRelationMember selfOfEntityType = newOneWayRelationMemberBuilder()
                .withName("self")
                .withTarget(entityType)
                .withLower(1).withUpper(1)
                .withRelationKind(RelationKind.ASSOCIATION)
                .withMemberType(MemberType.DERIVED)
                .withGetterExpression("self")
                .build();
        useEntityType(entityType)
                .withMapping(newMappingBuilder()
                        .withTarget(entityType)
                        .build())
                .withRelations(selfOfEntityType)
                .build();

        final TransferObjectType transferObjectType1 = newTransferObjectTypeBuilder()
                .withName("T1")
                .withMapping(newMappingBuilder()
                        .withTarget(entityType)
                        .build())
                .build();
        final TransferObjectType transferObjectType2 = newTransferObjectTypeBuilder()
                .withName("T2")
                .withMapping(newMappingBuilder()
                        .withTarget(entityType)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.MAPPED)
                        .withBinding(nameOfEntityType)
                        .build())
                .build();
        useTransferObjectType(transferObjectType1)
                .withGeneralizations(newGeneralizationBuilder()
                        .withTarget(transferObjectType2)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("self")
                        .withTarget(transferObjectType2)
                        .withLower(1).withUpper(1)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self")
                        .build())
                .build();

        model.getElements().addAll(Arrays.asList(
                stringType, booleanType, entityType, transferObjectType1, transferObjectType2
        ));
        return model;
    }

    RdbmsDaoFixture daoFixture;

    @BeforeEach
    public void setup(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
        this.daoFixture = daoFixture;
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    public void testSelfNavigation() {
        final EClass entityType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".E").get();
        final EReference selfOfE = entityType.getEAllReferences().stream().filter(r -> "self".equals(r.getName())).findAny().get();
        final EClass transferObjectType1 = daoFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".T1").get();

        final Payload entity1 = daoFixture.getDao().create(entityType, map("name", "Entity1"), null);
        log.debug("Saved entity #1: {}", entity1);
        final UUID entity1Id = entity1.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final Payload entity2 = daoFixture.getDao().create(entityType, map("name", "Entity2"), null);
        log.debug("Saved entity #2: {}", entity2);
        final UUID entity2Id = entity2.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final List<Payload> selfOfEntity1 = daoFixture.getDao().getNavigationResultAt(entity1Id, selfOfE);
        log.debug("Self of E #1: {}", selfOfEntity1);
        assertThat(selfOfEntity1, hasSize(1));
        assertThat(selfOfEntity1.get(0), equalTo(entity1));

        final List<Payload> selfOfEntity2 = daoFixture.getDao().getNavigationResultAt(entity2Id, selfOfE);
        log.debug("Self of E #2: {}", selfOfEntity2);
        assertThat(selfOfEntity2, hasSize(1));
        assertThat(selfOfEntity2.get(0), equalTo(entity2));

        final Optional<Payload> transferObject1 = daoFixture.getDao().getByIdentifier(transferObjectType1, entity1Id);
        log.debug("TransferObject1 of E #1: {}", transferObject1);
        assertThat(transferObject1.isPresent(), equalTo(Boolean.TRUE));
        final Payload expected1T1 = Payload.asPayload(entity1);
        final Payload expected1T2 = Payload.asPayload(entity1);
        expected1T1.put("self", expected1T2);
        assertThat(transferObject1.get(), equalTo(expected1T1));

        final Optional<Payload> transferObject2 = daoFixture.getDao().getByIdentifier(transferObjectType1, entity2Id);
        log.debug("TransferObject1 of E #2: {}", transferObject2);
        assertThat(transferObject2.isPresent(), equalTo(Boolean.TRUE));
        final Payload expected2T1 = Payload.asPayload(entity2);
        final Payload expected2T2 = Payload.asPayload(entity2);
        expected2T1.put("self", expected2T2);
        assertThat(transferObject2.get(), equalTo(expected2T1));
    }
}
