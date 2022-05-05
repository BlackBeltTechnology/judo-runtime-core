package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.psm.data.AssociationEnd;
import hu.blackbelt.judo.meta.psm.data.Attribute;
import hu.blackbelt.judo.meta.psm.data.EntityType;
import hu.blackbelt.judo.meta.psm.namespace.Model;
import hu.blackbelt.judo.meta.psm.service.MappedTransferObjectType;
import hu.blackbelt.judo.meta.psm.type.StringType;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.psm.data.util.builder.DataBuilders.*;
import static hu.blackbelt.judo.meta.psm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.psm.service.util.builder.ServiceBuilders.*;
import static hu.blackbelt.judo.meta.psm.type.util.builder.TypeBuilders.newCardinalityBuilder;
import static hu.blackbelt.judo.meta.psm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class GraphTest {

    public static final String MODEL_NAME = "tree";

    public static final String NODE_ENTITY = "Node";
    public static final String NODE_DTO = "NodeDTO";

    public static final String TARGETS_RELATION = "targets";
    public static final String SOURCE_RELATION = "source";

    public static final String NAME = "name";

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getPsmModel(boolean singleSource) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(10).build();

        final Attribute name = newAttributeBuilder()
                .withName(NAME)
                .withDataType(stringType)
                .withRequired(true)
                .build();

        final EntityType node = newEntityTypeBuilder()
                .withName(NODE_ENTITY)
                .withAttributes(Arrays.asList(name))
                .build();

        final AssociationEnd targets = newAssociationEndBuilder()
                .withName(TARGETS_RELATION)
                .withTarget(node)
                .withCardinality(newCardinalityBuilder().withLower(0).withUpper(-1).build())
                .build();

        useEntityType(node).withRelations(targets).build();

        final AssociationEnd source;
        if (singleSource) {
            source = newAssociationEndBuilder()
                    .withName(SOURCE_RELATION)
                    .withTarget(node)
                    .withCardinality(newCardinalityBuilder().withLower(0).withUpper(1).build())
                    .withPartner(targets)
                    .build();

            useAssociationEnd(targets).withPartner(source).build();

            useEntityType(node).withRelations(source).build();
        } else {
            source = null;
        }

        final MappedTransferObjectType nodeDTO = newMappedTransferObjectTypeBuilder()
                .withName(NODE_DTO)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withRequired(true)
                        .withBinding(name)
                        .build())
                .withEntityType(node)
                .build();

        useMappedTransferObjectType(nodeDTO)
                .withRelations(newTransferObjectRelationBuilder()
                        .withName(TARGETS_RELATION)
                        .withCardinality(newCardinalityBuilder().withLower(0).withUpper(-1).build())
                        .withTarget(nodeDTO)
                        .withBinding(targets)
                        .withEmbedded(true)
                        .build())
                .build();

        if (singleSource) {
            useMappedTransferObjectType(nodeDTO)
                    .withRelations(newTransferObjectRelationBuilder()
                            .withName(SOURCE_RELATION)
                            .withCardinality(newCardinalityBuilder().withLower(0).withUpper(1).build())
                            .withTarget(nodeDTO)
                            .withBinding(source)
                            .build())
                    .build();
        }

        Model model = newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(Arrays.asList(stringType, node, nodeDTO)).build();
        return model;
    }

    void testCount(RdbmsDaoFixture testFixture) {
        final Payload source1 = map(NAME, "source1");

        final Payload target1 = map(NAME, "target1");
        final Payload target2 = map(NAME, "target2");

        final EClass nodeDto = testFixture.getAsmUtils().getClassByFQName(MODEL_NAME + "." + NODE_DTO).get();

        final EReference targets = nodeDto.getEAllReferences().stream().filter(a -> TARGETS_RELATION.equals(a.getName())).findAny().get();

        log.debug("Saving source1...");
        final Payload savedSource1 = testFixture.getDao().create(nodeDto, source1, DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID source1Id = savedSource1.getAs(testFixture.getIdProvider().getType(), testFixture.getIdProvider().getName());
        log.debug("  - saved source1: {}", source1Id);
        log.debug("Saving target1...");
        final Payload savedTarget1 = testFixture.getDao().create(nodeDto, target1, DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID node1Id = savedTarget1.getAs(testFixture.getIdProvider().getType(), testFixture.getIdProvider().getName());
        log.debug("  - saved target1: {}", node1Id);
        log.debug("Saving target2...");
        final Payload savedTarget2 = testFixture.getDao().create(nodeDto, target2, DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID node2Id = savedTarget2.getAs(testFixture.getIdProvider().getType(), testFixture.getIdProvider().getName());
        log.debug("  - saved target2: {}", node2Id);

        testFixture.getDao().addReferences(targets, source1Id, Arrays.asList(node1Id, node2Id));

        final Optional<Payload> loadedSource1 = testFixture.getDao().getByIdentifier(nodeDto, source1Id);
        assertTrue(loadedSource1.isPresent());

        log.debug("Loaded source1: {}", loadedSource1);

        testFixture.getDao().delete(nodeDto, node1Id);
        testFixture.getDao().delete(nodeDto, node2Id);
        testFixture.getDao().delete(nodeDto, source1Id);
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    void testGraphWithJoin(RdbmsDaoFixture testFixture, RdbmsDatasourceFixture datasourceFixture) {
        testFixture.init(getPsmModel(false), datasourceFixture);
        assertTrue(testFixture.isInitialized(), "DAO initialized");
        testCount(testFixture);
    }

    @Test
    void testGraphWithoutJoin(RdbmsDaoFixture testFixture, RdbmsDatasourceFixture datasourceFixture) {
        testFixture.init(getPsmModel(true), datasourceFixture);
        assertTrue(testFixture.isInitialized(), "DAO initialized");
        testCount(testFixture);
    }
}
