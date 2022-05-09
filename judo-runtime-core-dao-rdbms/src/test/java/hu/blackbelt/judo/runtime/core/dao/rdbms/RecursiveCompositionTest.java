package hu.blackbelt.judo.runtime.core.dao.rdbms;

import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.psm.data.Attribute;
import hu.blackbelt.judo.meta.psm.data.Containment;
import hu.blackbelt.judo.meta.psm.data.EntityType;
import hu.blackbelt.judo.meta.psm.namespace.Model;
import hu.blackbelt.judo.meta.psm.service.MappedTransferObjectType;
import hu.blackbelt.judo.meta.psm.service.TransferAttribute;
import hu.blackbelt.judo.meta.psm.service.TransferObjectRelation;
import hu.blackbelt.judo.meta.psm.type.StringType;
import hu.blackbelt.judo.meta.psm.type.util.builder.TypeBuilders;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceGraph;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.of;
import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.psm.data.util.builder.DataBuilders.*;
import static hu.blackbelt.judo.meta.psm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.psm.service.util.builder.ServiceBuilders.*;
import static hu.blackbelt.judo.meta.psm.type.util.builder.TypeBuilders.newCardinalityBuilder;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class RecursiveCompositionTest {

    public static final String MODEL_NAME = "recursivecomposition";
    public static final String ENTITY_X = "recursivecomposition.X";
    public static final String ENTITY_Y = "recursivecomposition.Y";

    public static final String P = "recursivecomposition.P";
    public static final String Q = "recursivecomposition.Q";

    public static final String X = "x";
    public static final String XS = "xs";
    public static final String Y = "y";
    public static final String YS = "ys";

    public static final String T_X = "p";
    public static final String T_XS = "ps";
    public static final String T_Y = "q";
    public static final String T_YS = "qs";

    public static final String X1 = "x1";
    public static final String X2 = "x2";
    public static final String X3 = "x3";
    public static final String X4 = "x4";
    public static final String X5 = "x5";
    public static final String X6 = "x6";
    public static final String X7 = "x7";
    public static final String X8 = "x8";
    public static final String X9 = "x9";
    public static final String X10 = "x10";
    public static final String X11 = "x11";
    public static final String X12 = "x12";
    public static final String X13 = "x13";
    public static final String Y1 = "y1";
    public static final String Y2 = "y2";
    public static final String Y3 = "y3";
    public static final String Y4 = "y4";
    public static final String Y5 = "y5";
    public static final String Y6 = "y6";
    public static final String Y7 = "y7";
    public static final String Y8 = "y8";
    public static final String Y9 = "y9";
    public static final String NAME = "name";
    public static final String ID = "id";

    NamedParameterJdbcTemplate jdbcTemplate;
    Map<String, UUID> ids = new ConcurrentHashMap<>();

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getPsmModel() {
        StringType stringType = TypeBuilders.newStringTypeBuilder().withName("String").withMaxLength(10).build();

        Attribute x_name = newAttributeBuilder().withName(NAME).withDataType(stringType).build();
        EntityType x = newEntityTypeBuilder()
                .withName("X")
                .withAttributes(x_name)
                .build();

        Attribute y_name = newAttributeBuilder().withName(NAME).withDataType(stringType).build();
        EntityType y = newEntityTypeBuilder()
                .withName("Y")
                .withAttributes(y_name)
                .build();

        Containment x_x = newContainmentBuilder()
                .withName("x")
                .withCardinality(newCardinalityBuilder().build())
                .withTarget(x)
                .build();

        Containment x_xs = newContainmentBuilder()
                .withName("xs")
                .withCardinality(newCardinalityBuilder().withUpper(-1).build())
                .withTarget(x)
                .build();

        Containment x_y = newContainmentBuilder()
                .withName("y")
                .withCardinality(newCardinalityBuilder().build())
                .withTarget(y)
                .build();

        Containment x_ys = newContainmentBuilder()
                .withName("ys")
                .withCardinality(newCardinalityBuilder().withUpper(-1).build())
                .withTarget(y)
                .build();

        Containment y_x = newContainmentBuilder()
                .withName("x")
                .withCardinality(newCardinalityBuilder().build())
                .withTarget(x)
                .build();

        Containment y_xs = newContainmentBuilder()
                .withName("xs")
                .withCardinality(newCardinalityBuilder().withUpper(-1).build())
                .withTarget(x)
                .build();

        useEntityType(x).withRelations(of(x_x, x_xs, x_y, x_ys)).build();
        useEntityType(y).withRelations(of(y_x, y_xs)).build();

        MappedTransferObjectType p = newMappedTransferObjectTypeBuilder()
                .withName("P")
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(x_name))
                .withEntityType(x)
                .build();

        MappedTransferObjectType q = newMappedTransferObjectTypeBuilder()
                .withName("Q")
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(y_name))
                .withEntityType(y)
                .build();

        TransferObjectRelation p_p = newTransferObjectRelationBuilder()
                .withName("p")
                .withCardinality(newCardinalityBuilder().build())
                .withBinding(x_x)
                .withTarget(p)
                .withEmbedded(true)
                .build();

        TransferObjectRelation p_ps = newTransferObjectRelationBuilder()
                .withName("ps")
                .withCardinality(newCardinalityBuilder().withUpper(-1).build())
                .withBinding(x_xs)
                .withTarget(p)
                .withEmbedded(true)
                .build();

        TransferObjectRelation p_q = newTransferObjectRelationBuilder()
                .withName("q")
                .withCardinality(newCardinalityBuilder().build())
                .withBinding(x_y)
                .withTarget(q)
                .withEmbedded(true)
                .build();

        TransferObjectRelation p_qs = newTransferObjectRelationBuilder()
                .withName("qs")
                .withCardinality(newCardinalityBuilder().withUpper(-1).build())
                .withBinding(x_ys)
                .withTarget(q)
                .withEmbedded(true)
                .build();


        TransferObjectRelation q_p = newTransferObjectRelationBuilder()
                .withName("p")
                .withCardinality(newCardinalityBuilder().build())
                .withBinding(y_x)
                .withTarget(p)
                .withEmbedded(true)
                .build();

        TransferObjectRelation q_ps = newTransferObjectRelationBuilder()
                .withName("ps")
                .withCardinality(newCardinalityBuilder().withUpper(-1).build())
                .withBinding(y_xs)
                .withTarget(p)
                .withEmbedded(true)
                .build();

        TransferAttribute p_name = newTransferAttributeBuilder()
                .withName("x_name")
                .withDataType(stringType)
                .withBinding(x_name)
                .build();

        TransferAttribute q_name = newTransferAttributeBuilder()
                .withName("y_name")
                .withDataType(stringType)
                .withBinding(y_name)
                .build();

        useMappedTransferObjectType(p).withAttributes(p_name).withRelations(of(p_p, p_ps, p_q, p_qs)).build();
        useMappedTransferObjectType(q).withAttributes(q_name).withRelations(of(q_p, q_ps)).build();

        Model model = newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(of(stringType, x, y, p, q))
                .build();

        return model;
    }

    private Payload getRecursiveContainmentSamplePayload() {

        Payload x13 = map(NAME, X13);
        Payload x11 = map(NAME, X11);
        Payload x8 = map(NAME, X8);
        Payload x5 = map(NAME, X5);
        Payload x3 = map(NAME, X3);

        Payload y9 = map(NAME, Y9);
        Payload y7 = map(NAME, Y7);
        Payload y4 = map(NAME, Y4);
        Payload y3 = map(NAME, Y3);
        Payload y2 = map(NAME, Y2);
        Payload y1 = map(NAME, Y1);

        Payload y8 = map(NAME, Y8,
                T_X, x13);
        Payload x12 = map(NAME, X12,
                T_Y, y9);
        Payload x4 = map(NAME, X4,
                T_Y, y4);

        Payload x10 = map(NAME, X10,
                T_YS, of(y7, y8));
        Payload x2 = map(NAME, X2,
                T_XS, of(x3, x4),
                T_X, x5);

        Payload y6 = map(NAME, Y6,
                T_XS, of(x11, x12));
        Payload y5 = map(NAME, Y5,
                T_X, x10);

        Payload x9 = map(NAME, X9,
                T_YS, of(y5, y6));
        Payload x7 = map(NAME, X7,
                T_XS, of(x8, x9));
        Payload x6 = map(NAME, X6,
                T_X, x7);

        Payload x1 = map(NAME, X1,
                T_X, x2,
                T_XS, of(x6),
                T_Y, y1,
                T_YS, of(y2, y3));

        return x1;
    }

    private void testInsertGraph(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final EClass x = daoFixture.getAsmUtils().getClassByFQName(ENTITY_X).get();
        final EClass y = daoFixture.getAsmUtils().getClassByFQName(ENTITY_Y).get();
        final EClass p = daoFixture.getAsmUtils().getClassByFQName(P).get();

        final EAttribute nameOfX = x.getEAllAttributes().stream().filter(r -> RecursiveCompositionTest.NAME.equals(r.getName())).findAny().get();
        final EAttribute nameOfY = y.getEAllAttributes().stream().filter(r -> RecursiveCompositionTest.NAME.equals(r.getName())).findAny().get();

        daoFixture.getDao().create(p, getRecursiveContainmentSamplePayload(), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        jdbcTemplate = new NamedParameterJdbcTemplate(datasourceFixture.getWrappedDataSource());

        String sql = "SELECT ID, " + daoFixture.getRdbmsResolver().rdbmsField(nameOfX).getSqlName() + " FROM " + daoFixture.getRdbmsResolver().rdbmsTable(x).getSqlName();
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, ImmutableMap.of());
        for (Map row : rows) {
            log.debug("Values: " + row.values());
            UUID id = RdbmsDaoFixture.DATA_TYPE_MANAGER.getCoercer().coerce(row.get(ID), UUID.class);
            ids.put((String) row.get(daoFixture.getRdbmsResolver().rdbmsField(nameOfX).getSqlName()), id);
        }

        sql = "SELECT ID, " + daoFixture.getRdbmsResolver().rdbmsField(nameOfY).getSqlName() + " FROM " + daoFixture.getRdbmsResolver().rdbmsTable(y).getSqlName();
        rows = jdbcTemplate.queryForList(sql, ImmutableMap.of());
        for (Map row : rows) {
            UUID id = RdbmsDaoFixture.DATA_TYPE_MANAGER.getCoercer().coerce(row.get(ID), UUID.class);
            ids.put((String) row.get(daoFixture.getRdbmsResolver().rdbmsField(nameOfY).getSqlName()), id);
        }
    }

    @BeforeEach
    public void setup(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getPsmModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    public void testRecursiveInstanceCollector(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        testInsertGraph(daoFixture, datasourceFixture);

        final EClass x = daoFixture.getAsmUtils().getClassByFQName(ENTITY_X).get();
        final EClass y = daoFixture.getAsmUtils().getClassByFQName(ENTITY_Y).get();

        final EReference xOfX = x.getEAllReferences().stream().filter(r -> X.equals(r.getName())).findAny().get();
        final EReference xsOfX = x.getEAllReferences().stream().filter(r -> XS.equals(r.getName())).findAny().get();
        final EReference yOfX = x.getEAllReferences().stream().filter(r -> Y.equals(r.getName())).findAny().get();
        final EReference ysOfX = x.getEAllReferences().stream().filter(r -> YS.equals(r.getName())).findAny().get();
        final EReference xOfY = y.getEAllReferences().stream().filter(r -> X.equals(r.getName())).findAny().get();
        final EReference xsOfY = y.getEAllReferences().stream().filter(r -> XS.equals(r.getName())).findAny().get();

        final long startTs = System.currentTimeMillis();
        final RdbmsInstanceCollector instanceCollector = RdbmsInstanceCollector.<UUID>builder()
                .asmUtils(daoFixture.getAsmUtils())
                .jdbcTemplate(jdbcTemplate)
                .rdbmsParameterMapper(daoFixture.getRdbmsParameterMapper())
                .rdbmsResolver(daoFixture.getRdbmsResolver())
                .coercer(RdbmsDaoFixture.DATA_TYPE_MANAGER.getCoercer())
                .identifierProvider(daoFixture.getIdProvider())
                .rdbmsParameterMapper(daoFixture.getRdbmsParameterMapper())
                .rdbmsModel(daoFixture.getRdbmsModel())
                .dialect(Dialect.parse(datasourceFixture.getDialect(), datasourceFixture.isJooqEnabled()))
                .build();

        instanceCollector.createSelects();
        final long modelCreated = System.currentTimeMillis();
        log.debug("Instance collector created in {} ms:", (modelCreated - startTs));
        final Map<UUID, InstanceGraph<UUID>> graphs = instanceCollector.collectGraph(x, Collections.singleton(ids.get(X1)));
        final long endTs = System.currentTimeMillis();
        log.debug("Graphs returned in {} ms:\n{}", (endTs - modelCreated), graphs);

        final Optional<InstanceGraph<UUID>> resultX1 = Optional.ofNullable(graphs.get(ids.get(X1)));
        assertTrue(resultX1.isPresent());
        assertEquals(resultX1.get().getId(), ids.get(X1));

        assertEquals(resultX1.get().getContainments().size(), 5);
        assertEquals(resultX1.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), xOfX)).count(), 1);
        assertEquals(resultX1.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), yOfX)).count(), 1);
        assertEquals(resultX1.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), xsOfX)).count(), 1);
        assertEquals(resultX1.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), ysOfX)).count(), 2);

        final Optional<InstanceGraph<UUID>> resultX2 = resultX1.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), xOfX))
                .filter(c -> ids.get(X2).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        final Optional<InstanceGraph<UUID>> resultX6 = resultX1.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), xsOfX))
                .filter(c -> ids.get(X6).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        final Optional<InstanceGraph<UUID>> resultY1 = resultX1.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), yOfX))
                .filter(c -> ids.get(Y1).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        final Optional<InstanceGraph<UUID>> resultY2 = resultX1.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), ysOfX))
                .filter(c -> ids.get(Y2).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        final Optional<InstanceGraph<UUID>> resultY3 = resultX1.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), ysOfX))
                .filter(c -> ids.get(Y3).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        assertTrue(resultX2.isPresent());
        assertTrue(resultX6.isPresent());
        assertTrue(resultY1.isPresent());
        assertTrue(resultY2.isPresent());
        assertTrue(resultY3.isPresent());

        assertEquals(resultX2.get().getContainments().size(), 3);
        assertEquals(resultX6.get().getContainments().size(), 1);
        assertEquals(resultY1.get().getContainments().size(), 0);
        assertEquals(resultY2.get().getContainments().size(), 0);
        assertEquals(resultY3.get().getContainments().size(), 0);

        final Optional<InstanceGraph<UUID>> resultX3 = resultX2.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), xsOfX))
                .filter(c -> ids.get(X3).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        final Optional<InstanceGraph<UUID>> resultX4 = resultX2.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), xsOfX))
                .filter(c -> ids.get(X4).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        final Optional<InstanceGraph<UUID>> resultX5 = resultX2.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), xOfX))
                .filter(c -> ids.get(X5).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        assertTrue(resultX3.isPresent());
        assertTrue(resultX4.isPresent());
        assertTrue(resultX5.isPresent());

        assertEquals(resultX3.get().getContainments().size(), 0);
        assertEquals(resultX4.get().getContainments().size(), 1);
        assertEquals(resultX5.get().getContainments().size(), 0);

        final Optional<InstanceGraph<UUID>> resultY4 = resultX4.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), yOfX))
                .filter(c -> ids.get(Y4).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        assertTrue(resultY4.isPresent());

        assertEquals(resultY4.get().getContainments().size(), 0);

        final Optional<InstanceGraph<UUID>> resultX7 = resultX6.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), xOfX))
                .filter(c -> ids.get(X7).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        assertTrue(resultX7.isPresent());

        assertEquals(resultX7.get().getContainments().size(), 2);

        final Optional<InstanceGraph<UUID>> resultX8 = resultX7.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), xsOfX))
                .filter(c -> ids.get(X8).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        final Optional<InstanceGraph<UUID>> resultX9 = resultX7.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), xsOfX))
                .filter(c -> ids.get(X9).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        assertTrue(resultX8.isPresent());
        assertTrue(resultX9.isPresent());

        assertEquals(resultX8.get().getContainments().size(), 0);
        assertEquals(resultX9.get().getContainments().size(), 2);

        final Optional<InstanceGraph<UUID>> resultY5 = resultX9.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), ysOfX))
                .filter(c -> ids.get(Y5).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        final Optional<InstanceGraph<UUID>> resultY6 = resultX9.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), ysOfX))
                .filter(c -> ids.get(Y6).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        assertTrue(resultY5.isPresent());
        assertTrue(resultY6.isPresent());

        assertEquals(resultY5.get().getContainments().size(), 1);
        assertEquals(resultY6.get().getContainments().size(), 2);

        final Optional<InstanceGraph<UUID>> resultX10 = resultY5.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), xOfY))
                .filter(c -> ids.get(X10).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        assertTrue(resultX10.isPresent());

        assertEquals(resultX10.get().getContainments().size(), 2);

        final Optional<InstanceGraph<UUID>> resultY7 = resultX10.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), ysOfX))
                .filter(c -> ids.get(Y7).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        final Optional<InstanceGraph<UUID>> resultY8 = resultX10.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), ysOfX))
                .filter(c -> ids.get(Y8).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        assertTrue(resultY7.isPresent());
        assertTrue(resultY8.isPresent());

        assertEquals(resultY7.get().getContainments().size(), 0);
        assertEquals(resultY8.get().getContainments().size(), 1);

        final Optional<InstanceGraph<UUID>> resultX13 = resultY8.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), xOfY))
                .filter(c -> ids.get(X13).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        assertTrue(resultX13.isPresent());

        assertEquals(resultX13.get().getContainments().size(), 0);

        final Optional<InstanceGraph<UUID>> resultX11 = resultY6.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), xsOfY))
                .filter(c -> ids.get(X11).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        final Optional<InstanceGraph<UUID>> resultX12 = resultY6.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), xsOfY))
                .filter(c -> ids.get(X12).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        assertTrue(resultX11.isPresent());
        assertTrue(resultX12.isPresent());

        assertEquals(resultX11.get().getContainments().size(), 0);
        assertEquals(resultX12.get().getContainments().size(), 1);

        final Optional<InstanceGraph<UUID>> resultY9 = resultX12.get().getContainments().stream().filter(c -> AsmUtils.equals(c.getReference(), yOfX))
                .filter(c -> ids.get(Y9).equals(c.getReferencedElement().getId())).map(c -> c.getReferencedElement())
                .findAny();
        assertTrue(resultY9.isPresent());

        assertEquals(resultY9.get().getContainments().size(), 0);
    }

    @Test
    public void testRecursiveDelete(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        testInsertGraph(daoFixture, datasourceFixture);

        final EClass x = daoFixture.getAsmUtils().getClassByFQName(ENTITY_X).get();
        final EClass y = daoFixture.getAsmUtils().getClassByFQName(ENTITY_Y).get();

        final EClass p = daoFixture.getAsmUtils().getClassByFQName(P).get();
        daoFixture.getDao().delete(p, ids.get(X1));

        jdbcTemplate = new NamedParameterJdbcTemplate(datasourceFixture.getOriginalDataSource());
        int count = jdbcTemplate.queryForObject("SELECT count(1) FROM " + daoFixture.getRdbmsResolver().rdbmsTable(x).getSqlName(), new MapSqlParameterSource(), Integer.class);
        assertEquals(0, count);
        count = jdbcTemplate.queryForObject("SELECT count(1) FROM " + daoFixture.getRdbmsResolver().rdbmsTable(y).getSqlName(), new MapSqlParameterSource(), Integer.class);
        assertEquals(0, count);
    }

    @Test
    public void testRecursiveQuery(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        testInsertGraph(daoFixture, datasourceFixture);

        final EClass p = daoFixture.getAsmUtils().getClassByFQName(P).get();
        final EClass q = daoFixture.getAsmUtils().getClassByFQName(Q).get();

        final long queryPrepared = System.currentTimeMillis();
        final Map<String, Object> resultX1 = daoFixture.getDao().getByIdentifier(p, ids.get(X1)).get();
        final long queryCompleted = System.currentTimeMillis();

        log.debug("Query returned in {} ms:\n{}", (queryCompleted - queryPrepared), resultX1);

        final EReference pOfP = p.getEAllReferences().stream().filter(r -> "p".equals(r.getName())).findAny().get();
        final EReference psOfP = p.getEAllReferences().stream().filter(r -> "ps".equals(r.getName())).findAny().get();
        final EReference qOfP = p.getEAllReferences().stream().filter(r -> "q".equals(r.getName())).findAny().get();
        final EReference qsOfP = p.getEAllReferences().stream().filter(r -> "qs".equals(r.getName())).findAny().get();
        final EReference pOfQ = q.getEAllReferences().stream().filter(r -> "p".equals(r.getName())).findAny().get();
        final EReference psOfQ = q.getEAllReferences().stream().filter(r -> "ps".equals(r.getName())).findAny().get();

        assertEquals(ids.get(X1), resultX1.get(daoFixture.getUuid().getName()));

        final Map<String, Object> resultX2 = (Map<String, Object>) resultX1.get(pOfP.getName());
        final Map<String, Object> resultY1 = (Map<String, Object>) resultX1.get(qOfP.getName());
        final Collection<Map<String, Object>> xsOfX1 = (Collection<Map<String, Object>>) resultX1.get(psOfP.getName());
        final Collection<Map<String, Object>> ysOfX1 = (Collection<Map<String, Object>>) resultX1.get(qsOfP.getName());
        assertEquals(1, xsOfX1.size());
        assertEquals(2, ysOfX1.size());
        final Map<String, Object> resultX6 = xsOfX1.stream().filter(x -> Objects.equals(x.get(daoFixture.getUuid().getName()), ids.get(X6))).findAny().orElse(null);
        final Map<String, Object> resultY2 = ysOfX1.stream().filter(y -> Objects.equals(y.get(daoFixture.getUuid().getName()), ids.get(Y2))).findAny().orElse(null);
        final Map<String, Object> resultY3 = ysOfX1.stream().filter(y -> Objects.equals(y.get(daoFixture.getUuid().getName()), ids.get(Y3))).findAny().orElse(null);
        assertNotNull(resultY1);
        assertNotNull(resultX2);
        assertNotNull(resultX6);
        assertNotNull(resultY2);
        assertNotNull(resultY3);

        assertEquals(ids.get(Y1), resultY1.get(daoFixture.getUuid().getName()));
        assertNull(resultY1.get(pOfQ.getName()));
        assertEquals(Collections.emptySet(), resultY1.get(psOfQ.getName()));

        assertEquals(ids.get(X2), resultX2.get(daoFixture.getUuid().getName()));
        final Map<String, Object> resultX5 = (Map<String, Object>) resultX2.get(pOfP.getName());
        assertNull(resultX2.get(qOfP.getName()));
        final Collection<Map<String, Object>> xsOfX2 = (Collection<Map<String, Object>>) resultX2.get(psOfP.getName());
        assertEquals(Collections.emptySet(), resultX2.get(qsOfP.getName()));
        assertEquals(2, xsOfX2.size());
        final Map<String, Object> resultX3 = xsOfX2.stream().filter(x -> Objects.equals(x.get(daoFixture.getUuid().getName()), ids.get(X3))).findAny().orElse(null);
        final Map<String, Object> resultX4 = xsOfX2.stream().filter(x -> Objects.equals(x.get(daoFixture.getUuid().getName()), ids.get(X4))).findAny().orElse(null);
        assertNotNull(resultX5);
        assertNotNull(resultX3);
        assertNotNull(resultX4);

        assertEquals(ids.get(X5), resultX5.get(daoFixture.getUuid().getName()));
        assertNull(resultX5.get(pOfP.getName()));
        assertNull(resultX5.get(qOfP.getName()));
        assertEquals(Collections.emptySet(), resultX5.get(psOfP.getName()));
        assertEquals(Collections.emptySet(), resultX5.get(qsOfP.getName()));

        assertNull(resultX3.get(pOfP.getName()));
        assertNull(resultX3.get(qOfP.getName()));
        assertEquals(Collections.emptySet(), resultX3.get(psOfP.getName()));
        assertEquals(Collections.emptySet(), resultX3.get(qsOfP.getName()));

        assertNull(resultX4.get(pOfP.getName()));
        final Map<String, Object> resultY4 = (Map<String, Object>) resultX4.get(qOfP.getName());
        assertEquals(Collections.emptySet(), resultX4.get(psOfP.getName()));
        assertEquals(Collections.emptySet(), resultX4.get(qsOfP.getName()));
        assertNotNull(resultY4);

        assertEquals(ids.get(Y4), resultY4.get(daoFixture.getUuid().getName()));
        assertNull(resultY4.get(pOfQ.getName()));
        assertEquals(Collections.emptySet(), resultY4.get(psOfQ.getName()));

        final Map<String, Object> resultX7 = (Map<String, Object>) resultX6.get(pOfP.getName());
        assertNull(resultX6.get(qOfP.getName()));
        assertEquals(Collections.emptySet(), resultX6.get(psOfP.getName()));
        assertEquals(Collections.emptySet(), resultX6.get(qsOfP.getName()));
        assertNotNull(resultX7);

        assertEquals(ids.get(X7), resultX7.get(daoFixture.getUuid().getName()));
        assertNull(resultX7.get(pOfP.getName()));
        assertNull(resultX7.get(qOfP.getName()));
        final Collection<Map<String, Object>> xsOfX7 = (Collection<Map<String, Object>>) resultX7.get(psOfP.getName());
        assertEquals(Collections.emptySet(), resultX7.get(qsOfP.getName()));
        assertEquals(2, xsOfX7.size());
        final Map<String, Object> resultX8 = xsOfX7.stream().filter(x -> Objects.equals(x.get(daoFixture.getUuid().getName()), ids.get(X8))).findAny().orElse(null);
        final Map<String, Object> resultX9 = xsOfX7.stream().filter(x -> Objects.equals(x.get(daoFixture.getUuid().getName()), ids.get(X9))).findAny().orElse(null);
        assertNotNull(resultX8);
        assertNotNull(resultX9);

        assertNull(resultX8.get(pOfP.getName()));
        assertNull(resultX8.get(qOfP.getName()));
        assertEquals(Collections.emptySet(), resultX8.get(psOfP.getName()));
        assertEquals(Collections.emptySet(), resultX8.get(qsOfP.getName()));

        assertNull(resultX9.get(pOfP.getName()));
        assertNull(resultX9.get(qOfP.getName()));
        assertEquals(Collections.emptySet(), resultX9.get(psOfP.getName()));
        final Collection<Map<String, Object>> ysOfX9 = (Collection<Map<String, Object>>) resultX9.get(qsOfP.getName());
        assertEquals(2, ysOfX9.size());
        final Map<String, Object> resultY5 = ysOfX9.stream().filter(y -> Objects.equals(y.get(daoFixture.getUuid().getName()), ids.get(Y5))).findAny().orElse(null);
        final Map<String, Object> resultY6 = ysOfX9.stream().filter(y -> Objects.equals(y.get(daoFixture.getUuid().getName()), ids.get(Y6))).findAny().orElse(null);
        assertNotNull(resultY5);
        assertNotNull(resultY6);

        final Map<String, Object> resultX10 = (Map<String, Object>) resultY5.get(pOfQ.getName());
        assertEquals(Collections.emptySet(), resultY5.get(psOfQ.getName()));
        assertNotNull(resultX10);

        assertEquals(ids.get(X10), resultX10.get(daoFixture.getUuid().getName()));
        assertNull(resultX10.get(pOfP.getName()));
        assertNull(resultX10.get(qOfP.getName()));
        assertEquals(Collections.emptySet(), resultX10.get(psOfP.getName()));
        final Collection<Map<String, Object>> ysOfX10 = (Collection<Map<String, Object>>) resultX10.get(qsOfP.getName());
        assertEquals(2, ysOfX10.size());
        final Map<String, Object> resultY7 = ysOfX10.stream().filter(y -> Objects.equals(y.get(daoFixture.getUuid().getName()), ids.get(Y7))).findAny().orElse(null);
        final Map<String, Object> resultY8 = ysOfX10.stream().filter(y -> Objects.equals(y.get(daoFixture.getUuid().getName()), ids.get(Y8))).findAny().orElse(null);
        assertNotNull(resultY7);
        assertNotNull(resultY8);

        assertNull(resultY7.get(pOfQ.getName()));
        assertEquals(Collections.emptySet(), resultY7.get(psOfQ.getName()));

        final Map<String, Object> resultX13 = (Map<String, Object>) resultY8.get(pOfQ.getName());
        assertEquals(Collections.emptySet(), resultY8.get(psOfQ.getName()));
        assertNotNull(resultX13);

        assertEquals(ids.get(X13), resultX13.get(daoFixture.getUuid().getName()));
        assertNull(resultX13.get(pOfP.getName()));
        assertNull(resultX13.get(qOfP.getName()));
        assertEquals(Collections.emptySet(), resultX13.get(psOfP.getName()));
        assertEquals(Collections.emptySet(), resultX13.get(qsOfP.getName()));

        assertNull(resultY6.get(pOfQ.getName()));
        final Collection<Map<String, Object>> xsOfY6 = (Collection<Map<String, Object>>) resultY6.get(psOfP.getName());
        assertEquals(2, xsOfY6.size());
        final Map<String, Object> resultX11 = xsOfY6.stream().filter(x -> Objects.equals(x.get(daoFixture.getUuid().getName()), ids.get(X11))).findAny().orElse(null);
        final Map<String, Object> resultX12 = xsOfY6.stream().filter(x -> Objects.equals(x.get(daoFixture.getUuid().getName()), ids.get(X12))).findAny().orElse(null);
        assertNotNull(resultX11);
        assertNotNull(resultX12);

        assertNull(resultX11.get(pOfP.getName()));
        assertNull(resultX11.get(qOfP.getName()));
        assertEquals(Collections.emptySet(), resultX11.get(psOfP.getName()));
        assertEquals(Collections.emptySet(), resultX11.get(qsOfP.getName()));

        assertNull(resultX12.get(pOfP.getName()));
        final Map<String, Object> resultY9 = (Map<String, Object>) resultX12.get(qOfP.getName());
        assertEquals(Collections.emptySet(), resultX12.get(psOfP.getName()));
        assertEquals(Collections.emptySet(), resultX12.get(qsOfP.getName()));

        assertEquals(ids.get(Y9), resultY9.get(daoFixture.getUuid().getName()));
        assertNull(resultY9.get(pOfQ.getName()));
        assertEquals(Collections.emptySet(), resultY9.get(psOfQ.getName()));

        assertNull(resultY2.get(pOfQ.getName()));
        assertEquals(Collections.emptySet(), resultY2.get(psOfQ.getName()));

        assertNull(resultY3.get(pOfQ.getName()));
        assertEquals(Collections.emptySet(), resultY3.get(psOfQ.getName()));
    }
}
