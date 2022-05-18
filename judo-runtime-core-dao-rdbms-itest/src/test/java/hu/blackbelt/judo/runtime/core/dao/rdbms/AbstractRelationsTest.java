package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.psm.accesspoint.ActorType;
import hu.blackbelt.judo.meta.psm.data.AssociationEnd;
import hu.blackbelt.judo.meta.psm.data.Attribute;
import hu.blackbelt.judo.meta.psm.data.Containment;
import hu.blackbelt.judo.meta.psm.data.EntityType;
import hu.blackbelt.judo.meta.psm.derived.ExpressionDialect;
import hu.blackbelt.judo.meta.psm.derived.StaticNavigation;
import hu.blackbelt.judo.meta.psm.namespace.Model;
import hu.blackbelt.judo.meta.psm.service.MappedTransferObjectType;
import hu.blackbelt.judo.meta.psm.service.UnmappedTransferObjectType;
import hu.blackbelt.judo.meta.psm.type.StringType;
import hu.blackbelt.judo.meta.psm.type.util.builder.TypeBuilders;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Types;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.psm.accesspoint.util.builder.AccesspointBuilders.newActorTypeBuilder;
import static hu.blackbelt.judo.meta.psm.data.util.builder.DataBuilders.*;
import static hu.blackbelt.judo.meta.psm.derived.util.builder.DerivedBuilders.newReferenceExpressionTypeBuilder;
import static hu.blackbelt.judo.meta.psm.derived.util.builder.DerivedBuilders.newStaticNavigationBuilder;
import static hu.blackbelt.judo.meta.psm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.psm.service.util.builder.ServiceBuilders.*;
import static hu.blackbelt.judo.meta.psm.type.util.builder.TypeBuilders.newCardinalityBuilder;
import static hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Example relation strucyure.
 * <img src="doc-files/relations.png">
 */

/*
 * @startuml doc-files/relations.png
 * A *-- "0..1" B: b
 * A *-- "*" C: cs
 * B *-- "0..1" D: d
 * B *-- "*" E: es
 * C *-- "0..1" F: f
 * C *-- "*" G: gs
 * D *-- "0..1" H: h
 * D *-- "*" I: is
 * E *-- "0..1" J: j
 * E *-- "*" K: ks
 * F *-- "0..1" L: l
 * F *-- "*" M: ms
 * G *-- "0..1" N: n
 * G *-- "*" O: os
 *
 * A --> "0..1" A1: a1
 * A --> "*" A1: a1s
 * A "0..1" <-- A2: a
 * A "*" <-- A2: as
 * A "0..1\naoo" <--> "0..1\na3oo" A3
 * A "*\nasX" <--> "0..1\na3" A3
 * A "0..1\naX" <--> "*\na3s" A3
 * A "*\namm" <--> "*\na3mm" A3
 * A --> "1..1" A4: a4
 * A "1..1" <-- A5: a
 * A "0..1\naoo1" <--> "1..1\na6oo" A6
 * A "*\nasX1" <--> "1..1\na6" A6
 * A "1..1\naoo2" <--> "0..1\na7oo" A7
 * A "1..1\naX1" <--> "*\na7s" A7
 *
 * B --> "0..1" B1: b1
 * B --> "*" B1: b1s
 * B "0..1" <-- B2: b
 * B "*" <-- B2: bs
 * B "0..1\nboo" <--> "0..1\nb3oo" B3
 * B "*\nbsX" <--> "0..1\nb3" B3
 * B "0..1\nbX" <--> "*\nb3s" B3
 * B "*\nbmm" <--> "*\nb3mm" B3
 *
 * A *-- "0..1" B: b2
 *
 * A -- "*" A: parts
 * A -- "0..1" A: containment
 * @enduml
 */

@Slf4j
public abstract class AbstractRelationsTest {

    private static final String MODEL_NAME = "relations";
    public static final String ID = "id";

    public static final String REF_ID = "__referenceId";
    public static final String NAME = "name";
    public static final String ASSOC_A1 = "a1";
    public static final String ASSOC_A4 = "a4";
    public static final String ASSOC_A3OO = "a3oo";
    public static final String ASSOC_A6OO = "a6oo";
    public static final String ASSOC_A7OO = "a7oo";
    public static final String ASSOC_A3 = "a3";
    public static final String ASSOC_A6 = "a6";
    public static final String ASSOC_A7 = "a7";
    public static final String CON_H = "h";
    public static final String CON_IS = "is";
    public static final String CON_J = "j";
    public static final String CON_KS = "ks";
    public static final String CON_L = "l";
    public static final String CON_MS = "ms";
    public static final String CON_N = "n";
    public static final String CON_OS = "os";
    public static final String CON_D = "d";
    public static final String CON_ES = "es";
    public static final String ASSOC_B1 = "b1";
    public static final String ASSOC_B1S = "b1s";
    public static final String ASSOC_BOO = "boo";
    public static final String ASSOC_B3OO = "b3oo";
    public static final String ASSOC_BSX = "bsX";
    public static final String ASSOC_B3 = "b3";
    public static final String ASSOC_BX = "bX";
    public static final String ASSOC_B3S = "b3s";
    public static final String ASSOC_BMM = "bmm";
    public static final String ASSOC_B3MM = "b3mm";
    public static final String CON_F = "f";
    public static final String CON_GS = "gs";
    public static final String ASSOC_B = "b";
    public static final String CON_B = "b";
    public static final String CON_CS = "cs";
    public static final String ASSOC_A1S = "a1s";
    public static final String ASSOC_A = "a";
    public static final String ASSOC_AS = "as";
    public static final String ASSOC_AOO = "aoo";
    public static final String ASSOC_AOO1 = "aoo1";
    public static final String ASSOC_AOO2 = "aoo2";
    public static final String ASSOC_ASX = "asX";
    public static final String ASSOC_AX = "aX";
    public static final String ASSOC_ASX1 = "asX1";
    public static final String ASSOC_AX1 = "aX1";
    public static final String ASSOC_A3S = "a3s";
    public static final String ASSOC_A7S = "a7s";
    public static final String ASSOC_AMM = "amm";
    public static final String ASSOC_A3MM = "a3mm";
    public static final String ASSOC_BS = "bs";

    public static final String A_1 = "a_1";
    public static final String A1_1 = "a1_1";
    public static final String A4_1 = "a4_1";
    public static final String A4_2 = "a4_2";
    public static final String A3_OO = "a3_oo";
    public static final String A3_MO_1 = "a3_mo_1";
    public static final String A3_MO_2 = "a3_mo_2";
    public static final String A3_OM_1 = "a3_om_1";
    public static final String A3_OM_2 = "a3_om_2";
    public static final String A3_MM_1_1 = "a3_mm_1_1";
    public static final String A3_MM_1_2 = "a3_mm_1_2";
    public static final String A3_MM_2_2 = "a3_mm_2_2";
    public static final String A3_MM_2_1 = "a3_mm_2_1";
    public static final String A6_OO_1 = "a6_oo_1";
    public static final String A6_OO_2 = "a6_oo_2";
    public static final String A6_MO_1 = "a6_mo_1";
    public static final String A6_MO_2 = "a6_mo_2";
    public static final String A7_OO_1 = "a7_oo_1";
    public static final String A7_OM_1 = "a7_om_1";

    public static final String A1_2 = "a1_2";
    public static final String A1_3 = "a1_3";
    public static final String B1_1 = "b1_1";
    public static final String B1_2 = "b1_2";
    public static final String B1_3 = "b1_3";
    public static final String B3_OM_1 = "b3_om_1";
    public static final String B3_OM_2 = "b3_om_2";
    public static final String B3_MM_1_1 = "b3_mm_1_1";
    public static final String B3_MM_1_2 = "b3_mm_1_2";
    public static final String B3_MM_2_1 = "b3_mm_2_1";
    public static final String B3_MM_2_2 = "b3_mm_2_2";
    public static final String B3_OO = "b3_oo";
    public static final String B3_MO_1 = "b3_mo_1";
    public static final String B3_MO_2 = "b3_mo_2";
    public static final String D_1 = "d_1";
    public static final String H_1 = "h_1";
    public static final String I_1 = "i_1";
    public static final String I_2 = "i_2";
    public static final String E_1 = "e_1";
    public static final String J_1 = "j_1";
    public static final String K_1 = "k_1";
    public static final String K_2 = "k_2";
    public static final String E_2 = "e_2";
    public static final String J_2 = "j_2";
    public static final String K_3 = "k_3";
    public static final String K_4 = "k_4";
    public static final String C_1 = "c_1";
    public static final String F_1 = "f_1";
    public static final String L_1 = "l_1";
    public static final String B_1 = "b_1";
    public static final String M_1 = "m_1";
    public static final String M_2 = "m_2";
    public static final String G_1 = "g_1";
    public static final String N_1 = "n_1";
    public static final String O_1 = "o_1";
    public static final String O_2 = "o_2";
    public static final String G_2 = "g_2";
    public static final String N_2 = "n_2";
    public static final String O_3 = "o_3";
    public static final String O_4 = "o_4";
    public static final String C_2 = "c_2";
    public static final String F_2 = "f_2";
    public static final String L_2 = "l_2";
    public static final String M_3 = "m_3";
    public static final String M_4 = "m_4";
    public static final String G_3 = "g_3";
    public static final String N_3 = "n_3";
    public static final String O_5 = "o_5";
    public static final String O_6 = "o_6";
    public static final String G_4 = "g_4";
    public static final String N_4 = "n_4";
    public static final String O_7 = "o_7";
    public static final String O_8 = "o_8";
    public static final String A_2 = "a_2";
    public static final String B_2 = "b_2";
    public static final String A2_1 = "a2_1";
    public static final String A2_2 = "a2_2";
    public static final String A5_1 = "a5_1";
    public static final String A2_3 = "a2_3";
    public static final String B2_1 = "b2_1";
    public static final String B2_2 = "b2_2";
    public static final String B2_3 = "b2_3";

    public static final String DUMMY_ACCESS_POINT_NAME = "Dummy";
    public static final String DUMMY_ALL_MS = "allMs";

    public String getModelName() {
        return MODEL_NAME;
    }

    public EClass a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, a1, a2, a3, a4, a5, a6, a7, b1, b2, b3;
    public EClass aM, bM, cM, dM, eM, fM, gM, hM, iM, jM, kM, lM, mM, nM, oM, a1M, a2M, a2M_Insert, a3M, a3M_Insert, a4M, a5M, a5M_Insert, a6M, a6M_Insert, a7M, a7M_Insert, b1M, b2M, b2M_Insert, b3M, b3M_Insert;
    public EReference a1s, as, a3s, b1s, bs, b3s;
    public EReference a1sMR, asMR, a3sMR, b1sMR, b1MR, bsMR, b3sMR, b3ooMR, isMR, aRefMR, a4RefMR, bMR, dMR;

    public EClass dummy;
    public EReference allMsInDummy;

    public UUID a_1_id;
    public UUID a_2_id;
    public UUID b_1_id;
    public UUID b_2_id;
    public UUID c_1_id;
    public UUID c_2_id;
    public UUID d_1_id;
    public UUID e_1_id;
    public UUID e_2_id;
    public UUID f_1_id;
    public UUID f_2_id;
    public UUID g_1_id;
    public UUID g_2_id;
    public UUID g_3_id;
    public UUID g_4_id;
    public UUID h_1_id;
    public UUID i_1_id;
    public UUID i_2_id;
    public UUID j_1_id;
    public UUID j_2_id;
    public UUID k_1_id;
    public UUID k_2_id;
    public UUID k_3_id;
    public UUID k_4_id;
    public UUID l_1_id;
    public UUID l_2_id;
    public UUID m_1_id;
    public UUID m_2_id;
    public UUID m_3_id;
    public UUID m_4_id;
    public UUID n_1_id;
    public UUID n_2_id;
    public UUID n_3_id;
    public UUID n_4_id;
    public UUID o_1_id;
    public UUID o_2_id;
    public UUID o_3_id;
    public UUID o_4_id;
    public UUID o_5_id;
    public UUID o_6_id;
    public UUID o_7_id;
    public UUID o_8_id;

    public UUID a1_1_id;
    public UUID a1_2_id;
    public UUID a1_3_id;

    public UUID a2_1_id;
    public UUID a2_2_id;
    public UUID a2_3_id;

    public UUID a3_oo_id;
    public UUID a3_mo_1_id;
    public UUID a3_mo_2_id;
    public UUID a3_om_1_id;
    public UUID a3_om_2_id;
    public UUID a3_mm_1_1_id;
    public UUID a3_mm_1_2_id;
    public UUID a3_mm_2_1_id;
    public UUID a3_mm_2_2_id;

    public UUID a4_1_id;
    public UUID a4_2_id;
    public UUID a5_1_id;
    public UUID a6_oo_1_id;
    public UUID a6_oo_2_id;
    public UUID a6_mo_1_id;
    public UUID a6_mo_2_id;
    public UUID a7_oo_1_id;
    public UUID a7_om_1_id;

    public UUID b1_1_id;
    public UUID b1_2_id;
    public UUID b1_3_id;

    public UUID b2_1_id;
    public UUID b2_2_id;
    public UUID b2_3_id;

    public UUID b3_oo_id;
    public UUID b3_mo_1_id;
    public UUID b3_mo_2_id;
    public UUID b3_om_1_id;
    public UUID b3_om_2_id;
    public UUID b3_mm_1_1_id;
    public UUID b3_mm_1_2_id;
    public UUID b3_mm_2_1_id;
    public UUID b3_mm_2_2_id;

    public class TestDataHolder {
        Set<TestEntry> entities = new HashSet<>();

        public TestDataHolder() {
            entities.add(new TestEntry(a_1_id, A_1, a, aM));
            entities.add(new TestEntry(a_2_id, A_2, a, aM));
            entities.add(new TestEntry(b_1_id, B_1, b, bM));
            entities.add(new TestEntry(b_2_id, B_2, b, bM));
            entities.add(new TestEntry(a1_1_id, A1_1, a1, a1M));
            entities.add(new TestEntry(a1_2_id, A1_2, a1, a1M));
            entities.add(new TestEntry(a1_3_id, A1_3, a1, a1M));
            entities.add(new TestEntry(a3_oo_id, A3_OO, a3, a3M));
            entities.add(new TestEntry(a3_mo_1_id, A3_MO_1, a3, a3M));
            entities.add(new TestEntry(a3_mo_2_id, A3_MO_2, a3, a3M));
            entities.add(new TestEntry(a3_om_1_id, A3_OM_1, a3, a3M));
            entities.add(new TestEntry(a3_om_2_id, A3_OM_2, a3, a3M));
            entities.add(new TestEntry(a3_mm_1_1_id, A3_MM_1_1, a3, a3M));
            entities.add(new TestEntry(a3_mm_1_2_id, A3_MM_1_2, a3, a3M));
            entities.add(new TestEntry(a3_mm_2_1_id, A3_MM_2_1, a3, a3M));
            entities.add(new TestEntry(a3_mm_2_2_id, A3_MM_2_2, a3, a3M));
            entities.add(new TestEntry(a4_1_id, A4_1, a4, a4M));
            entities.add(new TestEntry(a4_2_id, A4_2, a4, a4M));
            entities.add(new TestEntry(a6_oo_1_id, A6_OO_1, a6, a6M));
            entities.add(new TestEntry(a6_oo_2_id, A6_OO_2, a6, a6M));
            entities.add(new TestEntry(a6_mo_1_id, A6_MO_1, a6, a6M));
            entities.add(new TestEntry(a6_mo_2_id, A6_MO_2, a6, a6M));
            entities.add(new TestEntry(a7_oo_1_id, A7_OO_1, a7, a7M));
            entities.add(new TestEntry(a7_om_1_id, A7_OM_1, a7, a7M));
            entities.add(new TestEntry(b1_1_id, B1_1, b1, b1M));
            entities.add(new TestEntry(b1_2_id, B1_2, b1, b1M));
            entities.add(new TestEntry(b1_3_id, B1_3, b1, b1M));
            entities.add(new TestEntry(b3_om_1_id, B3_OM_1, b3, b3M));
            entities.add(new TestEntry(b3_om_2_id, B3_OM_2, b3, b3M));
            entities.add(new TestEntry(b3_mm_1_1_id, B3_MM_1_1, b3, b3M));
            entities.add(new TestEntry(b3_mm_1_2_id, B3_MM_1_2, b3, b3M));
            entities.add(new TestEntry(b3_mm_2_1_id, B3_MM_2_1, b3, b3M));
            entities.add(new TestEntry(b3_mm_2_2_id, B3_MM_2_2, b3, b3M));
            entities.add(new TestEntry(b3_om_1_id, B3_OM_1, b3, b3M));
            entities.add(new TestEntry(b3_om_2_id, B3_OM_2, b3, b3M));
            entities.add(new TestEntry(b3_mo_1_id, B3_MO_1, b3, b3M));
            entities.add(new TestEntry(b3_mo_2_id, B3_MO_2, b3, b3M));
            entities.add(new TestEntry(b3_oo_id, B3_OO, b3, b3M));
            entities.add(new TestEntry(c_1_id, C_1, c, cM));
            entities.add(new TestEntry(c_2_id, C_2, c, cM));
            entities.add(new TestEntry(d_1_id, D_1, d, dM));
            entities.add(new TestEntry(e_1_id, E_1, e, eM));
            entities.add(new TestEntry(e_2_id, E_2, e, eM));
            entities.add(new TestEntry(f_1_id, F_1, f, fM));
            entities.add(new TestEntry(f_2_id, F_2, f, fM));
            entities.add(new TestEntry(g_1_id, G_1, g, gM));
            entities.add(new TestEntry(g_2_id, G_2, g, gM));
            entities.add(new TestEntry(g_3_id, G_3, g, gM));
            entities.add(new TestEntry(g_4_id, G_4, g, gM));
            entities.add(new TestEntry(h_1_id, H_1, h, hM));
            entities.add(new TestEntry(i_1_id, I_1, i, iM));
            entities.add(new TestEntry(i_2_id, I_2, i, iM));
            entities.add(new TestEntry(j_1_id, J_1, j, jM));
            entities.add(new TestEntry(j_2_id, J_2, j, jM));
            entities.add(new TestEntry(k_1_id, K_1, k, kM));
            entities.add(new TestEntry(k_2_id, K_2, k, kM));
            entities.add(new TestEntry(k_3_id, K_3, k, kM));
            entities.add(new TestEntry(k_4_id, K_4, k, kM));
            entities.add(new TestEntry(l_1_id, L_1, l, lM));
            entities.add(new TestEntry(l_2_id, L_2, l, lM));
            entities.add(new TestEntry(m_1_id, M_1, m, mM));
            entities.add(new TestEntry(m_2_id, M_2, m, mM));
            entities.add(new TestEntry(m_3_id, M_3, m, mM));
            entities.add(new TestEntry(m_4_id, M_4, m, mM));
            entities.add(new TestEntry(n_1_id, N_1, n, nM));
            entities.add(new TestEntry(n_2_id, N_2, n, nM));
            entities.add(new TestEntry(n_3_id, N_3, n, nM));
            entities.add(new TestEntry(n_4_id, N_4, n, nM));
            entities.add(new TestEntry(o_1_id, O_1, o, oM));
            entities.add(new TestEntry(o_2_id, O_2, o, oM));
            entities.add(new TestEntry(o_3_id, O_3, o, oM));
            entities.add(new TestEntry(o_4_id, O_4, o, oM));
            entities.add(new TestEntry(o_5_id, O_5, o, oM));
            entities.add(new TestEntry(o_6_id, O_6, o, oM));
            entities.add(new TestEntry(o_7_id, O_7, o, oM));
            entities.add(new TestEntry(o_8_id, O_8, o, oM));
        }

        public TestEntry byName(String name) {
            return entities.stream().filter(e -> e.name.equals(name)).findFirst().get();
        }

        public TestEntry byId(UUID id) {
            return entities.stream().filter(e -> e.id.equals(id)).findFirst().get();
        }

    }

    @AllArgsConstructor
    @Getter
    public class TestEntry {
        UUID id;
        @Setter
        String name;
        EClass entityType;
        EClass transferObjectType;

        public Payload toIdNamePayload() {
            return map(NAME, name, uuid.getName(), id, ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(entityType));
        }

        public Payload toIdPayload() {
            return map(uuid.getName(), id);
        }

        public Payload toNamePayload() {
            return map(NAME, name);
        }

        public UUID loadByName() {
            return getUuidByName(name, entityType);
        }
    }

    protected JudoRuntimeFixture runtimeFixture;
    protected IdentifierProvider<UUID> uuid;
    protected JudoDatasourceFixture datasourceFixture;

    void init(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        this.runtimeFixture = runtimeFixture;
        this.datasourceFixture = datasourceFixture;
        runtimeFixture.init(getPsmModel(), datasourceFixture);
        this.uuid = runtimeFixture.getIdProvider();

        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        a = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A").get();
        b = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".B").get();
        c = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".C").get();
        d = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".D").get();
        e = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".E").get();
        f = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".F").get();
        g = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".G").get();
        h = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".H").get();
        i = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".I").get();
        j = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".J").get();
        k = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".K").get();
        l = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".L").get();
        m = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".M").get();
        n = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".N").get();
        o = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".O").get();

        a1 = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A1").get();
        a2 = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A2").get();
        a3 = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A3").get();
        a4 = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A4").get();
        a5 = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A5").get();
        a6 = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A6").get();
        a7 = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A7").get();

        a1s = (EReference) a.getEStructuralFeature(ASSOC_A1S);
        as = (EReference) a2.getEStructuralFeature(ASSOC_AS);
        a3s = (EReference) a.getEStructuralFeature(ASSOC_A3MM);

        b1 = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".B1").get();
        b2 = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".B2").get();
        b3 = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".B3").get();

        b1s = (EReference) b.getEStructuralFeature(ASSOC_B1S);
        bs = (EReference) b2.getEStructuralFeature(ASSOC_BS);
        b3s = (EReference) b.getEStructuralFeature(ASSOC_B3MM);

        aM = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".AM").get();
        bM = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".BM").get();
        cM = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".CM").get();
        dM = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".DM").get();
        eM = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".EM").get();
        fM = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".FM").get();
        gM = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".GM").get();
        hM = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".HM").get();
        iM = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".IM").get();
        jM = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".JM").get();
        kM = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".KM").get();
        lM = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".LM").get();
        mM = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".MM").get();
        nM = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".NM").get();
        oM = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".OM").get();

        a1M = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A1M").get();
        a2M = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A2M").get();
        a2M_Insert = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A2M_INS").get();

        a3M = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A3M").get();
        a3M_Insert = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A3M_INS").get();
        a4M = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A4M").get();

        a5M = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A5M").get();
        a5M_Insert = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A5M_INS").get();
        a6M = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A6M").get();
        a6M_Insert = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A6M_INS").get();
        a7M = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A7M").get();
        a7M_Insert = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".A7M_INS").get();

        b1M = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".B1M").get();
        b2M = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".B2M").get();
        b2M_Insert = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".B2M_INS").get();
        b3M = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".B3M").get();
        b3M_Insert = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".B3M_INS").get();

        a1sMR = (EReference) aM.getEStructuralFeature(ASSOC_A1S);
        asMR = (EReference) a2M.getEStructuralFeature(ASSOC_AS);
        a3sMR = (EReference) aM.getEStructuralFeature(ASSOC_A3MM);
        b1MR = (EReference) bM.getEStructuralFeature(ASSOC_B1);
        b1sMR = (EReference) bM.getEStructuralFeature(ASSOC_B1S);
        bsMR = (EReference) b2M.getEStructuralFeature(ASSOC_BS);
        b3sMR = (EReference) bM.getEStructuralFeature(ASSOC_B3MM);
        b3ooMR = (EReference) bM.getEStructuralFeature(ASSOC_B3OO);
        isMR = (EReference) dM.getEStructuralFeature(CON_IS);

        dummy = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + "." + DUMMY_ACCESS_POINT_NAME).get();
        allMsInDummy = (EReference) dummy.getEStructuralFeature(DUMMY_ALL_MS);

        aRefMR = (EReference) a2M.getEStructuralFeature(ASSOC_A);
        a4RefMR = (EReference) aM.getEStructuralFeature(ASSOC_A4);
        bMR = (EReference) aM.getEStructuralFeature(CON_B);
        dMR = (EReference) bM.getEStructuralFeature(CON_D);
    }

    public Model getPsmModel() {
        final StringType stringType = TypeBuilders.newStringTypeBuilder().withName("String").withMaxLength(100).build();

        // H
        final Attribute h_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType h = newEntityTypeBuilder().withName("H").withAttributes(h_name).build();
        final MappedTransferObjectType hM = newMappedTransferObjectTypeBuilder().withName("HM")
                .withEntityType(h)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(h_name))
                .build();

        // I
        final Attribute i_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType i = newEntityTypeBuilder().withName("I").withAttributes(i_name).build();
        final MappedTransferObjectType iM = newMappedTransferObjectTypeBuilder().withName("IM")
                .withEntityType(i)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(i_name))
                .build();

        // J
        final Attribute j_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType j = newEntityTypeBuilder().withName("J").withAttributes(j_name).build();
        final MappedTransferObjectType jM = newMappedTransferObjectTypeBuilder().withName("JM")
                .withEntityType(j)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(j_name))
                .build();

        // K
        final Attribute k_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType k = newEntityTypeBuilder().withName("K").withAttributes(k_name).build();
        final MappedTransferObjectType kM = newMappedTransferObjectTypeBuilder().withName("KM")
                .withEntityType(k)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(k_name))
                .build();

        // L
        final Attribute l_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType l = newEntityTypeBuilder().withName("L").withAttributes(l_name).build();
        final MappedTransferObjectType lM = newMappedTransferObjectTypeBuilder().withName("LM")
                .withEntityType(l)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(l_name))
                .build();

        // M
        final Attribute m_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType m = newEntityTypeBuilder().withName("M").withAttributes(m_name).build();
        final MappedTransferObjectType mM = newMappedTransferObjectTypeBuilder().withName("MM")
                .withEntityType(m)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(m_name))
                .build();

        // N
        final Attribute n_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType n = newEntityTypeBuilder().withName("N").withAttributes(n_name).build();
        final MappedTransferObjectType nM = newMappedTransferObjectTypeBuilder().withName("NM")
                .withEntityType(n)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(n_name))
                .build();

        // O
        final Attribute o_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType o = newEntityTypeBuilder().withName("O").withAttributes(o_name).build();
        final MappedTransferObjectType oM = newMappedTransferObjectTypeBuilder().withName("OM")
                .withEntityType(o)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(o_name))
                .build();

        // D
        final Attribute d_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final Containment hOfD = newContainmentBuilder().withName(CON_H).withTarget(h).withCardinality(newCardinalityBuilder().build()).build();
        final Containment isOfD = newContainmentBuilder().withName(CON_IS).withTarget(i).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();
        final EntityType d = newEntityTypeBuilder().withName("D")
                .withRelations(Arrays.asList(hOfD, isOfD)).withAttributes(d_name).build();
        final MappedTransferObjectType dM = newMappedTransferObjectTypeBuilder().withName("DM")
                .withEntityType(d)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(d_name))
                .withRelations(Arrays.asList(
                        newTransferObjectRelationBuilder().withName(CON_H)
                                .withEmbedded(true).withBinding(hOfD).withTarget(hM)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(CON_IS)
                                .withEmbedded(true).withBinding(isOfD).withTarget(iM)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build()
                )).build();

        // E
        final Attribute e_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final Containment jOfE = newContainmentBuilder().withName(CON_J).withTarget(j).withCardinality(newCardinalityBuilder().build()).build();
        final Containment ksOfE = newContainmentBuilder().withName(CON_KS).withTarget(k).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();
        final EntityType e = newEntityTypeBuilder().withName("E")
                .withRelations(Arrays.asList(jOfE, ksOfE)).withAttributes(e_name).build();
        final MappedTransferObjectType eM = newMappedTransferObjectTypeBuilder().withName("EM")
                .withEntityType(e)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(e_name))
                .withRelations(Arrays.asList(
                        newTransferObjectRelationBuilder().withName(CON_J)
                                .withEmbedded(true).withBinding(jOfE).withTarget(jM)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(CON_KS)
                                .withEmbedded(true).withBinding(ksOfE).withTarget(kM)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build()
                )).build();


        // F
        final Attribute f_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final Containment lOfF = newContainmentBuilder().withName(CON_L).withTarget(l).withCardinality(newCardinalityBuilder().build()).build();
        final Containment msOfF = newContainmentBuilder().withName(CON_MS).withTarget(m).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();
        final EntityType f = newEntityTypeBuilder().withName("F")
                .withRelations(Arrays.asList(lOfF, msOfF)).withAttributes(f_name).build();
        final MappedTransferObjectType fM = newMappedTransferObjectTypeBuilder().withName("FM")
                .withEntityType(f)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(f_name))
                .withRelations(Arrays.asList(
                        newTransferObjectRelationBuilder().withName(CON_L)
                                .withEmbedded(true).withBinding(lOfF).withTarget(lM)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(CON_MS)
                                .withEmbedded(true).withBinding(msOfF).withTarget(mM)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build()
                )).build();

        // G
        final Attribute g_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final Containment nOfG = newContainmentBuilder().withName(CON_N).withTarget(n).withCardinality(newCardinalityBuilder().build()).build();
        final Containment osOfG = newContainmentBuilder().withName(CON_OS).withTarget(o).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();
        final EntityType g = newEntityTypeBuilder().withName("G")
                .withRelations(Arrays.asList(nOfG, osOfG)).withAttributes(g_name).build();
        final MappedTransferObjectType gM = newMappedTransferObjectTypeBuilder().withName("GM")
                .withEntityType(g)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(g_name))
                .withRelations(Arrays.asList(
                        newTransferObjectRelationBuilder().withName(CON_N)
                                .withEmbedded(true).withBinding(nOfG).withTarget(nM)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(CON_OS)
                                .withEmbedded(true).withBinding(osOfG).withTarget(oM)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build()
                )).build();

        // B1
        final Attribute b1_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType b1 = newEntityTypeBuilder().withName("B1").withAttributes(b1_name).build();
        final MappedTransferObjectType b1M = newMappedTransferObjectTypeBuilder().withName("B1M")
                .withEntityType(b1)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(b1_name))
                .build();
        final Containment dOfB = newContainmentBuilder().withName(CON_D).withTarget(d).withCardinality(newCardinalityBuilder().build()).build();
        final Containment esOfB = newContainmentBuilder().withName(CON_ES).withTarget(e).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();
        final AssociationEnd b1OfB = newAssociationEndBuilder().withName(ASSOC_B1).withTarget(b1).withCardinality(newCardinalityBuilder().build()).build();
        final AssociationEnd b1sOfB = newAssociationEndBuilder().withName(ASSOC_B1S).withTarget(b1).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();

        // B
        final Attribute b_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType b = newEntityTypeBuilder().withName("B")
                .withRelations(Arrays.asList(dOfB, esOfB, b1OfB, b1sOfB)).withAttributes(b_name).build();
        final MappedTransferObjectType bM = newMappedTransferObjectTypeBuilder().withName("BM")
                .withEntityType(b)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(b_name))
                .withRelations(Arrays.asList(
                        newTransferObjectRelationBuilder().withName(CON_D)
                                .withEmbedded(true).withBinding(dOfB).withTarget(dM)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(CON_ES)
                                .withEmbedded(true).withBinding(esOfB).withTarget(eM)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_B1)
                                .withEmbedded(true).withBinding(b1OfB).withTarget(b1M)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_B1S)
                                .withEmbedded(true).withBinding(b1sOfB).withTarget(b1M)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build()
                )).build();


        // B2
        final Attribute b2_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();

        final AssociationEnd bOfB2 = newAssociationEndBuilder().withName(ASSOC_B).withTarget(b).withCardinality(newCardinalityBuilder().build()).build();
        final AssociationEnd bsOfB2 = newAssociationEndBuilder().withName(ASSOC_BS).withTarget(b).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();

        final EntityType b2 = newEntityTypeBuilder().withName("B2").withAttributes(b2_name)
                .withRelations(Arrays.asList(
                        bOfB2, bsOfB2
                )).build();

        final MappedTransferObjectType b2M = newMappedTransferObjectTypeBuilder().withName("B2M")
                .withEntityType(b2)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(b2_name))
                .build();

        final MappedTransferObjectType b2M_Insert = newMappedTransferObjectTypeBuilder().withName("B2M_INS")
                .withEntityType(b2)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(b2_name))
                .withRelations(Arrays.asList(
                        newTransferObjectRelationBuilder().withName(ASSOC_B)
                                .withEmbedded(false).withBinding(bOfB2).withTarget(bM)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_BS)
                                .withEmbedded(false).withBinding(bsOfB2).withTarget(bM)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build()
                ))
                .build();

        // B3
        final Attribute b3_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType b3 = newEntityTypeBuilder().withName("B3").withAttributes(b3_name).build();
        final MappedTransferObjectType b3M = newMappedTransferObjectTypeBuilder().withName("B3M")
                .withEntityType(b3)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(b3_name))
                .build();

        final MappedTransferObjectType b3M_Insert = newMappedTransferObjectTypeBuilder().withName("B3M_INS")
                .withEntityType(b3)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(b3_name))
                .build();

        final AssociationEnd _boo = newAssociationEndBuilder().withName(ASSOC_BOO).withTarget(b).withCardinality(newCardinalityBuilder().build()).build();
        final AssociationEnd _b3oo = newAssociationEndBuilder().withName(ASSOC_B3OO).withTarget(b3).withCardinality(newCardinalityBuilder().build()).withPartner(_boo).build();
        _boo.setPartner(_b3oo);

        final AssociationEnd _bsX = newAssociationEndBuilder().withName(ASSOC_BSX).withTarget(b).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();
        final AssociationEnd _b3 = newAssociationEndBuilder().withName(ASSOC_B3).withTarget(b3).withCardinality(newCardinalityBuilder().build()).withPartner(_bsX).build();
        _bsX.setPartner(_b3);

        final AssociationEnd _bX = newAssociationEndBuilder().withName(ASSOC_BX).withTarget(b).withCardinality(newCardinalityBuilder().build()).build();
        final AssociationEnd _b3s = newAssociationEndBuilder().withName(ASSOC_B3S).withTarget(b3).withCardinality(newCardinalityBuilder().withUpper(-1).build()).withPartner(_bX).build();
        _bX.setPartner(_b3s);

        final AssociationEnd _bmm = newAssociationEndBuilder().withName(ASSOC_BMM).withTarget(b).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();
        final AssociationEnd _b3mm = newAssociationEndBuilder().withName(ASSOC_B3MM).withTarget(b3).withCardinality(newCardinalityBuilder().withUpper(-1).build()).withPartner(_bmm).build();
        _bmm.setPartner(_b3mm);

        b.getRelations().addAll(Arrays.asList(_b3oo, _b3, _b3s, _b3mm));
        b3.getRelations().addAll(Arrays.asList(_boo, _bX, _bsX, _bmm));

        bM.getRelations().addAll(
                Arrays.asList(
                        newTransferObjectRelationBuilder().withName(ASSOC_B3OO)
                                .withEmbedded(true).withBinding(_b3oo).withTarget(b3M)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_B3)
                                .withEmbedded(true).withBinding(_b3).withTarget(b3M)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_B3S)
                                .withEmbedded(true).withBinding(_b3s).withTarget(b3M)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_B3MM)
                                .withEmbedded(true).withBinding(_b3mm).withTarget(b3M)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build()
                ));

        b3M_Insert.getRelations().addAll(
                Arrays.asList(
                        newTransferObjectRelationBuilder().withName(ASSOC_BOO)
                                .withEmbedded(false).withBinding(_boo).withTarget(bM)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_BSX)
                                .withEmbedded(false).withBinding(_bsX).withTarget(bM)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_BX)
                                .withEmbedded(false).withBinding(_bX).withTarget(bM)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_BMM)
                                .withEmbedded(false).withBinding(_bmm).withTarget(bM)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build()
                ));


        // C
        final Attribute c_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final Containment fOfC = newContainmentBuilder().withName(CON_F).withTarget(f).withCardinality(newCardinalityBuilder().build()).build();
        final Containment gsOfC = newContainmentBuilder().withName(CON_GS).withTarget(g).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();
        final EntityType c = newEntityTypeBuilder().withName("C")
                .withRelations(Arrays.asList(fOfC, gsOfC)).withAttributes(c_name).build();
        final MappedTransferObjectType cM = newMappedTransferObjectTypeBuilder().withName("CM")
                .withEntityType(c)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(c_name))
                .withRelations(Arrays.asList(
                        newTransferObjectRelationBuilder().withName(CON_F)
                                .withEmbedded(true).withBinding(fOfC).withTarget(fM)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(CON_GS)
                                .withEmbedded(true).withBinding(gsOfC).withTarget(gM)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build()
                )).build();

        // A1
        final Attribute a1_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType a1 = newEntityTypeBuilder().withName("A1").withAttributes(a1_name).build();
        final MappedTransferObjectType a1M = newMappedTransferObjectTypeBuilder().withName("A1M")
                .withEntityType(a1)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(a1_name))
                .build();
        final AssociationEnd a1OfA = newAssociationEndBuilder().withName(ASSOC_A1).withTarget(a1).withCardinality(newCardinalityBuilder().build()).build();
        final AssociationEnd a1sOfA = newAssociationEndBuilder().withName(ASSOC_A1S).withTarget(a1).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();

        // A4
        final Attribute a4_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType a4 = newEntityTypeBuilder().withName("A4").withAttributes(a4_name).build();
        final MappedTransferObjectType a4M = newMappedTransferObjectTypeBuilder().withName("A4M")
                .withEntityType(a4)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(a4_name))
                .build();
        final AssociationEnd a4OfA = newAssociationEndBuilder().withName(ASSOC_A4).withTarget(a4).withCardinality(newCardinalityBuilder().withLower(1).build()).build();

        // A
        final Attribute a_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();

        final Containment bOfA = newContainmentBuilder().withName(CON_B).withTarget(b).withCardinality(newCardinalityBuilder().build()).build();
        final Containment csOfA = newContainmentBuilder().withName(CON_CS).withTarget(c).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();

        final EntityType a = newEntityTypeBuilder().withName("A")
                .withRelations(Arrays.asList(bOfA, csOfA, a1OfA, a1sOfA, a4OfA)).withAttributes(a_name).build();
        final MappedTransferObjectType aM = newMappedTransferObjectTypeBuilder().withName("AM")
                .withEntityType(a)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(a_name))
                .withRelations(Arrays.asList(
                        newTransferObjectRelationBuilder().withName(CON_B)
                                .withEmbedded(true).withBinding(bOfA).withTarget(bM)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(CON_CS)
                                .withEmbedded(true).withBinding(csOfA).withTarget(cM)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_A1)
                                .withEmbedded(true).withBinding(a1OfA).withTarget(a1M)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_A1S)
                                .withEmbedded(true).withBinding(a1sOfA).withTarget(a1M)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_A4)
                                .withEmbedded(true).withBinding(a4OfA).withTarget(a4M)
                                .withCardinality(newCardinalityBuilder().withLower(1).build()).build()
                )).build();

        // A2
        final Attribute a2_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final AssociationEnd aOfA2 = newAssociationEndBuilder().withName(ASSOC_A).withTarget(a).withCardinality(newCardinalityBuilder().build()).build();
        final AssociationEnd asOfA2 = newAssociationEndBuilder().withName(ASSOC_AS).withTarget(a).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();
        final EntityType a2 = newEntityTypeBuilder().withName("A2").withAttributes(a2_name)
                .withRelations(Arrays.asList(aOfA2, asOfA2)).build();
        final MappedTransferObjectType a2M = newMappedTransferObjectTypeBuilder().withName("A2M")
                .withEntityType(a2)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(a2_name))
                .withRelations(newTransferObjectRelationBuilder()
                        .withName(ASSOC_A)
                        .withTarget(aM)
                        .withCardinality(newCardinalityBuilder().build())
                        .withBinding(aOfA2)
                        .build())
                .build();

        final MappedTransferObjectType a2M_Insert = newMappedTransferObjectTypeBuilder().withName("A2M_INS")
                .withEntityType(a2)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(a2_name))
                .withRelations(Arrays.asList(
                        newTransferObjectRelationBuilder().withName(ASSOC_A)
                                .withEmbedded(false).withBinding(aOfA2).withTarget(aM)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_AS)
                                .withEmbedded(false).withBinding(asOfA2).withTarget(aM)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build()
                ))
                .build();

        // A3
        final Attribute a3_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType a3 = newEntityTypeBuilder().withName("A3").withAttributes(a3_name).build();
        final MappedTransferObjectType a3M = newMappedTransferObjectTypeBuilder().withName("A3M")
                .withEntityType(a3)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(a3_name))
                .build();

        final MappedTransferObjectType a3M_Insert = newMappedTransferObjectTypeBuilder().withName("A3M_INS")
                .withEntityType(a3)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(a3_name))
                .build();

        final AssociationEnd _aoo = newAssociationEndBuilder().withName(ASSOC_AOO).withTarget(a).withCardinality(newCardinalityBuilder().build()).build();
        final AssociationEnd _a3oo = newAssociationEndBuilder().withName(ASSOC_A3OO).withTarget(a3).withCardinality(newCardinalityBuilder().build()).withPartner(_aoo).build();
        _aoo.setPartner(_a3oo);

        final AssociationEnd _asX = newAssociationEndBuilder().withName(ASSOC_ASX).withTarget(a).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();
        final AssociationEnd _a3 = newAssociationEndBuilder().withName(ASSOC_A3).withTarget(a3).withCardinality(newCardinalityBuilder().build()).withPartner(_asX).build();
        _asX.setPartner(_a3);

        final AssociationEnd _aX = newAssociationEndBuilder().withName(ASSOC_AX).withTarget(a).withCardinality(newCardinalityBuilder().build()).build();
        final AssociationEnd _a3s = newAssociationEndBuilder().withName(ASSOC_A3S).withTarget(a3).withCardinality(newCardinalityBuilder().withUpper(-1).build()).withPartner(_aX).build();
        _aX.setPartner(_a3s);

        final AssociationEnd _amm = newAssociationEndBuilder().withName(ASSOC_AMM).withTarget(a).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();
        final AssociationEnd _a3mm = newAssociationEndBuilder().withName(ASSOC_A3MM).withTarget(a3).withCardinality(newCardinalityBuilder().withUpper(-1).build()).withPartner(_amm).build();
        _amm.setPartner(_a3mm);

        a.getRelations().addAll(Arrays.asList(_a3oo, _a3, _a3s, _a3mm));
        a3.getRelations().addAll(Arrays.asList(_aoo, _aX, _asX, _amm));

        a3M_Insert.getRelations().addAll(
                Arrays.asList(
                        newTransferObjectRelationBuilder().withName(ASSOC_AOO)
                                .withEmbedded(false).withBinding(_aoo).withTarget(aM)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_ASX)
                                .withEmbedded(false).withBinding(_asX).withTarget(aM)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_AX)
                                .withEmbedded(false).withBinding(_aX).withTarget(aM)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_AMM)
                                .withEmbedded(false).withBinding(_amm).withTarget(aM)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build()
                ));


        aM.getRelations().addAll(
                Arrays.asList(
                        newTransferObjectRelationBuilder().withName(ASSOC_A3OO)
                                .withEmbedded(true).withBinding(_a3oo).withTarget(a3M)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_A3)
                                .withEmbedded(true).withBinding(_a3).withTarget(a3M)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_A3S)
                                .withEmbedded(true).withBinding(_a3s).withTarget(a3M)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_A3MM)
                                .withEmbedded(true).withBinding(_a3mm).withTarget(a3M)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build()
                ));

        // A5
        final Attribute a5_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final AssociationEnd aOfA5 = newAssociationEndBuilder().withName(ASSOC_A).withTarget(a).withCardinality(newCardinalityBuilder().withLower(1).build()).build();
        final EntityType a5 = newEntityTypeBuilder().withName("A5").withAttributes(a5_name)
                .withRelations(Arrays.asList(aOfA5)).build();
        final MappedTransferObjectType a5M = newMappedTransferObjectTypeBuilder().withName("A5M")
                .withEntityType(a5)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(a5_name))
                .build();

        final MappedTransferObjectType a5M_Insert = newMappedTransferObjectTypeBuilder().withName("A5M_INS")
                .withEntityType(a5)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(a5_name))
                .withRelations(Arrays.asList(
                        newTransferObjectRelationBuilder().withName(ASSOC_A)
                                .withEmbedded(false).withBinding(aOfA5).withTarget(aM)
                                .withCardinality(newCardinalityBuilder().withLower(1).build()).build()
                ))
                .build();


        // A6
        final Attribute a6_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType a6 = newEntityTypeBuilder().withName("A6").withAttributes(a6_name).build();
        final MappedTransferObjectType a6M = newMappedTransferObjectTypeBuilder().withName("A6M")
                .withEntityType(a6)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(a6_name))
                .build();
        final MappedTransferObjectType a6M_Insert = newMappedTransferObjectTypeBuilder().withName("A6M_INS")
                .withEntityType(a6)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(a6_name))
                .build();

        final AssociationEnd _aoo1 = newAssociationEndBuilder().withName(ASSOC_AOO1).withTarget(a).withCardinality(newCardinalityBuilder().build()).build();
        final AssociationEnd _a6oo = newAssociationEndBuilder().withName(ASSOC_A6OO).withTarget(a6).withCardinality(newCardinalityBuilder().withLower(1).build()).withPartner(_aoo1).build();
        _aoo1.setPartner(_a6oo);

        final AssociationEnd _asX1 = newAssociationEndBuilder().withName(ASSOC_ASX1).withTarget(a).withCardinality(newCardinalityBuilder().withUpper(-1).build()).build();
        final AssociationEnd _a6 = newAssociationEndBuilder().withName(ASSOC_A6).withTarget(a6).withCardinality(newCardinalityBuilder().withLower(1).build()).withPartner(_asX1).build();
        _asX1.setPartner(_a6);

        a.getRelations().addAll(Arrays.asList(_a6oo, _a6));
        a6.getRelations().addAll(Arrays.asList(_aoo1, _asX1));

        aM.getRelations().addAll(
                Arrays.asList(
                        newTransferObjectRelationBuilder().withName(ASSOC_A6OO)
                                .withEmbedded(true).withBinding(_a6oo).withTarget(a6M)
                                .withCardinality(newCardinalityBuilder().withLower(1).build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_A6)
                                .withEmbedded(true).withBinding(_a6).withTarget(a6M)
                                .withCardinality(newCardinalityBuilder().withLower(1).build()).build()
                ));

        a6M_Insert.getRelations().addAll(
                Arrays.asList(
                        newTransferObjectRelationBuilder().withName(ASSOC_AOO1)
                                .withEmbedded(false).withBinding(_aoo1).withTarget(aM)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_ASX1)
                                .withEmbedded(false).withBinding(_asX1).withTarget(aM)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build()
                ));


        // A7
        final Attribute a7_name = newAttributeBuilder().withDataType(stringType).withName(NAME).build();
        final EntityType a7 = newEntityTypeBuilder().withName("A7").withAttributes(a7_name).build();
        final MappedTransferObjectType a7M = newMappedTransferObjectTypeBuilder().withName("A7M")
                .withEntityType(a7)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(a7_name))
                .build();

        final MappedTransferObjectType a7M_Insert = newMappedTransferObjectTypeBuilder().withName("A7M_INS")
                .withEntityType(a7)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withBinding(a7_name))
                .build();

        final AssociationEnd _aoo2 = newAssociationEndBuilder().withName(ASSOC_AOO2).withTarget(a).withCardinality(newCardinalityBuilder().withLower(1).build()).build();
        final AssociationEnd _a7oo = newAssociationEndBuilder().withName(ASSOC_A7OO).withTarget(a7).withCardinality(newCardinalityBuilder().build()).withPartner(_aoo2).build();
        _aoo2.setPartner(_a7oo);

        final AssociationEnd _aX1 = newAssociationEndBuilder().withName(ASSOC_AX1).withTarget(a).withCardinality(newCardinalityBuilder().withLower(1).build()).build();
        final AssociationEnd _a7s = newAssociationEndBuilder().withName(ASSOC_A7S).withTarget(a7).withCardinality(newCardinalityBuilder().withUpper(-1).build()).withPartner(_aX1).build();
        _aX1.setPartner(_a7s);


        a.getRelations().addAll(Arrays.asList(_a7oo, _a7s));
        a7.getRelations().addAll(Arrays.asList(_aoo2, _aX1));

        aM.getRelations().addAll(
                Arrays.asList(
                        newTransferObjectRelationBuilder().withName(ASSOC_A7OO)
                                .withEmbedded(true).withBinding(_a7oo).withTarget(a7M)
                                .withCardinality(newCardinalityBuilder().build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_A7S)
                                .withEmbedded(true).withBinding(_a7s).withTarget(a7M)
                                .withCardinality(newCardinalityBuilder().withUpper(-1).build()).build()
                ));


        a7M_Insert.getRelations().addAll(
                Arrays.asList(
                        newTransferObjectRelationBuilder().withName(ASSOC_AOO2)
                                .withEmbedded(false).withBinding(_aoo2).withTarget(aM)
                                .withCardinality(newCardinalityBuilder().withLower(1).build()).build(),
                        newTransferObjectRelationBuilder().withName(ASSOC_AX1)
                                .withEmbedded(false).withBinding(_aX1).withTarget(aM)
                                .withCardinality(newCardinalityBuilder().withLower(1).build()).build()
                ));

        final StaticNavigation allMs = newStaticNavigationBuilder()
                .withName(DUMMY_ALL_MS)
                .withCardinality(newCardinalityBuilder().build())
                .withGetterExpression(newReferenceExpressionTypeBuilder().withExpression(MODEL_NAME + "::M").withDialect(ExpressionDialect.JQL).build())
                .withTarget(m)
                .build();

        final UnmappedTransferObjectType dummyAP = newUnmappedTransferObjectTypeBuilder().withName(DUMMY_ACCESS_POINT_NAME)
                .withRelations(newTransferObjectRelationBuilder()
                        .withName(DUMMY_ALL_MS)
                        .withBinding(allMs)
                        .withTarget(mM)
                        .withCardinality(newCardinalityBuilder().build())
                        .build())
                .build();

        final ActorType actor = newActorTypeBuilder()
                .withName("Actor")
                .withTransferObjectType(dummyAP)
                .build();

        final Model model = newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(Arrays.asList(stringType, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, a1, a2, a3, a4, a5, a6, a7, b1, b2, b3,
                        aM, bM, cM, dM, eM, fM, gM, hM, iM, jM, kM, lM, mM, nM, oM, a1M, a2M, a2M_Insert, a3M, a3M_Insert, a4M, a5M, a5M_Insert, a6M, a6M_Insert, a7M, a7M_Insert, b1M, b2M, b2M_Insert, b3M, b3M_Insert,
                        allMs, dummyAP, actor))
                .build();

        return model;
    }

    public UUID getUuidByName(String name, EClass clazz) {
        final NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(datasourceFixture.getDataSource());
        String tableName;
        if (runtimeFixture.getAsmUtils().isMappedTransferObjectType(clazz)) {
            tableName = runtimeFixture.getRdbmsResolver().rdbmsTable(runtimeFixture.getAsmUtils().getMappedEntityType(clazz).get()).getSqlName();
        } else {
            tableName = runtimeFixture.getRdbmsResolver().rdbmsTable(clazz).getSqlName();
        }

        String sql = "SELECT ID FROM " + tableName + " WHERE " + NAME_COLUMN_NAME + "= :name";
        return UUID.fromString(jdbcTemplate.queryForObject(sql, new MapSqlParameterSource().addValue(NAME, name, Types.VARCHAR), String.class));
    }

    public boolean checkExists(EClass clazz, UUID id) {
        //return testFixture.getDao().getByIdentifier(clazz, id).isPresent();

        final NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(datasourceFixture.getDataSource());
        String tableName;
        if (runtimeFixture.getAsmUtils().isMappedTransferObjectType(clazz)) {
            tableName = runtimeFixture.getRdbmsResolver().rdbmsTable(runtimeFixture.getAsmUtils().getMappedEntityType(clazz).get()).getSqlName();
        } else {
            tableName = runtimeFixture.getRdbmsResolver().rdbmsTable(clazz).getSqlName();
        }

        String sql = "SELECT COUNT(1) FROM " + tableName + " WHERE " + ID_COLUMN_NAME + "= :" + uuid.getName();
        return jdbcTemplate.queryForObject(sql, new MapSqlParameterSource()
                .addValue(uuid.getName(), runtimeFixture.getDataTypeManager().
                                getCoercer().coerce(id, runtimeFixture.getRdbmsParameterMapper().getIdClassName()),
                        runtimeFixture.getRdbmsParameterMapper().getIdSqlType()), Integer.class) == 1;
    }

}
