package hu.blackbelt.judo.runtime.core.dao.rdbms;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceGraph;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceReference;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.*;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class RelationsTest extends AbstractRelationsTest {

    public static final String REFERENCE = "reference";
    public static final String REFERENCED_ELEMENT = "referencedElement";
    public static final String CONTAINMENTS = "containments";
    public static final String REFERENCES = "references";
    public static final String BACK_REFERENCES = "backReferences";
    public static final String KEY = "key";
    public static final String VALUE = "value";


    private UUID create(EClass type, Payload payload) {
        return (UUID) runtimeFixture.getDao().create(type, payload, null).get(runtimeFixture.getIdProvider().getName());
    }

    private void insertPayloads() {
        // Insert associated entities for B 1

        Payload payload = runtimeFixture.getDao().create(aM,
                map(
                        NAME, A_1,
                        REF_ID, A_1,
                        ASSOC_A1, map(NAME, A1_1, REF_ID, A1_1),
                        ASSOC_A1S, ImmutableSet.of(
                                map(NAME, A1_2, REF_ID, A1_2),
                                map(NAME, A1_3, REF_ID, A1_3)
                        ),
                        ASSOC_A3OO, map(NAME, A3_OO, REF_ID, A3_OO),
                        ASSOC_A3, map(NAME, A3_MO_1, REF_ID, A3_MO_1),
                        ASSOC_A3S, ImmutableSet.of(
                                map(NAME, A3_OM_1, REF_ID, A3_OM_1)
                        ),
                        ASSOC_A3MM, ImmutableSet.of(
                                map(NAME, A3_MM_1_1, REF_ID, A3_MM_1_1),
                                map(NAME, A3_MM_1_2, REF_ID, A3_MM_1_2),
                                map(NAME, A3_MM_2_2, REF_ID, A3_MM_2_2)),
                        ASSOC_A4, map(NAME, A4_1, REF_ID, A4_1),
                        ASSOC_A6OO, map(NAME, A6_OO_1, REF_ID, A6_OO_1),
                        ASSOC_A6, map(NAME, A6_MO_1, REF_ID, A6_MO_1),
                        CON_B, map(
                                NAME, B_1,
                                REF_ID, B_1,
                                ASSOC_B1, map(NAME, B1_1, REF_ID, B1_1),
                                ASSOC_B1S, ImmutableSet.of(
                                        map(NAME, B1_2, REF_ID, B1_2),
                                        map(NAME, B1_3, REF_ID, B1_3)),
                                ASSOC_B3S, ImmutableSet.of(
                                        map(NAME, B3_OM_1, REF_ID, B3_OM_1)
                                ),
                                ASSOC_B3MM, ImmutableSet.of(
                                        map(NAME, B3_MM_1_1, REF_ID, B3_MM_1_1),
                                        map(NAME, B3_MM_1_2, REF_ID, B3_MM_1_2),
                                        map(NAME, B3_MM_2_2, REF_ID, B3_MM_2_2)),
                                CON_D, map(
                                        NAME, D_1,
                                        REF_ID, D_1,
                                        CON_H, map(NAME, H_1, REF_ID, H_1),
                                        CON_IS, ImmutableSet.of(
                                                map(NAME, I_1, REF_ID, I_1),
                                                map(NAME, I_2, REF_ID, I_2)
                                        )),
                                CON_ES, ImmutableSet.of(
                                        map(
                                                NAME, E_1,
                                                REF_ID, E_1,
                                                CON_J, map(NAME, J_1, REF_ID, J_1),
                                                CON_KS, ImmutableSet.of(
                                                        map(NAME, K_1, REF_ID, K_1),
                                                        map(NAME, K_2, REF_ID, K_2)
                                                )
                                        ),
                                        map(
                                                NAME, E_2,
                                                REF_ID, E_2,
                                                CON_J, map(NAME, J_2, REF_ID, J_2),
                                                CON_KS, ImmutableSet.of(
                                                        map(NAME, K_3, REF_ID, K_3),
                                                        map(NAME, K_4, REF_ID, K_4)
                                                )
                                        )
                                )),
                        CON_CS, ImmutableSet.of(
                                map(
                                        NAME, C_1,
                                        REF_ID, C_1,
                                        CON_F, map(
                                                NAME, F_1,
                                                REF_ID, F_1,
                                                CON_L, map(NAME, L_1, REF_ID, L_1),
                                                CON_MS, ImmutableSet.of(
                                                        map(NAME, M_1, REF_ID, M_1),
                                                        map(NAME, M_2, REF_ID, M_2)
                                                )
                                        ),
                                        CON_GS, ImmutableSet.of(
                                                map(
                                                        NAME, G_1,
                                                        REF_ID, G_1,
                                                        CON_N, map(NAME, N_1, REF_ID, N_1),
                                                        CON_OS, ImmutableSet.of(
                                                                map(NAME, O_1, REF_ID, O_1),
                                                                map(NAME, O_2, REF_ID, O_2)
                                                        )
                                                ),
                                                map(
                                                        NAME, G_2,
                                                        REF_ID, G_2,
                                                        CON_N, map(NAME, N_2, REF_ID, N_2),
                                                        CON_OS, ImmutableSet.of(
                                                                map(NAME, O_3, REF_ID, O_3),
                                                                map(NAME, O_4, REF_ID, O_4)
                                                        )
                                                )
                                        )
                                ),
                                map(
                                        NAME, C_2,
                                        REF_ID, C_2,
                                        CON_F, map(
                                                NAME, F_2,
                                                REF_ID, F_2,
                                                CON_L, map(NAME, L_2, REF_ID, L_2),
                                                CON_MS, ImmutableSet.of(
                                                        map(NAME, M_3, REF_ID, M_3),
                                                        map(NAME, M_4, REF_ID, M_4)
                                                )
                                        ),
                                        CON_GS, ImmutableSet.of(
                                                map(
                                                        NAME, G_3,
                                                        REF_ID, G_3,
                                                        CON_N, map(NAME, N_3, REF_ID, N_3),
                                                        CON_OS, ImmutableSet.of(
                                                                map(NAME, O_5, REF_ID, O_5),
                                                                map(NAME, O_6, REF_ID, O_6)
                                                        )
                                                ),
                                                map(
                                                        NAME, G_4,
                                                        REF_ID, G_4,
                                                        CON_N, map(NAME, N_4, REF_ID, N_4),
                                                        CON_OS, ImmutableSet.of(
                                                                map(NAME, O_7, REF_ID, O_7),
                                                                map(NAME, O_8, REF_ID, O_8)
                                                        )
                                                )
                                        )
                                )
                        )
                ), null);

        // Check reference ID
        assertThat(A_1, equalTo(payload.get(REF_ID)));
        assertThat(A1_1, equalTo(payload.getAsPayload(ASSOC_A1).get(REF_ID)));

        assertThat(payload.getAsCollectionPayload(ASSOC_A1S),
                containsInAnyOrder(
                    hasEntry(REF_ID, A1_2),
                    hasEntry(REF_ID, A1_3)
                )
        );
        assertThat(A3_OO, equalTo(payload.getAsPayload(ASSOC_A3OO).get(REF_ID)));
        assertThat(A3_MO_1, equalTo(payload.getAsPayload(ASSOC_A3).get(REF_ID)));


        assertThat(payload.getAsCollectionPayload(ASSOC_A3S),
                containsInAnyOrder(
                        hasEntry(REF_ID, A3_OM_1)
                )
        );

        assertThat(payload.getAsCollectionPayload(ASSOC_A3MM),
                containsInAnyOrder(
                        hasEntry(REF_ID, A3_MM_1_1),
                        hasEntry(REF_ID, A3_MM_1_2),
                        hasEntry(REF_ID, A3_MM_2_2)
                )
        );
        assertThat(A4_1, equalTo(payload.getAsPayload(ASSOC_A4).get(REF_ID)));
        assertThat(A6_OO_1, equalTo(payload.getAsPayload(ASSOC_A6OO).get(REF_ID)));
        assertThat(A6_MO_1, equalTo(payload.getAsPayload(ASSOC_A6).get(REF_ID)));

        assertThat(B_1, equalTo(payload.getAsPayload(CON_B).get(REF_ID)));

        assertThat(B1_1, equalTo(payload.getAsPayload(CON_B).getAsPayload(ASSOC_B1).get(REF_ID)));

        assertThat(payload.getAsPayload(CON_B).getAsCollectionPayload(ASSOC_B1S),
                containsInAnyOrder(
                        hasEntry(REF_ID, B1_2),
                        hasEntry(REF_ID, B1_3)
                )
        );
        assertThat(payload.getAsPayload(CON_B).getAsCollectionPayload(ASSOC_B3S),
                containsInAnyOrder(
                        hasEntry(REF_ID, B3_OM_1)
                )
        );
        assertThat(payload.getAsPayload(CON_B).getAsCollectionPayload(ASSOC_B3MM),
                containsInAnyOrder(
                        hasEntry(REF_ID, B3_MM_1_1),
                        hasEntry(REF_ID, B3_MM_1_2),
                        hasEntry(REF_ID, B3_MM_2_2)
                )
        );

        assertThat(D_1, equalTo(payload.getAsPayload(CON_B).getAsPayload(CON_D).get(REF_ID)));
        assertThat(H_1, equalTo(payload.getAsPayload(CON_B).getAsPayload(CON_D).getAsPayload(CON_H).get(REF_ID)));

        assertThat(payload.getAsPayload(CON_B).getAsPayload(CON_D).getAsCollectionPayload(CON_IS),
                containsInAnyOrder(
                        hasEntry(REF_ID, I_1),
                        hasEntry(REF_ID, I_2)
                )
        );

        assertThat(payload.getAsPayload(CON_B).getAsCollectionPayload(CON_ES),
                containsInAnyOrder(
                        hasEntry(REF_ID, E_1),
                        hasEntry(REF_ID, E_2)
                )
        );

        assertThat(J_1, equalTo(payload.getAsPayload(CON_B).getAsCollectionPayload(CON_ES).stream()
                        .filter(p -> E_1.equals(p.getAs(String.class, REF_ID))).findFirst().get()
                .getAsPayload(CON_J).get(REF_ID)));

        assertThat(payload.getAsPayload(CON_B).getAsCollectionPayload(CON_ES).stream()
                        .filter(p -> E_1.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsCollectionPayload(CON_KS),
                containsInAnyOrder(
                        hasEntry(REF_ID, K_1),
                        hasEntry(REF_ID, K_2)
                )
        );

        assertThat(J_2, equalTo(payload.getAsPayload(CON_B).getAsCollectionPayload(CON_ES).stream()
                .filter(p -> E_2.equals(p.getAs(String.class, REF_ID))).findFirst().get()
                .getAsPayload(CON_J).get(REF_ID)));

        assertThat(payload.getAsPayload(CON_B).getAsCollectionPayload(CON_ES).stream()
                        .filter(p -> E_2.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsCollectionPayload(CON_KS),
                containsInAnyOrder(
                        hasEntry(REF_ID, K_3),
                        hasEntry(REF_ID, K_4)
                )
        );

        assertThat(F_1, equalTo(payload.getAsCollectionPayload(CON_CS).stream()
                        .filter(p -> C_1.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsPayload(CON_F).get(REF_ID)));

        assertThat(L_1, equalTo(payload.getAsCollectionPayload(CON_CS).stream()
                .filter(p -> C_1.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsPayload(CON_F).getAsPayload(CON_L).get(REF_ID)));

        assertThat(payload.getAsCollectionPayload(CON_CS).stream()
                .filter(p -> C_1.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsPayload(CON_F).getAsCollectionPayload(CON_MS),
                containsInAnyOrder(
                        hasEntry(REF_ID, M_1),
                        hasEntry(REF_ID, M_2)
                ));

        assertThat(payload.getAsCollectionPayload(CON_CS).stream()
                        .filter(p -> C_1.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsCollectionPayload(CON_GS).stream()
                        .filter(p -> G_1.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsCollectionPayload(CON_OS),
                containsInAnyOrder(
                        hasEntry(REF_ID, O_1),
                        hasEntry(REF_ID, O_2)
                ));

        assertThat(N_1, equalTo(payload.getAsCollectionPayload(CON_CS).stream()
                        .filter(p -> C_1.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsCollectionPayload(CON_GS).stream()
                        .filter(p -> G_1.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsPayload(CON_N).get(REF_ID)));

        assertThat(payload.getAsCollectionPayload(CON_CS).stream()
                        .filter(p -> C_1.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsCollectionPayload(CON_GS).stream()
                        .filter(p -> G_2.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsCollectionPayload(CON_OS),
                containsInAnyOrder(
                        hasEntry(REF_ID, O_3),
                        hasEntry(REF_ID, O_4)
                ));

        assertThat(N_2, equalTo(payload.getAsCollectionPayload(CON_CS).stream()
                .filter(p -> C_1.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsCollectionPayload(CON_GS).stream()
                .filter(p -> G_2.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsPayload(CON_N).get(REF_ID)));

        assertThat(F_2, equalTo(payload.getAsCollectionPayload(CON_CS).stream()
                .filter(p -> C_2.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsPayload(CON_F).get(REF_ID)));

        assertThat(L_2, equalTo(payload.getAsCollectionPayload(CON_CS).stream()
                .filter(p -> C_2.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsPayload(CON_F).getAsPayload(CON_L).get(REF_ID)));

        assertThat(payload.getAsCollectionPayload(CON_CS).stream()
                        .filter(p -> C_2.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsPayload(CON_F).getAsCollectionPayload(CON_MS),
                containsInAnyOrder(
                        hasEntry(REF_ID, M_3),
                        hasEntry(REF_ID, M_4)
                ));

        assertThat(payload.getAsCollectionPayload(CON_CS).stream()
                        .filter(p -> C_2.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsCollectionPayload(CON_GS).stream()
                        .filter(p -> G_3.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsCollectionPayload(CON_OS),
                containsInAnyOrder(
                        hasEntry(REF_ID, O_5),
                        hasEntry(REF_ID, O_6)
                ));

        assertThat(N_3, equalTo(payload.getAsCollectionPayload(CON_CS).stream()
                .filter(p -> C_2.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsCollectionPayload(CON_GS).stream()
                .filter(p -> G_3.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsPayload(CON_N).get(REF_ID)));

        assertThat(payload.getAsCollectionPayload(CON_CS).stream()
                        .filter(p -> C_2.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsCollectionPayload(CON_GS).stream()
                        .filter(p -> G_4.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsCollectionPayload(CON_OS),
                containsInAnyOrder(
                        hasEntry(REF_ID, O_7),
                        hasEntry(REF_ID, O_8)
                ));

        assertThat(N_4, equalTo(payload.getAsCollectionPayload(CON_CS).stream()
                .filter(p -> C_2.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsCollectionPayload(CON_GS).stream()
                .filter(p -> G_4.equals(p.getAs(String.class, REF_ID))).findFirst().get().getAsPayload(CON_N).get(REF_ID)));

        a_1_id = (UUID) payload.get(runtimeFixture.getIdProvider().getName());

        // Associations
        b1_1_id = getUuidByName(B1_1, b1);
        b1_2_id = getUuidByName(B1_2, b1);
        b1_3_id = getUuidByName(B1_3, b1);

        b3_om_1_id = getUuidByName(B3_OM_1, b3);

        b3_mm_1_1_id = getUuidByName(B3_MM_1_1, b3);
        b3_mm_1_2_id = getUuidByName(B3_MM_1_2, b3);
        b3_mm_2_2_id = getUuidByName(B3_MM_2_2, b3);

        // Insert associated entities for A 1
        a1_1_id = getUuidByName(A1_1, a1);
        a1_2_id = getUuidByName(A1_2, a1);
        a1_3_id = getUuidByName(A1_3, a1);

        a3_oo_id = getUuidByName(A3_OO, a3);
        a3_mo_1_id = getUuidByName(A3_MO_1, a3);
        a3_om_1_id = getUuidByName(A3_OM_1, a3);

        a3_mm_1_1_id = getUuidByName(A3_MM_1_1, a3);
        a3_mm_1_2_id = getUuidByName(A3_MM_1_2, a3);
        a3_mm_2_2_id = getUuidByName(A3_MM_2_2, a3);

        a4_1_id = getUuidByName(A4_1, a4);

        a6_oo_1_id = getUuidByName(A6_OO_1, a6);
        a6_mo_1_id = getUuidByName(A6_MO_1, a6);


        // Read B 1's composited entity IDs
        b_1_id = getUuidByName(B_1, b);
        d_1_id = getUuidByName(D_1, d);
        h_1_id = getUuidByName(H_1, h);
        i_1_id = getUuidByName(I_1, i);
        i_2_id = getUuidByName(I_2, i);
        e_1_id = getUuidByName(E_1, e);
        i_2_id = getUuidByName(I_2, i);
        j_1_id = getUuidByName(J_1, j);
        k_1_id = getUuidByName(K_1, k);
        k_2_id = getUuidByName(K_2, k);
        e_2_id = getUuidByName(E_2, e);
        j_2_id = getUuidByName(J_2, j);
        k_3_id = getUuidByName(K_3, k);
        k_4_id = getUuidByName(K_4, k);


        // Read A 1's composited entity IDs
        c_1_id = getUuidByName(C_1, c);
        f_1_id = getUuidByName(F_1, f);
        l_1_id = getUuidByName(L_1, l);
        m_1_id = getUuidByName(M_1, m);
        m_2_id = getUuidByName(M_2, m);
        g_1_id = getUuidByName(G_1, g);
        n_1_id = getUuidByName(N_1, n);
        o_1_id = getUuidByName(O_1, o);
        o_2_id = getUuidByName(O_2, o);
        g_2_id = getUuidByName(G_2, g);
        n_2_id = getUuidByName(N_2, n);
        o_3_id = getUuidByName(O_3, o);
        o_4_id = getUuidByName(O_4, o);
        c_2_id = getUuidByName(C_2, c);
        f_2_id = getUuidByName(F_2, f);
        l_2_id = getUuidByName(L_2, l);
        m_3_id = getUuidByName(M_3, m);
        m_4_id = getUuidByName(M_4, m);
        g_3_id = getUuidByName(G_3, g);
        n_3_id = getUuidByName(N_3, n);
        o_5_id = getUuidByName(O_5, o);
        o_6_id = getUuidByName(O_6, o);
        g_4_id = getUuidByName(G_4, g);
        n_4_id = getUuidByName(N_4, n);
        o_7_id = getUuidByName(O_7, o);
        o_8_id = getUuidByName(O_8, o);

        // Test to insert from the other side of reference
        b3_om_2_id = create(b3M_Insert, map(NAME, B3_OM_2, ASSOC_BX, map(uuid.getName(), b_1_id)));
        a3_om_2_id = create(a3M_Insert, map(NAME, A3_OM_2, ASSOC_AX, map(uuid.getName(), a_1_id)));

        b3_oo_id = create(b3M_Insert, map(NAME, B3_OO, ASSOC_BOO, map(uuid.getName(), b_1_id)));
        b3_mo_1_id = create(b3M_Insert, map(NAME, B3_MO_1, ASSOC_BSX, ImmutableSet.of(map(uuid.getName(), b_1_id))));

        // Insert A 2
        a3_mo_2_id = create(a3M_Insert, map(NAME, A3_MO_2));

        a4_2_id = create(a4M, map(NAME, A4_2));
        a6_oo_2_id = create(a6M_Insert, map(NAME, A6_OO_2));
        a6_mo_2_id = create(a6M_Insert, map(NAME, A6_MO_2));

        a_2_id = create(aM,
                map(
                        NAME, A_2,
                        ASSOC_A3, map(uuid.getName(), a3_mo_2_id),
                        ASSOC_A4, map(uuid.getName(), a4_2_id),
                        ASSOC_A6OO, map(uuid.getName(), a6_oo_2_id),
                        ASSOC_A6, map(uuid.getName(), a6_mo_2_id),
                        ASSOC_A3MM, ImmutableSet.of(
                                map(uuid.getName(), a3_mm_1_1_id),
                                map(uuid.getName(), a3_mm_1_2_id),
                                map(uuid.getName(), a3_mm_2_2_id))
                ));

        // Insert A2 - back reference to A1
        a2_1_id = create(a2M_Insert, map(NAME, A2_1,
                ASSOC_A, map(uuid.getName(), a_1_id)
        ));

        a2_2_id = create(a2M_Insert, map(NAME, A2_2,
                ASSOC_A, map(uuid.getName(), a_1_id),
                ASSOC_AS, ImmutableSet.of(map(uuid.getName(), a_1_id))
        ));

        a2_3_id = create(a2M_Insert, map(NAME, A2_3,
                ASSOC_A, map(uuid.getName(), a_1_id),
                ASSOC_AS, ImmutableSet.of(map(uuid.getName(), a_1_id))
        ));

        a5_1_id = create(a5M_Insert, map(NAME, A5_1,
                ASSOC_A, map(uuid.getName(), a_1_id)));

        a7_oo_1_id = create(a7M_Insert, map(NAME, A7_OO_1,
                ASSOC_AOO2, map(uuid.getName(), a_1_id),
                ASSOC_AX1,  map(uuid.getName(), a_2_id)
        ));

        a7_om_1_id = create(a7M_Insert, map(NAME, A7_OM_1,
                ASSOC_AOO2, map(uuid.getName(), a_2_id),
                ASSOC_AX1,  map(uuid.getName(), a_1_id)
        ));

        // Insert B2
        b3_mo_2_id = create(b3M_Insert, map(NAME, B3_MO_2));

        b_2_id = create(bM, map(NAME, B_2, ASSOC_B3, map(uuid.getName(), b3_mo_2_id)));

        // Add many to many 2 - It contained by A and A3
        a3_mm_2_1_id = create(a3M_Insert, map(NAME, A3_MM_2_1,
                ASSOC_AX, map(uuid.getName(), a_2_id)
        ));
        b2_1_id = create(b2M_Insert, map(NAME, B2_1,
                ASSOC_B, map(uuid.getName(), b_1_id)
        ));
        b2_2_id = create(b2M_Insert, map(NAME, B2_2,
                ASSOC_BS, ImmutableSet.of(map(uuid.getName(), b_1_id))
        ));
        b2_3_id = create(b2M_Insert, map(NAME, B2_3,
                ASSOC_BS, ImmutableSet.of(map(uuid.getName(), b_1_id))
        ));

        b3_mm_2_1_id = create(b3M_Insert, map(NAME, B3_MM_2_1, ASSOC_BX, map(uuid.getName(), b_2_id)));
    }


    public Payload getExpectedResult(Map<UUID, String> updatedNames) {
        TestDataHolder t = new TestDataHolder();

        updatedNames.entrySet().stream().forEach(e -> {
            t.byId(e.getKey()).setName(e.getValue());
        });

        return Payload.asPayload(ImmutableMap.<String, Object>builder()
                .put(uuid.getName(), a_1_id)
                .put(StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(a))
                .put(NAME, t.byId(a_1_id).name)
                .put(ASSOC_A1, t.byId(a1_1_id).toIdNamePayload())
                .put(ASSOC_A1S, ImmutableSet.of(
                        t.byId(a1_2_id).toIdNamePayload(),
                        t.byId(a1_3_id).toIdNamePayload()
                ))
                .put(ASSOC_A3OO, t.byId(a3_oo_id).toIdNamePayload())
                .put(ASSOC_A3, t.byId(a3_mo_1_id).toIdNamePayload())
                .put(ASSOC_A3S, ImmutableSet.of(
                        t.byId(a3_om_1_id).toIdNamePayload(),
                        t.byId(a3_om_2_id).toIdNamePayload()
                ))
                .put(ASSOC_A3MM, ImmutableSet.of(
                        t.byId(a3_mm_1_1_id).toIdNamePayload(),
                        t.byId(a3_mm_1_2_id).toIdNamePayload(),
                        t.byId(a3_mm_2_2_id).toIdNamePayload()
                ))
                .put(ASSOC_A4, t.byId(a4_1_id).toIdNamePayload())
                .put(ASSOC_A6OO, t.byId(a6_oo_1_id).toIdNamePayload())
                .put(ASSOC_A6, t.byId(a6_mo_1_id).toIdNamePayload())
                .put(ASSOC_A7OO, t.byId(a7_oo_1_id).toIdNamePayload())
                .put(ASSOC_A7S, ImmutableSet.of(
                        t.byId(a7_om_1_id).toIdNamePayload()
                ))
                .put(CON_B, ImmutableMap.<String, Object>builder()
                        .put(uuid.getName(), b_1_id)
                        .put(StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(b))
                        .put(NAME, t.byId(b_1_id).name)
                        .put(ASSOC_B1, t.byId(b1_1_id).toIdNamePayload())
                        .put(ASSOC_B1S, ImmutableSet.of(
                                t.byId(b1_2_id).toIdNamePayload(),
                                t.byId(b1_3_id).toIdNamePayload()
                        ))
                        .put(ASSOC_B3S, ImmutableSet.of(
                                t.byId(b3_om_1_id).toIdNamePayload(),
                                t.byId(b3_om_2_id).toIdNamePayload()
                        ))
                        .put(ASSOC_B3MM, ImmutableSet.of(
                                t.byId(b3_mm_1_1_id).toIdNamePayload(),
                                t.byId(b3_mm_1_2_id).toIdNamePayload(),
                                t.byId(b3_mm_2_2_id).toIdNamePayload()
                        ))
                        .put(ASSOC_B3OO, t.byId(b3_oo_id).toIdNamePayload())
                        .put(ASSOC_B3, t.byId(b3_mo_1_id).toIdNamePayload())
                        .put(CON_D, ImmutableMap.of(
                                uuid.getName(), d_1_id, NAME, t.byId(d_1_id).name,
                                StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(d),
                                CON_H, t.byId(h_1_id).toIdNamePayload(),
                                CON_IS, ImmutableSet.of(
                                        t.byId(i_1_id).toIdNamePayload(),
                                        t.byId(i_2_id).toIdNamePayload()
                                )
                        ))
                        .put(CON_ES, ImmutableSet.of(
                                ImmutableMap.of(
                                        uuid.getName(), e_1_id, NAME, t.byId(e_1_id).name,
                                        StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(e),
                                        CON_J, t.byId(j_1_id).toIdNamePayload(),
                                        CON_KS, ImmutableSet.of(
                                                t.byId(k_1_id).toIdNamePayload(),
                                                t.byId(k_2_id).toIdNamePayload()
                                        )
                                ),
                                ImmutableMap.of(
                                        uuid.getName(), e_2_id, NAME, t.byId(e_2_id).name,
                                        StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(e),
                                        CON_J, t.byId(j_2_id).toIdNamePayload(),
                                        CON_KS, ImmutableSet.of(
                                                t.byId(k_3_id).toIdNamePayload(),
                                                t.byId(k_4_id).toIdNamePayload()
                                        )
                                )
                        )).build()
                )
                .put(CON_CS, ImmutableSet.of(
                        ImmutableMap.of(
                                uuid.getName(), c_1_id, NAME, t.byId(c_1_id).name,
                                StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(c),
                                CON_F, ImmutableMap.of(
                                        uuid.getName(), f_1_id, NAME, t.byId(f_1_id).name,
                                        StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(f),
                                        CON_L, t.byId(l_1_id).toIdNamePayload(),
                                        CON_MS, ImmutableSet.of(
                                                t.byId(m_1_id).toIdNamePayload(),
                                                t.byId(m_2_id).toIdNamePayload()
                                        )
                                ),
                                CON_GS, ImmutableSet.of(
                                        ImmutableMap.of(
                                                uuid.getName(), g_1_id, NAME, t.byId(g_1_id).name,
                                                StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(g),
                                                CON_N, t.byId(n_1_id).toIdNamePayload(),
                                                CON_OS, ImmutableSet.of(
                                                        t.byId(o_1_id).toIdNamePayload(),
                                                        t.byId(o_2_id).toIdNamePayload()
                                                )
                                        ),
                                        ImmutableMap.of(
                                                uuid.getName(), g_2_id, NAME, t.byId(g_2_id).name,
                                                StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(g),
                                                CON_N, t.byId(n_2_id).toIdNamePayload(),
                                                CON_OS, ImmutableSet.of(
                                                        t.byId(o_3_id).toIdNamePayload(),
                                                        t.byId(o_4_id).toIdNamePayload()
                                                )
                                        )
                                )
                        ),
                        ImmutableMap.of(
                                uuid.getName(), c_2_id, NAME, t.byId(c_2_id).name,
                                StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(c),
                                CON_F, ImmutableMap.of(
                                        uuid.getName(), f_2_id, NAME, t.byId(f_2_id).name,
                                        StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(f),
                                        CON_L, t.byId(l_2_id).toIdNamePayload(),
                                        CON_MS, ImmutableSet.of(
                                                t.byId(m_3_id).toIdNamePayload(),
                                                t.byId(m_4_id).toIdNamePayload()
                                        )
                                ),
                                CON_GS, ImmutableSet.of(
                                        ImmutableMap.of(
                                                uuid.getName(), g_3_id, NAME, t.byId(g_3_id).name,
                                                StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(g),
                                                CON_N, t.byId(n_3_id).toIdNamePayload(),
                                                CON_OS, ImmutableSet.of(
                                                        t.byId(o_5_id).toIdNamePayload(),
                                                        t.byId(o_6_id).toIdNamePayload()
                                                )
                                        ),
                                        ImmutableMap.of(
                                                uuid.getName(), g_4_id, NAME, t.byId(g_4_id).name,
                                                StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(g),
                                                CON_N, t.byId(n_4_id).toIdNamePayload(),
                                                CON_OS, ImmutableSet.of(
                                                        t.byId(o_7_id).toIdNamePayload(),
                                                        t.byId(o_8_id).toIdNamePayload()
                                                )
                                        )
                                )
                        )
                ))
                .build());
    }

    public DAO<UUID> dao() {
        return runtimeFixture.getDao();
    }

    @BeforeEach
    public void insertDb(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        this.runtimeFixture = runtimeFixture;
        init(runtimeFixture, datasourceFixture);
        this.uuid = runtimeFixture.getIdProvider();
        insertPayloads();
    }

    @AfterEach
    public void purgeDb(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
    }

    @Test
    void testDelete() {
        dao().delete(a5M, a5_1_id);

        dao().delete(a7M, a7_om_1_id);
        dao().delete(a7M, a7_oo_1_id);

        dao().delete(b1M, b1_2_id);
        dao().delete(b2M, b2_2_id);

        dao().delete(a1M, a1_3_id);

        ImmutableSet.of(a_1_id, a_2_id).forEach(id -> dao().delete(aM, id));

        ImmutableSet.of(a1_1_id, a1_2_id, a1_2_id).forEach(id -> dao().delete(a1M, id));
        ImmutableSet.of(a2_1_id, a2_2_id, a2_3_id).forEach(id -> dao().delete(a2M, id));

        ImmutableSet.of(a3_mo_1_id, a3_mo_2_id,
                a3_om_1_id, a3_om_2_id,
                a3_mm_1_1_id, a3_mm_1_2_id,
                a3_mm_2_1_id, a3_mm_2_2_id, a3_oo_id)
                .forEach(id -> dao().delete(a3M, id));

        ImmutableSet.of(a4_1_id, a4_2_id).stream().forEach(id -> dao().delete(a4M, id));
        ImmutableSet.of(a6_mo_1_id, a6_mo_2_id, a6_oo_1_id, a6_oo_2_id).stream().forEach(id -> dao().delete(a6M, id));

        ImmutableSet.of(b1_1_id, b1_3_id).stream().forEach(id -> dao().delete(b1M, id));
        ImmutableSet.of(b2_1_id, b2_3_id).stream().forEach(id -> dao().delete(b2M, id));

        ImmutableSet.of(b3_mo_1_id, b3_mo_2_id,
                b3_om_1_id, b3_om_2_id,
                b3_mm_1_1_id, b3_mm_1_2_id,
                b3_mm_2_1_id, b3_mm_2_2_id,
                b3_oo_id).stream().forEach(id ->
                dao().delete(b3M, id));

        dao().delete(bM, b_2_id);


        assertThat(checkExists(a,  a_1_id), is(false));
        assertThat(checkExists(a,  a_2_id), is(false));
        assertThat(checkExists(b,  b_1_id), is(false));
        assertThat(checkExists(b,  b_2_id), is(false));
        assertThat(checkExists(c,  c_1_id), is(false));
        assertThat(checkExists(c,  c_2_id), is(false));
        assertThat(checkExists(d,  d_1_id), is(false));
        assertThat(checkExists(e,  e_1_id), is(false));
        assertThat(checkExists(f,  e_2_id), is(false));
        assertThat(checkExists(f,  f_1_id), is(false));
        assertThat(checkExists(f,  f_2_id), is(false));
        assertThat(checkExists(g,  g_1_id), is(false));
        assertThat(checkExists(g,  g_2_id), is(false));
        assertThat(checkExists(g,  g_3_id), is(false));
        assertThat(checkExists(g,  g_4_id), is(false));
        assertThat(checkExists(h,  h_1_id), is(false));
        assertThat(checkExists(i,  i_1_id), is(false));
        assertThat(checkExists(i,  i_2_id), is(false));
        assertThat(checkExists(j,  j_1_id), is(false));
        assertThat(checkExists(j,  j_2_id), is(false));
        assertThat(checkExists(k,  k_1_id), is(false));
        assertThat(checkExists(k,  k_2_id), is(false));
        assertThat(checkExists(k,  k_3_id), is(false));
        assertThat(checkExists(k,  k_4_id), is(false));
        assertThat(checkExists(l,  l_1_id), is(false));
        assertThat(checkExists(l,  l_2_id), is(false));
        assertThat(checkExists(m,  m_1_id), is(false));
        assertThat(checkExists(m,  m_2_id), is(false));
        assertThat(checkExists(m,  m_3_id), is(false));
        assertThat(checkExists(m,  m_4_id), is(false));
        assertThat(checkExists(n,  n_1_id), is(false));
        assertThat(checkExists(n,  n_2_id), is(false));
        assertThat(checkExists(n,  n_3_id), is(false));
        assertThat(checkExists(n,  n_4_id), is(false));
        assertThat(checkExists(o,  o_1_id), is(false));
        assertThat(checkExists(o,  o_2_id), is(false));
        assertThat(checkExists(o,  o_3_id), is(false));
        assertThat(checkExists(o,  o_4_id), is(false));
        assertThat(checkExists(o,  o_5_id), is(false));
        assertThat(checkExists(o,  o_6_id), is(false));
        assertThat(checkExists(o,  o_7_id), is(false));
        assertThat(checkExists(o,  o_8_id), is(false));

        assertThat(checkExists(a1,  a1_1_id), is(false));
        assertThat(checkExists(a1,  a1_2_id), is(false));
        assertThat(checkExists(a1,  a1_3_id), is(false));

        assertThat(checkExists(a2,  a2_1_id), is(false));
        assertThat(checkExists(a2,  a2_2_id), is(false));
        assertThat(checkExists(a2,  a2_3_id), is(false));

        assertThat(checkExists(a3,  a3_oo_id), is(false));
        assertThat(checkExists(a3,  a3_mo_1_id), is(false));
        assertThat(checkExists(a3,  a3_mo_2_id), is(false));
        assertThat(checkExists(a3,  a3_om_1_id), is(false));
        assertThat(checkExists(a3,  a3_om_2_id), is(false));
        assertThat(checkExists(a3,  a3_mm_1_1_id), is(false));
        assertThat(checkExists(a3,  a3_mm_1_2_id), is(false));
        assertThat(checkExists(a3,  a3_mm_2_1_id), is(false));
        assertThat(checkExists(a3,  a3_mm_2_2_id), is(false));

        assertThat(checkExists(a4,  a4_1_id), is(false));
        assertThat(checkExists(a4,  a4_2_id), is(false));
        assertThat(checkExists(a5,  a5_1_id), is(false));
        assertThat(checkExists(a6,  a6_oo_1_id), is(false));
        assertThat(checkExists(a6,  a6_oo_2_id), is(false));
        assertThat(checkExists(a6,  a6_mo_1_id), is(false));
        assertThat(checkExists(a6,  a6_mo_2_id), is(false));
        assertThat(checkExists(a7,  a7_oo_1_id), is(false));
        assertThat(checkExists(a7,  a7_om_1_id), is(false));

        assertThat(checkExists(b1,  b1_1_id), is(false));
        assertThat(checkExists(b1,  b1_2_id), is(false));
        assertThat(checkExists(b1,  b1_3_id), is(false));

        assertThat(checkExists(b2,  b2_1_id), is(false));
        assertThat(checkExists(b2,  b2_2_id), is(false));
        assertThat(checkExists(b2,  b2_3_id), is(false));

        assertThat(checkExists(b3,  b3_oo_id), is(false));
        assertThat(checkExists(b3,  b3_mo_1_id), is(false));
        assertThat(checkExists(b3,  b3_mo_2_id), is(false));
        assertThat(checkExists(b3,  b3_om_1_id), is(false));
        assertThat(checkExists(b3,  b3_om_2_id), is(false));
        assertThat(checkExists(b3,  b3_mm_1_1_id), is(false));
        assertThat(checkExists(b3,  b3_mm_1_2_id), is(false));
        assertThat(checkExists(b3,  b3_mm_2_1_id), is(false));
        assertThat(checkExists(b3,  b3_mm_2_2_id), is(false));
    }

    @Test
    void testUpdate() {
        // Update call without any data update
        Payload selectedPayload = dao().getByIdentifier(aM, a_1_id).get();
        Payload result = dao().update(aM, Payload.asPayload(selectedPayload), null);
        Payload expectedPayload = getExpectedResult(ImmutableMap.of());

        log.debug("Expected: " + expectedPayload);
        log.debug("Results:  " + result);

        assertThat(removeMetadata(result), equalTo(expectedPayload));

        Payload updatedPayload;

        // Update root name
        updatedPayload = Payload.asPayload(result);
        expectedPayload = getExpectedResult(ImmutableMap.of());
        
        // Root is updated
        updatedPayload.put(NAME, "a1 Updated");
        expectedPayload.put(NAME, "a1 Updated");

        // Association no change
        updatedPayload.getAsPayload(ASSOC_A3).put(NAME, "a3 Updated");

        // Embedded entity name changed
        updatedPayload.getAsPayload(CON_B).put(NAME, "b1 Updated");
        expectedPayload.getAsPayload(CON_B).put(NAME, "b1 Updated");

        // Remove collection containment entity
        updatedPayload
                .getAsPayload(CON_B)
                .getAsPayload(CON_D)
                .getAsCollectionPayload(CON_IS)
                .removeIf(e -> e.get(uuid.getName()).equals(i_1_id));

        expectedPayload
                .getAsPayload(CON_B)
                .getAsPayload(CON_D)
                .getAsCollectionPayload(CON_IS)
                .removeIf(e -> e.get(uuid.getName()).equals(i_1_id));

        // Remove single containment entity - with empty payload
        updatedPayload
                .getAsPayload(CON_B)
                .getAsCollectionPayload(CON_ES).stream().filter(e -> e.get(uuid.getName()).equals(e_1_id)).findFirst().get()
                .put(CON_J, Payload.asPayload(ImmutableMap.of()));

        expectedPayload
                .getAsPayload(CON_B)
                .getAsCollectionPayload(CON_ES).stream().filter(e -> e.get(uuid.getName()).equals(e_1_id)).findFirst().get()
                .put(CON_J, null);

        // Remove single containment entity and replace with a new one- with empty payload
        updatedPayload
                .getAsPayload(CON_B)
                .getAsPayload(CON_D)
                .put(CON_H, map(NAME, "New H"));

        // Remove collection association entity
        expectedPayload
                .getAsCollectionPayload(ASSOC_A1S)
                .removeIf(e -> e.get(uuid.getName()).equals(a1_2_id));

        updatedPayload
                .getAsCollectionPayload(ASSOC_A1S)
                .removeIf(e -> e.get(uuid.getName()).equals(a1_2_id));

        // Add collection association existing entity
        expectedPayload
                .getAsPayload(CON_B)
                .getAsCollectionPayload(ASSOC_B1S)
                .add(map(uuid.getName(), b1_1_id, StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(b1), NAME, B1_1));

        updatedPayload
                .getAsPayload(CON_B)
                .getAsCollectionPayload(ASSOC_B1S)
                .add(map(uuid.getName(), b1_1_id));

        // Remove old and add single association existing entity
        expectedPayload
                .getAsPayload(CON_B)
                .put(ASSOC_B1, map(uuid.getName(), b1_2_id, StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(b1), NAME, B1_2));

        updatedPayload
                .getAsPayload(CON_B)
                .put(ASSOC_B1, map(uuid.getName(), b1_2_id));

        // Remove old association existing entity
        updatedPayload
                .getAsPayload(CON_B)
                .put(ASSOC_B3, Payload.asPayload(ImmutableMap.of()));

        expectedPayload
                .getAsPayload(CON_B)
                .put(ASSOC_B3, null);

        // A new entity
        updatedPayload
                .getAsPayload(CON_B)
                .getAsPayload(CON_D)
                .getAsCollectionPayload(CON_IS)
                .add(map(NAME, "Added I on IS"));

        // Execute the Update
        result = dao().update(aM, updatedPayload, null);

        // A new entity - implicit insert so have to lookup new
        expectedPayload
                .getAsPayload(CON_B)
                .getAsPayload(CON_D)
                .getAsCollectionPayload(CON_IS)
                .add(map(uuid.getName(), getUuidByName("Added I on IS", i), StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(i), NAME, "Added I on IS"));

        expectedPayload
                .getAsPayload(CON_B)
                .getAsPayload(CON_D)
                .put(CON_H, map(uuid.getName(), getUuidByName("New H", h), StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(h), NAME, "New H"));

        // change reference a1_1.a4 to a4_2
        dao().setReferencesOfNavigationInstanceAt(a2_1_id, aRefMR, a4RefMR, a_1_id, Collections.singleton(a4_2_id));
        expectedPayload
                .put(ASSOC_A4, map(uuid.getName(), a4_2_id, StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(a4), NAME, A4_2));

        result = dao().getByIdentifier(aM, a_1_id).get();
        assertThat(removeMetadata(result), equalTo(expectedPayload));

        // change reference a1_1.a4 back to a4_1
        dao().setReferencesOfReferencedInstancesOf(aRefMR, a4RefMR, a_1_id, Collections.singleton(a4_1_id));
        expectedPayload
                .put(ASSOC_A4, map(uuid.getName(), a4_1_id, StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(a4), NAME, A4_1));
        result = dao().getByIdentifier(aM, a_1_id).get();
        assertThat(removeMetadata(result), equalTo(expectedPayload));

    }

    @Test
    void testReferencesOfReferencedInstanced() {
        Payload expectedPayload = getExpectedResult(ImmutableMap.of());

        // Unset single association
        expectedPayload
                .getAsPayload(CON_B)
                .put(ASSOC_B1, null);

        dao().unsetReferencesOfReferencedInstancesOf(bMR, b1MR, b_1_id);

        // Set single association
        expectedPayload
                .getAsPayload(CON_B)
                .put(ASSOC_B3OO, map(uuid.getName(), b3_mm_1_1_id, StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(b3), NAME, B3_MM_1_1) );

        dao().setReferencesOfReferencedInstancesOf(bMR, b3ooMR, b_1_id, Collections.singleton(b3_mm_1_1_id));

        // Add to collection association
        expectedPayload
                .getAsPayload(CON_B)
                .getAsCollectionPayload(ASSOC_B1S)
                .add(map(uuid.getName(), b1_1_id, StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(b1), NAME, B1_1));

        dao().addAllReferencesOfReferencedInstancesOf(bMR, b1sMR, b_1_id, Collections.singleton(b1_1_id));

        // Remove from collection association
        expectedPayload
                .getAsCollectionPayload(ASSOC_A1S)
                .removeIf(e -> e.get(uuid.getName()).equals(a1_2_id));

        dao().removeAllReferencesOfReferencedInstancesOf(aRefMR, a1sMR, a_1_id, Collections.singleton(a1_2_id));


        // Remove collection containment entity
        expectedPayload
                .getAsPayload(CON_B)
                .getAsPayload(CON_D)
                .getAsCollectionPayload(CON_IS)
                .removeIf(e -> e.get(uuid.getName()).equals(i_1_id));

        dao().removeAllReferencesOfReferencedInstancesOf(dMR, isMR, d_1_id, Collections.singleton(i_1_id));

        Payload result = dao().getByIdentifier(aM, a_1_id).get();

        log.debug("Expected: " + expectedPayload);
        log.debug("Results:  " + result);

        assertEquals(removeMetadata(result), expectedPayload);
    }

    @Test
    void testReferencesOfNavigations() {
        Payload expectedPayload = getExpectedResult(ImmutableMap.of());

        // Unset single association
        expectedPayload
                .getAsPayload(CON_B)
                .put(ASSOC_B1, null);

        dao().unsetReferenceOfNavigationInstanceAt(a_1_id, bMR, b1MR, b_1_id);

        // Set single association
        expectedPayload
                .getAsPayload(CON_B)
                .put(ASSOC_B3OO, map(uuid.getName(), b3_mm_1_1_id, StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(b3), NAME, B3_MM_1_1) );

        dao().setReferencesOfNavigationInstanceAt(a_1_id, bMR, b3ooMR, b_1_id, Collections.singleton(b3_mm_1_1_id));

        // Add to collection association
        expectedPayload
                .getAsPayload(CON_B)
                .getAsCollectionPayload(ASSOC_B1S)
                .add(map(uuid.getName(), b1_1_id, StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(b1), NAME, B1_1));

        dao().addAllReferencesOfNavigationInstanceAt(a_1_id, bMR, b1sMR, b_1_id, Collections.singleton(b1_1_id));

        // Remove from collection association
        expectedPayload
                .getAsCollectionPayload(ASSOC_A1S)
                .removeIf(e -> e.get(uuid.getName()).equals(a1_2_id));

        dao().removeAllReferencesOfNavigationInstanceAt(a2_1_id, aRefMR, a1sMR, a_1_id, Collections.singleton(a1_2_id));

        // Remove collection containment entity
        expectedPayload
                .getAsPayload(CON_B)
                .getAsPayload(CON_D)
                .getAsCollectionPayload(CON_IS)
                .removeIf(e -> e.get(uuid.getName()).equals(i_1_id));

        dao().removeAllReferencesOfNavigationInstanceAt(b_1_id, dMR, isMR, d_1_id, Collections.singleton(i_1_id));

        Payload result = dao().getByIdentifier(aM, a_1_id).get();

        log.debug("Expected: " + expectedPayload);
        log.debug("Results:  " + result);

        assertEquals(removeMetadata(result), expectedPayload);
    }

    @Test
    void testReferences() {
        Payload expectedPayload = getExpectedResult(ImmutableMap.of());

        // Unset single association
        expectedPayload
                .getAsPayload(CON_B)
                .put(ASSOC_B1, null);

        dao().unsetReference(b1MR, b_1_id);

        // Set single association
        expectedPayload
                .getAsPayload(CON_B)
                .put(ASSOC_B3OO, map(uuid.getName(), b3_mm_1_1_id, StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(b3), NAME, B3_MM_1_1) );

        dao().setReference(b3ooMR, b_1_id, ImmutableSet.of(b3_mm_1_1_id));

        // Add to collection association
        expectedPayload
                .getAsPayload(CON_B)
                .getAsCollectionPayload(ASSOC_B1S)
                .add(map(uuid.getName(), b1_1_id, StatementExecutor.ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(b1), NAME, B1_1));

        dao().addReferences(b1sMR, b_1_id, ImmutableSet.of(b1_1_id));

        // Remove from collection association
        expectedPayload
                .getAsCollectionPayload(ASSOC_A1S)
                .removeIf(e -> e.get(uuid.getName()).equals(a1_2_id));

        dao().removeReferences(a1sMR, a_1_id, ImmutableSet.of(a1_2_id));


        // Remove collection containment entity
        expectedPayload
                .getAsPayload(CON_B)
                .getAsPayload(CON_D)
                .getAsCollectionPayload(CON_IS)
                .removeIf(e -> e.get(uuid.getName()).equals(i_1_id));

        dao().removeReferences(isMR, d_1_id, ImmutableSet.of(i_1_id));

        Payload result = dao().getByIdentifier(aM, a_1_id).get();

        log.debug("Expected: " + expectedPayload);
        log.debug("Results:  " + result);

        assertEquals(removeMetadata(result), expectedPayload);
    }

    @Test
    void testQuery() {
        final Payload result = dao().getByIdentifier(aM, a_1_id).get();
        final Payload expectedResult = getExpectedResult(ImmutableMap.of());

        log.debug("Expected: " + expectedResult);
        log.debug("Results:  " + result);
        assertThat(removeMetadata(result), equalTo(expectedResult));

        final EReference csOfA = aM.getEAllReferences().stream().filter(r -> CON_CS.equals(r.getName())).findAny().get();

        // check "a1".cs relation
        final List<Payload> csOfA1 = dao().getNavigationResultAt(a_1_id, csOfA);
        assertThat(csOfA1, hasSize(2));
        assertTrue(csOfA1.stream().anyMatch(c -> C_1.equals(c.get(NAME))));
        assertTrue(csOfA1.stream().anyMatch(c -> C_2.equals(c.get(NAME))));

        final String newNameOfC = "C_changed";
        csOfA1.get(0).put(NAME, newNameOfC);
        // update 1st instance of "a1".cs
        dao().updateNavigationInstanceAt(a_1_id, csOfA, csOfA1.get(0), null);
        // delete 2nd instance of "a1".cs
        dao().deleteNavigationInstanceAt(a_1_id, csOfA, csOfA1.get(1));

        // check result of update and delete calls
        final List<Payload> csOfA1Changed = dao().getNavigationResultAt(a_1_id, csOfA);
        assertThat(csOfA1Changed, hasSize(1));
        assertTrue(csOfA1Changed.stream().anyMatch(c -> newNameOfC.equals(c.get(NAME))));

        // check "Dummy".allMs exposed graph
        final List<Payload> msForDummy = dao().getAllReferencedInstancesOf(allMsInDummy, mM);
        assertThat(msForDummy, hasSize(2));
        assertTrue(msForDummy.stream().anyMatch(c -> M_1.equals(c.get(NAME)) || M_3.equals(c.get(NAME))));
        assertTrue(msForDummy.stream().anyMatch(c -> M_2.equals(c.get(NAME)) || M_4.equals(c.get(NAME))));

        final String newNameOfM = "M_changed";
        msForDummy.get(0).put(NAME, newNameOfM);
        // update 1st instance of "Dummy".allMs
        dao().updateReferencedInstancesOf(mM, allMsInDummy, msForDummy.get(0), null);
        // delete 2nd instance of "Dummy".allMs
        dao().deleteReferencedInstancesOf(mM, allMsInDummy, msForDummy.get(1));

        // check result of update and delete calls
        final List<Payload> msForDummyChanged = dao().getAllReferencedInstancesOf(allMsInDummy, mM);
        assertThat(msForDummyChanged, hasSize(1));
        assertTrue(msForDummyChanged.stream().anyMatch(c -> newNameOfM.equals(c.get(NAME))));

        // create new A1 instance (called A1_new)
        final String nameOfNewA1 = "A1_new";
        final Payload newA1 = Payload.map(NAME, nameOfNewA1);
        dao().createNavigationInstanceAt(a_1_id, a1sMR, newA1, null);

        final List<Payload> a1s = dao().getAllOf(a1M);
        assertThat(a1s, hasSize(4));
        assertTrue(a1s.stream().anyMatch(a1 -> A1_1.equals(a1.get(NAME))));
        assertTrue(a1s.stream().anyMatch(a1 -> A1_2.equals(a1.get(NAME))));
        assertTrue(a1s.stream().anyMatch(a1 -> A1_3.equals(a1.get(NAME))));
        assertTrue(a1s.stream().anyMatch(a1 -> nameOfNewA1.equals(a1.get(NAME))));

        // create new C instance (called C_new)
        final String nameOfNewC = "C_new";
        final Payload newC = Payload.map(NAME, nameOfNewC);
        dao().createNavigationInstanceAt(a_1_id, csOfA, newC, null);

        final List<Payload> csOfA1ChangedAndCreated = dao().getNavigationResultAt(a_1_id, csOfA);
        assertThat(csOfA1ChangedAndCreated, hasSize(2));
        assertTrue(csOfA1ChangedAndCreated.stream().anyMatch(c -> newNameOfC.equals(c.get(NAME))));
        assertTrue(csOfA1ChangedAndCreated.stream().anyMatch(c -> nameOfNewC.equals(c.get(NAME))));
    }

    @Test
    void testInstanceCollector(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        final long startTs = System.currentTimeMillis();
        final InstanceCollector instanceCollector = runtimeFixture.getInstanceCollector();

        final long modelCreated = System.currentTimeMillis();
        log.debug("Instance collector created in {} ms:", (modelCreated - startTs));

        final Map<UUID, InstanceGraph<UUID>> graphs = instanceCollector.collectGraph(a, Collections.singleton(a_1_id));
        final long endTs = System.currentTimeMillis();
        log.debug("Graphs returned in {} ms:\n{}", (endTs - modelCreated), graphs);

        log.debug("References of a1: {}", graphs.get(a_1_id).getReferences());
        log.debug("Back references of a1: {}", graphs.get(a_1_id).getBackReferences());

        log.debug("References of b1: {}", graphs.get(a_1_id).getContainments().stream().filter(containment -> b_1_id.equals(containment.getReferencedElement().getId())).findAny().get().getReferencedElement().getReferences());
        log.debug("Back references of b1: {}", graphs.get(a_1_id).getContainments().stream().filter(containment -> b_1_id.equals(containment.getReferencedElement().getId())).findAny().get().getReferencedElement().getBackReferences());

        final Matcher containmentsOfF1 = createContainmentGraphMatcher(CON_L, CON_MS, l_1_id, m_1_id, m_2_id,
                null, null, null,
                null, null, null,
                null, null, null);
        final Matcher containmentsOfF2 = createContainmentGraphMatcher(CON_L, CON_MS, l_2_id, m_3_id, m_4_id,
                null, null, null,
                null, null, null,
                null, null, null);
        final Matcher containmentsOfG1 = createContainmentGraphMatcher(CON_N, CON_OS, n_1_id, o_1_id, o_2_id,
                null, null, null,
                null, null, null,
                null, null, null);
        final Matcher containmentsOfG2 = createContainmentGraphMatcher(CON_N, CON_OS, n_2_id, o_3_id, o_4_id,
                null, null, null,
                null, null, null,
                null, null, null);
        final Matcher containmentsOfG3 = createContainmentGraphMatcher(CON_N, CON_OS, n_3_id, o_5_id, o_6_id,
                null, null, null,
                null, null, null,
                null, null, null);
        final Matcher containmentsOfG4 = createContainmentGraphMatcher(CON_N, CON_OS, n_4_id, o_7_id, o_8_id,
                null, null, null,
                null, null, null,
                null, null, null);
        final Matcher containmentsOfD1 = createContainmentGraphMatcher(CON_H, CON_IS, h_1_id, i_1_id, i_2_id,
                null, null, null,
                null, null, null,
                null, null, null);
        final Matcher containmentsOfE1 = createContainmentGraphMatcher(CON_J, CON_KS, j_1_id, k_1_id, k_2_id,
                null, null, null,
                null, null, null,
                null, null, null);
        final Matcher containmentsOfE2 = createContainmentGraphMatcher(CON_J, CON_KS, j_2_id, k_3_id, k_4_id,
                null, null, null,
                null, null, null,
                null, null, null);
        final Matcher containmentsOfB1 = createContainmentGraphMatcher(CON_D, CON_ES, d_1_id, e_1_id, e_2_id,
                containmentsOfD1, containmentsOfE1, containmentsOfE2,
                null, null, null,
                null, null, null);
        final Matcher containmentsOfC1 = createContainmentGraphMatcher(CON_F, CON_GS, f_1_id, g_1_id, g_2_id,
                containmentsOfF1, containmentsOfG1, containmentsOfG2,
                null, null, null,
                null, null, null);
        final Matcher containmentsOfC2 = createContainmentGraphMatcher(CON_F, CON_GS, f_2_id, g_3_id, g_4_id,
                containmentsOfF2, containmentsOfG3, containmentsOfG4,
                null, null, null,
                null, null, null);

        final Matcher referencesOfA1 = hasItems(
                hasReferenceWithNameAndId(ASSOC_A1, a1_1_id),
                hasReferenceWithNameAndId(ASSOC_A3OO, a3_oo_id),
                hasReferenceWithNameAndId(ASSOC_A3, a3_mo_1_id),
                hasReferenceWithNameAndId(ASSOC_A3S, a3_om_1_id),
                hasReferenceWithNameAndId(ASSOC_A3S, a3_om_2_id),
                hasReferenceWithNameAndId(ASSOC_A1S, a1_2_id),
                hasReferenceWithNameAndId(ASSOC_A1S, a1_3_id),
                hasReferenceWithNameAndId(ASSOC_A3MM, a3_mm_1_1_id),
                hasReferenceWithNameAndId(ASSOC_A3MM, a3_mm_1_2_id),
                hasReferenceWithNameAndId(ASSOC_A3MM, a3_mm_2_2_id),
                hasReferenceWithNameAndId(ASSOC_A4, a4_1_id),
                hasReferenceWithNameAndId(ASSOC_A6OO, a6_oo_1_id),
                hasReferenceWithNameAndId(ASSOC_A6, a6_mo_1_id),
                hasReferenceWithNameAndId(ASSOC_A7OO, a7_oo_1_id),
                hasReferenceWithNameAndId(ASSOC_A7S, a7_om_1_id)
        );

        final Matcher referencesOfB1 = hasItems(
                hasReferenceWithNameAndId(ASSOC_B1, b1_1_id),
                hasReferenceWithNameAndId(ASSOC_B3OO, b3_oo_id),
                hasReferenceWithNameAndId(ASSOC_B3, b3_mo_1_id),
                hasReferenceWithNameAndId(ASSOC_B3S, b3_om_1_id),
                hasReferenceWithNameAndId(ASSOC_B3S, b3_om_2_id),
                hasReferenceWithNameAndId(ASSOC_B1S, b1_2_id),
                hasReferenceWithNameAndId(ASSOC_B1S, b1_3_id),
                hasReferenceWithNameAndId(ASSOC_B3MM, b3_mm_1_1_id),
                hasReferenceWithNameAndId(ASSOC_B3MM, b3_mm_1_2_id),
                hasReferenceWithNameAndId(ASSOC_B3MM, b3_mm_2_2_id)
        );

        final Matcher backReferencesOfA1 = hasItems(
                hasReferenceWithNameAndId(ASSOC_A, a2_1_id),
                hasReferenceWithNameAndId(ASSOC_A, a5_1_id),
                hasReferenceWithNameAndId(ASSOC_AOO, a3_oo_id),
                hasReferenceWithNameAndId(ASSOC_ASX, a3_mo_1_id),
                hasReferenceWithNameAndId(ASSOC_AX, a3_om_1_id),
                hasReferenceWithNameAndId(ASSOC_AX, a3_om_2_id),
                hasReferenceWithNameAndId(ASSOC_AS, a2_2_id),
                hasReferenceWithNameAndId(ASSOC_AS, a2_3_id),
                hasReferenceWithNameAndId(ASSOC_AMM, a3_mm_1_1_id),
                hasReferenceWithNameAndId(ASSOC_AMM, a3_mm_1_2_id),
                hasReferenceWithNameAndId(ASSOC_AMM, a3_mm_2_2_id),
                hasReferenceWithNameAndId(ASSOC_AOO1, a6_oo_1_id),
                hasReferenceWithNameAndId(ASSOC_ASX1, a6_mo_1_id),
                hasReferenceWithNameAndId(ASSOC_AOO2, a7_oo_1_id),
                hasReferenceWithNameAndId(ASSOC_AX1, a7_om_1_id)
        );

        final Matcher backReferencesOfB1 = hasItems(
                hasReferenceWithNameAndId(ASSOC_B, b2_1_id),
                hasReferenceWithNameAndId(ASSOC_BOO, b3_oo_id),
                hasReferenceWithNameAndId(ASSOC_BSX, b3_mo_1_id),
                hasReferenceWithNameAndId(ASSOC_BX, b3_om_1_id),
                hasReferenceWithNameAndId(ASSOC_BX, b3_om_2_id),
                hasReferenceWithNameAndId(ASSOC_BS, b2_2_id),
                hasReferenceWithNameAndId(ASSOC_BS, b2_3_id),
                hasReferenceWithNameAndId(ASSOC_BMM, b3_mm_1_1_id),
                hasReferenceWithNameAndId(ASSOC_BMM, b3_mm_1_2_id),
                hasReferenceWithNameAndId(ASSOC_BMM, b3_mm_2_2_id)
        );


        final Matcher containmentsOfA1 = createContainmentGraphMatcher(ASSOC_B, CON_CS, b_1_id, c_1_id, c_2_id,
                containmentsOfB1, containmentsOfC1, containmentsOfC2,
                referencesOfB1, null, null,
                backReferencesOfB1, null, null);

        assertThat(graphs.entrySet(),
                hasItems(
                        allOf(
                                hasProperty(KEY, equalTo(a_1_id)),
                                hasProperty(VALUE,
                                        allOf(
                                                instanceOf(InstanceGraph.class),
                                                hasProperty(ID, equalTo(a_1_id)),
                                                hasProperty(CONTAINMENTS, containmentsOfA1),
                                                hasProperty(REFERENCES, referencesOfA1),
                                                hasProperty(BACK_REFERENCES, backReferencesOfA1)
                                        )
                                )
                        )
                )
        );
    }

    private static Matcher hasReferenceWithNameAndId(String name, UUID id) {
        return allOf(
                instanceOf(InstanceReference.class),
                hasProperty(REFERENCE, hasProperty(NAME, equalTo(name))),
                hasProperty(REFERENCED_ELEMENT,
                        allOf(
                                instanceOf(InstanceGraph.class),
                                hasProperty(ID, equalTo(id))
                        )
                )
        );
    }

    private static Matcher createContainmentGraphMatcher(final String singleReferenceName,
                                                         final String multipleReferenceName,
                                                         final UUID singleId,
                                                         final UUID multiple1Id,
                                                         final UUID multiple2Id,
                                                         final Matcher singleContainmentsMatcher,
                                                         final Matcher multiple1ContainmentsMatcher,
                                                         final Matcher multiple2ContainmentsMatcher,
                                                         final Matcher singleReferencesMatcher,
                                                         final Matcher multiple1ReferencesMatcher,
                                                         final Matcher multiple2ReferencesMatcher,
                                                         final Matcher singleBackReferencesMatcher,
                                                         final Matcher multiple1BackReferencesMatcher,
                                                         final Matcher multiple2BackReferencesMatcher) {

        final Collection<Matcher> singleMatchers = new ArrayList<>();
        singleMatchers.add(instanceOf(InstanceGraph.class));
        if (singleId != null) {
            singleMatchers.add(hasProperty(ID, equalTo(singleId)));
        }
        if (singleContainmentsMatcher != null) {
            singleMatchers.add((hasProperty(CONTAINMENTS, singleContainmentsMatcher)));
        }
        if (singleReferencesMatcher != null) {
            singleMatchers.add((hasProperty(REFERENCES, singleReferencesMatcher)));
        }
        if (singleBackReferencesMatcher != null) {
            singleMatchers.add((hasProperty(BACK_REFERENCES, singleBackReferencesMatcher)));
        }

        final Collection<Matcher> multiple1Matchers = new ArrayList<>();
        multiple1Matchers.add(instanceOf(InstanceGraph.class));
        if (multiple1Id != null) {
            multiple1Matchers.add(hasProperty(ID, equalTo(multiple1Id)));
        }
        if (multiple1ContainmentsMatcher != null) {
            multiple1Matchers.add((hasProperty(CONTAINMENTS, multiple1ContainmentsMatcher)));
        }
        if (multiple1ReferencesMatcher != null) {
            multiple1Matchers.add((hasProperty(REFERENCES, multiple1ReferencesMatcher)));
        }
        if (multiple1BackReferencesMatcher != null) {
            multiple1Matchers.add((hasProperty(BACK_REFERENCES, multiple1BackReferencesMatcher)));
        }

        final Collection<Matcher> multiple2Matchers = new ArrayList<>();
        multiple2Matchers.add(instanceOf(InstanceGraph.class));
        if (multiple2Id != null) {
            multiple2Matchers.add(hasProperty(ID, equalTo(multiple2Id)));
        }
        if (multiple2ContainmentsMatcher != null) {
            multiple2Matchers.add((hasProperty(CONTAINMENTS, multiple2ContainmentsMatcher)));
        }
        if (multiple2ReferencesMatcher != null) {
            multiple2Matchers.add((hasProperty(REFERENCES, multiple2ReferencesMatcher)));
        }
        if (multiple2BackReferencesMatcher != null) {
            multiple2Matchers.add((hasProperty(BACK_REFERENCES, multiple2BackReferencesMatcher)));
        }

        return hasItems(
                allOf(
                        instanceOf(InstanceReference.class),
                        hasProperty(REFERENCE, hasProperty(NAME, equalTo(singleReferenceName))),
                        hasProperty(REFERENCED_ELEMENT, allOf((Iterable) singleMatchers))
                ),
                allOf(
                        instanceOf(InstanceReference.class),
                        hasProperty(REFERENCE, hasProperty(NAME, equalTo(multipleReferenceName))),
                        hasProperty(REFERENCED_ELEMENT, allOf((Iterable) multiple1Matchers))
                ),
                allOf(
                        instanceOf(InstanceReference.class),
                        hasProperty(REFERENCE, hasProperty(NAME, equalTo(multipleReferenceName))),
                        hasProperty(REFERENCED_ELEMENT, allOf((Iterable) multiple2Matchers))
                )
        );
    }

    private Payload removeMetadata(final Payload payload) {
        final Payload result = Payload.asPayload(payload);

        result.remove(StatementExecutor.ENTITY_VERSION_MAP_KEY);
        result.remove(StatementExecutor.ENTITY_CREATE_USERNAME_MAP_KEY);
        result.remove(StatementExecutor.ENTITY_CREATE_USER_ID_MAP_KEY);
        result.remove(StatementExecutor.ENTITY_CREATE_TIMESTAMP_MAP_KEY);
        result.remove(StatementExecutor.ENTITY_UPDATE_USERNAME_MAP_KEY);
        result.remove(StatementExecutor.ENTITY_UPDATE_USER_ID_MAP_KEY);
        result.remove(StatementExecutor.ENTITY_UPDATE_TIMESTAMP_MAP_KEY);

        result.entrySet().forEach(e -> {
            if (e.getValue() instanceof Payload) {
                result.put(e.getKey(), removeMetadata((Payload) e.getValue()));
            } else if (e.getValue() instanceof Set) {
                result.put(e.getKey(), ((Set<Payload>) e.getValue()).stream().map(i -> removeMetadata(i)).collect(Collectors.toSet()));
            } else if (e.getValue() instanceof List) {
                result.put(e.getKey(), ((List<Payload>) e.getValue()).stream().map(i -> removeMetadata(i)).collect(Collectors.toList()));
            }
        });

        return result;
    }
}
