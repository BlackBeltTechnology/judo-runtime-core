package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.accesspoint.ActorType;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.meta.esm.structure.TransferObjectType;
import hu.blackbelt.judo.meta.esm.type.BooleanType;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
import hu.blackbelt.judo.runtime.core.query.CustomJoinDefinition;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import java.util.*;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.esm.accesspoint.util.builder.AccesspointBuilders.newActorTypeBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class TenderTest {

    public static final String MODEL_NAME = "M";

    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    private final boolean TEST_AGGREGATIONS = true;

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getEsmModel() {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();

        final Model model = NamespaceBuilders.newModelBuilder().withName(getModelName()).build();

        final EntityType company = newEntityTypeBuilder()
                .withName("Company")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        company.setMapping(newMappingBuilder().withTarget(company).build());

        final EntityType participant = newEntityTypeBuilder()
                .withName("Participant")
                .withAttributes(newDataMemberBuilder()
                        .withName("score")
                        .withDataType(integerType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("company")
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withTarget(company)
                        .withLower(1).withUpper(1)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        participant.setMapping(newMappingBuilder().withTarget(participant).build());

        final EntityType tender = newEntityTypeBuilder()
                .withName("Tender")
                .withAttributes(newDataMemberBuilder()
                        .withName("subject")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("participants")
                        .withRelationKind(RelationKind.COMPOSITION)
                        .withTarget(participant)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("winner")
                        .withRelationKind(TEST_AGGREGATIONS ? RelationKind.AGGREGATION : RelationKind.ASSOCIATION)
                        .withTarget(company)
                        .withLower(0).withUpper(1)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.participants!head(p | p.score DESC).company")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("winners")
                        .withRelationKind(TEST_AGGREGATIONS ? RelationKind.AGGREGATION : RelationKind.ASSOCIATION)
                        .withTarget(company)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.participants!heads(p | p.score DESC).company")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("last")
                        .withRelationKind(TEST_AGGREGATIONS ? RelationKind.AGGREGATION : RelationKind.ASSOCIATION)
                        .withTarget(company)
                        .withLower(0).withUpper(1)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.participants!tail(p | p.score DESC).company")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("lasts")
                        .withRelationKind(TEST_AGGREGATIONS ? RelationKind.AGGREGATION : RelationKind.ASSOCIATION)
                        .withTarget(company)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.participants!tails(p | p.score DESC).company")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("customWinner")
                        .withRelationKind(TEST_AGGREGATIONS ? RelationKind.AGGREGATION : RelationKind.ASSOCIATION)
                        .withTarget(company)
                        .withLower(0).withUpper(1)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.participants!head(p | p.score DESC).company")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("customWinners")
                        .withRelationKind(TEST_AGGREGATIONS ? RelationKind.AGGREGATION : RelationKind.ASSOCIATION)
                        .withTarget(company)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.participants!heads(p | p.score DESC).company")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("customLast")
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withTarget(company)
                        .withLower(0).withUpper(1)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.participants!tail(p | p.score DESC).company")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("customLasts")
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withTarget(company)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.participants!tails(p | p.score DESC).company")
                        .build())
                .build();
        tender.setMapping(newMappingBuilder().withTarget(tender).build());

        TransferObjectType ap = newTransferObjectTypeBuilder()
                .withName("AP")
                .withAttributes(newDataMemberBuilder()
                        .withName("t1t3Exist")
                        .withDataType(booleanType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(getModelName() + "::Tender!exists(t | t.subject == 'T1' or t.subject == 'T3')")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("topWinner")
                        .withTarget(company)
                        .withLower(0).withUpper(1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Tender.participants!head(p | p.score DESC).company")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("topWinners")
                        .withTarget(company)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Tender.participants!heads(p | p.score DESC).company")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("tenderWinnerList")
                        .withTarget(company)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Tender.winner")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("tenderWinnerListCount")
                        .withDataType(integerType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(getModelName() + "::Tender.participants!filter(p | " + getModelName() + "::Tender!exists(t | t.participants!filter(q | q.score == t.participants!max(r | r.score))!any() == p)).company!count()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("allWinners")
                        .withTarget(company)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Tender.winners")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("lastCompanies")
                        .withTarget(company)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Tender.lasts")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("t1t3Winners")
                        .withTarget(company)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Tender!filter(t | t.subject == 'T1' or t.subject == 'T3').winners")
                        .build())
                .build();
        ActorType actor = newActorTypeBuilder().withName("actor").withPrincipal(ap).build();
        useTransferObjectType(ap).withActorType(actor).build();

        model.getElements().addAll(Arrays.asList(
                stringType, integerType, booleanType,
                company, participant, tender, ap, actor
        ));
        return model;
    }

    JudoRuntimeFixture runtimeFixture;

    @BeforeEach
    public void initFixture(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        this.runtimeFixture = runtimeFixture;
        this.runtimeFixture.init(getEsmModel(), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");
    }

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.dropDatabase();
    }

    private UUID createCompany(final String name) {
        final EClass companyType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Company").get();

        final Payload company = runtimeFixture.getDao().create(companyType, map(
                "name", name
                ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build()
        );

        final UUID companyId = company.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());

        log.debug("Company {} created with ID: {}", name, companyId);

        return companyId;
    }

    private UUID createTender(final String subject) {
        final EClass tenderType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Tender").get();

        final Payload tender = runtimeFixture.getDao().create(tenderType, map(
                "subject", subject
                ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build()
        );

        final UUID tenderId = tender.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());

        log.debug("Tender {} created with ID: {}", subject, tenderId);

        return tenderId;
    }

    private UUID createParticipant(final UUID tender, final UUID company, final int score) {
        final EClass tenderType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Tender").get();
        final EReference participantsOfTender = tenderType.getEAllReferences().stream().filter(r -> "participants".equals(r.getName())).findAny().get();

        final Payload participant = runtimeFixture.getDao().createNavigationInstanceAt(tender, participantsOfTender, map(
                "company", map(runtimeFixture.getIdProvider().getName(), company),
                "score", score
                ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build()
        );

        final UUID participantId = participant.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());

        log.debug("Participant {} of tender {} with score {} created with ID: {}", new Object[]{company, tender, score, participantId});

        return participantId;
    }

    public enum Tender {
        T1, T2, T3, T4
    }

    public enum Company {
        COMPANY1, COMPANY2, COMPANY3, COMPANY4, COMPANY5, COMPANY6, COMPANY7, COMPANY8, COMPANY9, COMPANY10
    }

    @Test
    public void testWindowing() {
        final EClass tenderType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Tender").get();
        final EClass companyType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Company").get();
        final EClass apType = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".AP").get();

        final EReference winnerReference = tenderType.getEAllReferences().stream().filter(r -> "winner".equals(r.getName())).findAny().get();
        final EReference winnersReference = tenderType.getEAllReferences().stream().filter(r -> "winners".equals(r.getName())).findAny().get();
        final EReference customWinnerReference = tenderType.getEAllReferences().stream().filter(r -> "customWinner".equals(r.getName())).findAny().get();
        final EReference customWinnersReference = tenderType.getEAllReferences().stream().filter(r -> "customWinners".equals(r.getName())).findAny().get();
        final EReference topWinnerReference = apType.getEAllReferences().stream().filter(r -> "topWinner".equals(r.getName())).findAny().get();
        final EReference topWinnersReference = apType.getEAllReferences().stream().filter(r -> "topWinners".equals(r.getName())).findAny().get();
        final EReference allWinnersReference = apType.getEAllReferences().stream().filter(r -> "allWinners".equals(r.getName())).findAny().get();
        final EReference lastCompaniesReference = apType.getEAllReferences().stream().filter(r -> "lastCompanies".equals(r.getName())).findAny().get();
        final EReference tenderWinnerListReference = apType.getEAllReferences().stream().filter(r -> "tenderWinnerList".equals(r.getName())).findAny().get();
        final EReference t1t3WinnersReference = apType.getEAllReferences().stream().filter(r -> "t1t3Winners".equals(r.getName())).findAny().get();
        final EAttribute t1t3ExistsAttribute = apType.getEAllAttributes().stream().filter(r -> "t1t3Exist".equals(r.getName())).findAny().get();
        final EAttribute tenderWinnerListCountAttribute = apType.getEAllAttributes().stream().filter(r -> "tenderWinnerListCount".equals(r.getName())).findAny().get();

        runtimeFixture.addCustomJoinDefinition(customWinnersReference, CustomJoinDefinition.builder()
                .sourceIdParameterName("source_id")
                .sourceIdSetParameterName("source_id_set")
                .navigationSql("SELECT DISTINCT p.`M.Participant#company` AS ID, p.`M.Tender#participants` AS source_id\n" +
                        "FROM `M.Participant` p\n" +
                        "JOIN (SELECT p2.`M.Tender#participants` AS tender_id, MAX(p2.`M.Participant#score`) max_score\n" +
                        "      FROM `M.Participant` p2\n" +
                        "      GROUP BY p2.`M.Tender#participants`) AS max_scores ON (p.`M.Participant#score` = max_scores.max_score AND max_scores.tender_id = p.`M.Tender#participants`)")
                .build());
        runtimeFixture.addCustomJoinDefinition(customWinnerReference, CustomJoinDefinition.builder()
                .sourceIdParameterName("source_id")
                .sourceIdSetParameterName("source_id_set")
                .navigationSql("SELECT p0.`M.Participant#company` AS ID, p0.`M.Tender#participants` AS source_id\n" +
                        "FROM `M.Participant` p0\n" +
                        "JOIN (SELECT DISTINCT MIN(p.ID) AS ID, p.`M.Tender#participants` AS source_id\n" +
                        "      FROM `M.Participant` p\n" +
                        "      JOIN (SELECT p2.`M.Tender#participants` AS tender_id, MAX(p2.`M.Participant#score`) max_score\n" +
                        "            FROM `M.Participant` p2\n" +
                        "            GROUP BY p2.`M.Tender#participants`) AS max_scores ON (p.`M.Participant#score` = max_scores.max_score AND max_scores.tender_id = p.`M.Tender#participants`)\n" +
                        "      GROUP BY p.`M.Tender#participants`) AS winner ON (winner.ID = p0.ID)")
                .build());

        final Map<Tender, UUID> tenders = new HashMap<>();
        final Map<Company, UUID> companies = new HashMap<>();

        for (Tender t : Tender.values()) {
            tenders.put(t, createTender(t.name()));
        }

        for (Company c : Company.values()) {
            companies.put(c, createCompany(c.name()));
        }

        createParticipant(tenders.get(Tender.T1), companies.get(Company.COMPANY1), 3);
        createParticipant(tenders.get(Tender.T1), companies.get(Company.COMPANY2), 7);
        createParticipant(tenders.get(Tender.T1), companies.get(Company.COMPANY3), 2);
        createParticipant(tenders.get(Tender.T1), companies.get(Company.COMPANY4), 5);
        createParticipant(tenders.get(Tender.T2), companies.get(Company.COMPANY1), 5);
        createParticipant(tenders.get(Tender.T2), companies.get(Company.COMPANY3), 8);
        createParticipant(tenders.get(Tender.T2), companies.get(Company.COMPANY5), 3);
        createParticipant(tenders.get(Tender.T2), companies.get(Company.COMPANY6), 1);
        createParticipant(tenders.get(Tender.T2), companies.get(Company.COMPANY7), 0);
        createParticipant(tenders.get(Tender.T2), companies.get(Company.COMPANY8), 4);
        createParticipant(tenders.get(Tender.T3), companies.get(Company.COMPANY3), 4);
        createParticipant(tenders.get(Tender.T3), companies.get(Company.COMPANY6), 2);
        createParticipant(tenders.get(Tender.T3), companies.get(Company.COMPANY7), 4);

        log.debug("Checking query results...");

        final Optional<Payload> tender1 = runtimeFixture.getDao().getByIdentifier(tenderType, tenders.get(Tender.T1));
        assertTrue(tender1.isPresent());

        log.debug("Tender1: {}", tender1.get());

        final long start1 = System.currentTimeMillis();
        final Optional<Payload> winnerOfTender1 = runtimeFixture.getDao().getNavigationResultAt(tender1.get().getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()), winnerReference).stream().findAny();
        final long end1 = System.currentTimeMillis();
        final Optional<UUID> winnerIdOfTender1 = winnerOfTender1.map(winner -> winner.getAs(UUID.class, runtimeFixture.getIdProvider().getName()));
        assertThat(winnerOfTender1.isPresent(), equalTo(Boolean.TRUE));
        assertThat(winnerIdOfTender1.get(), equalTo(companies.get(Company.COMPANY2)));

        final long start2 = System.currentTimeMillis();
        final Optional<Payload> customWinnerOfTender1 = runtimeFixture.getDao().getNavigationResultAt(tender1.get().getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()), customWinnerReference).stream().findAny();
        final long end2 = System.currentTimeMillis();
        final Optional<UUID> customWinnerIdOfTender1 = customWinnerOfTender1.map(winner -> winner.getAs(UUID.class, runtimeFixture.getIdProvider().getName()));
        assertThat(customWinnerOfTender1.isPresent(), equalTo(Boolean.TRUE));
        assertThat(customWinnerIdOfTender1.get(), equalTo(companies.get(Company.COMPANY2)));

        if (TEST_AGGREGATIONS) {
            final Set<UUID> winnersIdsOfTender1 = tender1.filter(t -> t.get("winners") != null)
                    .map(t -> t.getAsCollectionPayload("winners").stream()
                            .map(w -> w.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()))
                            .collect(Collectors.toSet()))
                    .orElse(Collections.emptySet());
            assertThat(winnersIdsOfTender1, equalTo(ImmutableSet.of(
                    companies.get(Company.COMPANY2)
            )));
            final Optional<UUID> lastIdOfTender1 = tender1
                    .filter(t -> t.get("last") != null)
                    .map(t -> t.getAsPayload("last").getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()));
            assertThat(lastIdOfTender1.isPresent(), equalTo(Boolean.TRUE));
            assertThat(lastIdOfTender1.get(), equalTo(companies.get(Company.COMPANY3)));
            final Set<UUID> lastsIdsOfTender1 = tender1.filter(t -> t.get("lasts") != null)
                    .map(t -> t.getAsCollectionPayload("lasts").stream()
                            .map(l -> l.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()))
                            .collect(Collectors.toSet()))
                    .orElse(Collections.emptySet());
            assertThat(lastsIdsOfTender1, equalTo(ImmutableSet.of(
                    companies.get(Company.COMPANY3)
            )));
        }

        final Optional<Payload> tender2 = runtimeFixture.getDao().getByIdentifier(tenderType, tenders.get(Tender.T2));
        assertTrue(tender2.isPresent());

        log.debug("Tender2: {}", tender2.get());

        final Optional<Payload> winnerOfTender2 = runtimeFixture.getDao().getNavigationResultAt(tender2.get().getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()), winnerReference).stream().findAny();
        final Optional<UUID> winnerIdOfTender2 = winnerOfTender2.map(winner -> winner.getAs(UUID.class, runtimeFixture.getIdProvider().getName()));
        assertThat(winnerOfTender2.isPresent(), equalTo(Boolean.TRUE));
        assertThat(winnerIdOfTender2.get(), equalTo(companies.get(Company.COMPANY3)));

        final Optional<Payload> tender3 = runtimeFixture.getDao().getByIdentifier(tenderType, tenders.get(Tender.T3));
        assertTrue(tender3.isPresent());

        log.debug("Tender3: {}", tender3.get());

        final long start3 = System.currentTimeMillis();
        final Optional<Payload> winnerOfTender3 = runtimeFixture.getDao().getNavigationResultAt(tender3.get().getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()), winnerReference).stream().findAny();
        final long end3 = System.currentTimeMillis();
        final Optional<UUID> winnerIdOfTender3 = winnerOfTender3.map(winner -> winner.getAs(UUID.class, runtimeFixture.getIdProvider().getName()));
        assertThat(winnerOfTender3.isPresent(), equalTo(Boolean.TRUE));
        assertThat(winnerIdOfTender3.get(), anyOf(
                equalTo(companies.get(Company.COMPANY3)),
                equalTo(companies.get(Company.COMPANY7))
        ));

        final long start4 = System.currentTimeMillis();
        final Optional<Payload> customWinnerOfTender3 = runtimeFixture.getDao().getNavigationResultAt(tender3.get().getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()), customWinnerReference).stream().findAny();
        final long end4 = System.currentTimeMillis();
        final Optional<UUID> customWinnerIdOfTender3 = customWinnerOfTender3.map(winner -> winner.getAs(UUID.class, runtimeFixture.getIdProvider().getName()));
        assertThat(customWinnerOfTender3.isPresent(), equalTo(Boolean.TRUE));
        assertThat(customWinnerIdOfTender3.get(), anyOf(
                equalTo(companies.get(Company.COMPANY3)),
                equalTo(companies.get(Company.COMPANY7))
        ));

        final long start5 = System.currentTimeMillis();
        final List<Payload> winnersOfTender3 = runtimeFixture.getDao().getNavigationResultAt(tender3.get().getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()), winnersReference);
        final long end5 = System.currentTimeMillis();
        final Set<UUID> winnerIdsOfTender3 = winnersOfTender3.stream().map(winner -> winner.getAs(UUID.class, runtimeFixture.getIdProvider().getName())).collect(Collectors.toSet());
        assertThat(winnerIdsOfTender3, equalTo(ImmutableSet.of(
                companies.get(Company.COMPANY3),
                companies.get(Company.COMPANY7)
        )));

        final long start6 = System.currentTimeMillis();
        final List<Payload> customWinnersOfTender3 = runtimeFixture.getDao().getNavigationResultAt(tender3.get().getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()), customWinnersReference);
        final long end6 = System.currentTimeMillis();
        final Set<UUID> customWinnerIdsOfTender3 = customWinnersOfTender3.stream().map(winner -> winner.getAs(UUID.class, runtimeFixture.getIdProvider().getName())).collect(Collectors.toSet());
        assertThat(customWinnerIdsOfTender3, equalTo(ImmutableSet.of(
                companies.get(Company.COMPANY3),
                companies.get(Company.COMPANY7)
        )));

        if (TEST_AGGREGATIONS) {
            final Set<UUID> winnersIdsOfTender3 = tender3.filter(t -> t.get("winners") != null)
                    .map(t -> t.getAsCollectionPayload("winners").stream()
                            .map(w -> w.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()))
                            .collect(Collectors.toSet()))
                    .orElse(Collections.emptySet());
            assertThat(winnersIdsOfTender3, equalTo(ImmutableSet.of(
                    companies.get(Company.COMPANY3),
                    companies.get(Company.COMPANY7)
            )));

            final Set<UUID> customWinnersIdsOfTender3 = tender3.filter(t -> t.get("customWinners") != null)
                    .map(t -> t.getAsCollectionPayload("customWinners").stream()
                            .map(w -> w.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()))
                            .collect(Collectors.toSet()))
                    .orElse(Collections.emptySet());
            assertThat(customWinnersIdsOfTender3, equalTo(ImmutableSet.of(
                    companies.get(Company.COMPANY3),
                    companies.get(Company.COMPANY7)
            )));
        }

        final Optional<Payload> tender4 = runtimeFixture.getDao().getByIdentifier(tenderType, tenders.get(Tender.T4));
        assertTrue(tender4.isPresent());

        final Optional<Payload> topWinner = runtimeFixture.getDao().getAllReferencedInstancesOf(topWinnerReference, companyType).stream()
                .findAny();
        log.debug("Top winner: {}", topWinner);
        final Optional<UUID> topWinnerId = topWinner.map(winner -> winner.getAs(UUID.class, runtimeFixture.getIdProvider().getName()));
        assertThat(topWinner.isPresent(), equalTo(Boolean.TRUE));
        assertThat(topWinnerId.get(), equalTo(companies.get(Company.COMPANY3)));

        final List<Payload> topWinners = runtimeFixture.getDao().getAllReferencedInstancesOf(topWinnersReference, companyType);
        log.debug("Top winners: {}", topWinners);
        final Set<UUID> topWinnerIds = topWinners.stream().map(winner -> winner.getAs(UUID.class, runtimeFixture.getIdProvider().getName())).collect(Collectors.toSet());
        assertThat(topWinnerIds, equalTo(ImmutableSet.of(
                companies.get(Company.COMPANY3))
        ));

        final Collection<Payload> allWinners = runtimeFixture.getDao().getAllReferencedInstancesOf(allWinnersReference, companyType);
        log.debug("All winners: {}", allWinners);
        final Set<UUID> allWinnerIds = allWinners.stream()
                .map(winner -> winner.getAs(UUID.class, runtimeFixture.getIdProvider().getName()))
                .collect(Collectors.toSet());
        assertThat(allWinnerIds, equalTo(ImmutableSet.of(
                companies.get(Company.COMPANY2),
                companies.get(Company.COMPANY3),
                companies.get(Company.COMPANY7)
        )));

        final Collection<Payload> tenderWinnerList = runtimeFixture.getDao().getAllReferencedInstancesOf(tenderWinnerListReference, companyType);
        log.debug("Tender winners: {}", tenderWinnerList);
        final Set<UUID> tenderWinnerIds = tenderWinnerList.stream()
                .map(winner -> winner.getAs(UUID.class, runtimeFixture.getIdProvider().getName()))
                .collect(Collectors.toSet());
        assertThat(tenderWinnerIds, equalTo(ImmutableSet.of(
                companies.get(Company.COMPANY2),
                companies.get(Company.COMPANY3),
                winnerIdOfTender3.get()
        )));

        final Payload tenderWinnerListCount = runtimeFixture.getDao().getStaticData(tenderWinnerListCountAttribute);
        log.debug("Tender winner list count: {}", tenderWinnerListCount);
        assertThat(tenderWinnerListCount.getAs(Integer.class, "tenderWinnerListCount"), equalTo(tenderWinnerIds.size()));

        final Collection<Payload> lastCompanies = runtimeFixture.getDao().getAllReferencedInstancesOf(lastCompaniesReference, companyType);
        log.debug("Last companies: {}", lastCompanies);
        final Set<UUID> lastCompaniesIds = lastCompanies.stream()
                .map(last -> last.getAs(UUID.class, runtimeFixture.getIdProvider().getName()))
                .collect(Collectors.toSet());
        assertThat(lastCompaniesIds, equalTo(ImmutableSet.of(
                companies.get(Company.COMPANY3),
                companies.get(Company.COMPANY6),
                companies.get(Company.COMPANY7)
        )));

        final Payload t1t3Exist = runtimeFixture.getDao().getStaticData(t1t3ExistsAttribute);
        log.debug("T1 & T3 exist: {}", t1t3Exist);

        final Collection<Payload> t1t3Winners = runtimeFixture.getDao().getAllReferencedInstancesOf(t1t3WinnersReference, companyType);
        log.debug("T1 & T3 winners: {}", t1t3Winners);
        final Set<UUID> t1t3WinnerIds = t1t3Winners.stream()
                .map(winner -> winner.getAs(UUID.class, runtimeFixture.getIdProvider().getName()))
                .collect(Collectors.toSet());
        assertThat(t1t3WinnerIds, hasItems(
                equalTo(companies.get(Company.COMPANY2)),
                equalTo(companies.get(Company.COMPANY3)),
                equalTo(companies.get(Company.COMPANY7))
        ));
        assertThat(t1t3WinnerIds, hasSize(3));

        log.debug("Custom query times:\n" +
                "  - Tender1.winner: {} / {}\n" +
                "  - Tender3.winner: {} / {}\n" +
                "  - Tender3.winners: {} / {}", new Object[] {end2 - start2, end1 - start1, end4 - start4, end3 - start3, end6 - start6, end5 - start5});
    }
}
