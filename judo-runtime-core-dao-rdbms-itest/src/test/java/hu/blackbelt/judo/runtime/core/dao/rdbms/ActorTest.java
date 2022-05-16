package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.esm.accesspoint.AccessType;
import hu.blackbelt.judo.meta.esm.accesspoint.ActorKind;
import hu.blackbelt.judo.meta.esm.accesspoint.ActorType;
import hu.blackbelt.judo.meta.esm.accesspoint.ClaimType;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.structure.*;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import java.security.Principal;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.esm.accesspoint.util.builder.AccesspointBuilders.*;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class ActorTest {

    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    private static final String NAME = "name";
    private static final String GROUP = "group";
    private static final String EMAIL = "email";
    private static final String AS = "as";

    private static final String ALL_AS = "allAs";
    private static final String X_AS = "xAs";
    private static final String MY_AS = "myAs";

    private static final String X_FILTER = MODEL_NAME + "::EntityA!filter(a | a.group == 'X')";
    private static final String MY_FILTER = "self.\\as";

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected static Model getEsmModel() {

        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();

        final DataMember nameOfEntityA = newDataMemberBuilder()
                .withName(NAME)
                .withDataType(stringType)
                .withRequired(true)
                .withMemberType(MemberType.STORED)
                .withIdentifier(true)
                .build();

        final EntityType entityA = newEntityTypeBuilder()
                .withName("EntityA")
                .withAttributes(nameOfEntityA)
                .withAttributes(newDataMemberBuilder()
                        .withName(GROUP)
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(entityA).withMapping(newMappingBuilder().withTarget(entityA).build()).build();

        final TransferObjectType a = newTransferObjectTypeBuilder()
                .withName("A")
                .withMapping(newMappingBuilder().withTarget(entityA).build())
                .withAttributes(newDataMemberBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.MAPPED)
                        .withBinding(nameOfEntityA)
                        .build())
                .build();

        final DataMember emailOfUser = newDataMemberBuilder()
                .withName(EMAIL)
                .withDataType(stringType)
                .withIdentifier(true)
                .withMemberType(MemberType.STORED)
                .withRequired(true)
                .build();
        final EntityType user = newEntityTypeBuilder()
                .withName("User")
                .withAttributes(emailOfUser)
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName(AS)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.STORED)
                        .withTarget(entityA)
                        .withCreateable(true).withUpdateable(true).withDeleteable(true)
                        .build())
                .build();
        useEntityType(user).withMapping(newMappingBuilder().withTarget(user).build()).build();

        final DataMember emailOfUserDTO = newDataMemberBuilder()
                .withName(EMAIL)
                .withDataType(stringType)
                .withRequired(true)
                .withMemberType(MemberType.MAPPED)
                .withBinding(emailOfUser)
                .build();
        final TransferObjectType userDTO = newTransferObjectTypeBuilder()
                .withName("UserDTO")
                .withMapping(newMappingBuilder().withTarget(user).build())
                .withAttributes(emailOfUserDTO)
                .build();

        final DataMember emailOfExternalUserDTO = newDataMemberBuilder()
                .withName(EMAIL)
                .withDataType(stringType)
                .withRequired(true)
                .withMemberType(MemberType.TRANSIENT)
                .withBinding(emailOfUser)
                .build();
        final TransferObjectType externalUserDTO = newTransferObjectTypeBuilder()
                .withName("ExternalUserDTO")
                .withAttributes(emailOfExternalUserDTO)
                .build();

        final ActorType actorWithMappedPrincipal = newActorTypeBuilder()
                .withName("ActorWithMappedPrincipal")
                .withAnonymous(false)
                .withKind(ActorKind.HUMAN)
                .withPrincipal(userDTO)
                .withAccesses(newAccessBuilder()
                        .withName(ALL_AS)
                        .withLower(0).withUpper(-1)
                        .withTarget(a)
                        .withAccessType(AccessType.ALL)
                        .build())
                .withAccesses(newAccessBuilder()
                        .withName(X_AS)
                        .withLower(0).withUpper(-1)
                        .withTarget(a)
                        .withAccessType(AccessType.DERIVED)
                        .withGetterExpression(X_FILTER)
                        .build())
                .withAccesses(newAccessBuilder()
                        .withName(MY_AS)
                        .withLower(0).withUpper(-1)
                        .withTarget(a)
                        .withAccessType(AccessType.DERIVED)
                        .withGetterExpression(MY_FILTER)
                        .build())
                .withClaims(newClaimBuilder()
                        .withClaimType(ClaimType.EMAIL)
                        .withAttribute(emailOfUserDTO)
                        .build())
                .build();
        final ActorType actorWithEntityPrincipal = newActorTypeBuilder()
                .withName("ActorWithEntityPrincipal")
                .withAnonymous(false)
                .withKind(ActorKind.HUMAN)
                .withPrincipal(user)
                .withAccesses(newAccessBuilder()
                        .withName(ALL_AS)
                        .withLower(0).withUpper(-1)
                        .withTarget(a)
                        .withAccessType(AccessType.ALL)
                        .build())
                .withAccesses(newAccessBuilder()
                        .withName(X_AS)
                        .withLower(0).withUpper(-1)
                        .withTarget(a)
                        .withAccessType(AccessType.DERIVED)
                        .withGetterExpression(X_FILTER)
                        .build())
                .withAccesses(newAccessBuilder()
                        .withName(MY_AS)
                        .withLower(0).withUpper(-1)
                        .withTarget(a)
                        .withAccessType(AccessType.DERIVED)
                        .withGetterExpression(MY_FILTER)
                        .build())
                .withClaims(newClaimBuilder()
                        .withClaimType(ClaimType.EMAIL)
                        .withAttribute(emailOfUser)
                        .build())
                .build();
        final ActorType actorWithUnmappedPrincipal = newActorTypeBuilder()
                .withName("ActorWithUnmappedPrincipal")
                .withAnonymous(false)
                .withKind(ActorKind.HUMAN)
                .withPrincipal(externalUserDTO)
                .withAccesses(newAccessBuilder()
                        .withName(ALL_AS)
                        .withLower(0).withUpper(-1)
                        .withTarget(a)
                        .withAccessType(AccessType.ALL)
                        .build())
                .withAccesses(newAccessBuilder()
                        .withName(X_AS)
                        .withLower(0).withUpper(-1)
                        .withTarget(a)
                        .withAccessType(AccessType.DERIVED)
                        .withGetterExpression(X_FILTER)
                        .build())
                .withClaims(newClaimBuilder()
                        .withClaimType(ClaimType.EMAIL)
                        .withAttribute(emailOfExternalUserDTO)
                        .build())
                .build();
        final ActorType actorWithoutPrincipal = newActorTypeBuilder()
                .withName("ActorWithoutPrincipal")
                .withAnonymous(false)
                .withKind(ActorKind.HUMAN)
                .withAccesses(newAccessBuilder()
                        .withName(ALL_AS)
                        .withLower(0).withUpper(-1)
                        .withTarget(a)
                        .withAccessType(AccessType.ALL)
                        .build())
                .withAccesses(newAccessBuilder()
                        .withName(X_AS)
                        .withLower(0).withUpper(-1)
                        .withTarget(a)
                        .withAccessType(AccessType.DERIVED)
                        .withGetterExpression(X_FILTER)
                        .build())
                .build();
        final ActorType anonymousWithUnmappedPrincipal = newActorTypeBuilder()
                .withName("AnonymousWithUnmappedPrincipal")
                .withAnonymous(true)
                .withKind(ActorKind.HUMAN)
                .withPrincipal(externalUserDTO)
                .withClaims(newClaimBuilder()
                        .withClaimType(ClaimType.EMAIL)
                        .withAttribute(emailOfExternalUserDTO)
                        .build())
                .withAccesses(newAccessBuilder()
                        .withName(ALL_AS)
                        .withLower(0).withUpper(-1)
                        .withTarget(a)
                        .withAccessType(AccessType.ALL)
                        .build())
                .withAccesses(newAccessBuilder()
                        .withName(X_AS)
                        .withLower(0).withUpper(-1)
                        .withTarget(a)
                        .withAccessType(AccessType.DERIVED)
                        .withGetterExpression(X_FILTER)
                        .build())
                .build();
        final ActorType anonymousWithoutPrincipal = newActorTypeBuilder()
                .withName("AnonymousWithoutPrincipal")
                .withAnonymous(true)
                .withKind(ActorKind.HUMAN)
                .withAccesses(newAccessBuilder()
                        .withName(ALL_AS)
                        .withLower(0).withUpper(-1)
                        .withTarget(a)
                        .withAccessType(AccessType.ALL)
                        .build())
                .withAccesses(newAccessBuilder()
                        .withName(X_AS)
                        .withLower(0).withUpper(-1)
                        .withTarget(a)
                        .withAccessType(AccessType.DERIVED)
                        .withGetterExpression(X_FILTER)
                        .build())
                .build();

        final Model model = NamespaceBuilders.newModelBuilder().withName(MODEL_NAME).build();

        model.getElements().addAll(Arrays.asList(
                stringType, entityA, a, user, userDTO, externalUserDTO,
                actorWithMappedPrincipal, actorWithEntityPrincipal, actorWithUnmappedPrincipal, actorWithoutPrincipal,
                anonymousWithUnmappedPrincipal, anonymousWithoutPrincipal
        ));

        return model;
    }

    private static UUID a1Id;
    private static UUID a2Id;
    private static UUID a3Id;
    private static UUID user1Id;
    private static UUID user2Id;

    private static Collection<UUID> allAs;
    private static Collection<UUID> xAs;
    private static Collection<UUID> myAs;

    @BeforeAll
    public static void setup(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(getEsmModel(), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        final EClass entityAType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".EntityA").get();
        final EClass userType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".User").get();
        final EReference asOfUserReference = userType.getEAllReferences().stream().filter(r -> AS.equals(r.getName())).findAny().get();

        final Payload a1 = runtimeFixture.getDao().create(entityAType, Payload.map(NAME, "a1", GROUP, "Y"), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        a1Id = a1.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());

        final Payload a2 = runtimeFixture.getDao().create(entityAType, Payload.map(NAME, "a2", GROUP, "Y"), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        a2Id = a2.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());

        final Payload a3 = runtimeFixture.getDao().create(entityAType, Payload.map(NAME, "a3", GROUP, "X"), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        a3Id = a3.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());

        final Payload user1 = runtimeFixture.getDao().create(userType, Payload.map(NAME, "User1", EMAIL, "user1@example.com"), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        user1Id = user1.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        runtimeFixture.getDao().addReferences(asOfUserReference, user1Id, Collections.singleton(a1Id));

        final Payload user2 = runtimeFixture.getDao().create(userType, Payload.map(NAME, "User2", EMAIL, "user2@example.com"), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        user2Id = user2.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("A instances: {}", Arrays.asList(a1Id, a2Id, a3Id));
        log.debug("User instances: {}", Arrays.asList(user1Id, user2Id));

        allAs = ImmutableSet.of(a1Id, a2Id, a3Id);
        xAs = ImmutableSet.of(a3Id);
        myAs = ImmutableSet.of(a1Id);
    }

    private static final BiFunction<Collection<Payload>, IdentifierProvider<UUID>, Set<UUID>> EXTRACT_IDS =
            (payloadList, identifierProvider) -> payloadList.stream()
                    .map(p -> p.getAs(identifierProvider.getType(), identifierProvider.getName()))
                    .collect(Collectors.toSet());

    @AfterAll
    public static void teardown(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
    }

    @Test
    public void testActorWithMappedPrincipal(JudoRuntimeFixture runtimeFixture) {
        final EClass actorWithMappedPrincipalType = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".ActorWithMappedPrincipal").get();

        final Principal user1 = getPrincipal(runtimeFixture, actorWithMappedPrincipalType, user1Id);
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithMappedPrincipalType), "AllAs", user1), runtimeFixture.getIdProvider()), equalTo(allAs));
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithMappedPrincipalType), "XAs", user1), runtimeFixture.getIdProvider()), equalTo(xAs));
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithMappedPrincipalType), "MyAs", user1), runtimeFixture.getIdProvider()), equalTo(myAs));

        final Principal user2 = getPrincipal(runtimeFixture, actorWithMappedPrincipalType, user2Id);
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithMappedPrincipalType), "AllAs", user2), runtimeFixture.getIdProvider()), equalTo(allAs));
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithMappedPrincipalType), "XAs", user2), runtimeFixture.getIdProvider()), equalTo(xAs));
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithMappedPrincipalType), "MyAs", user2), runtimeFixture.getIdProvider()), equalTo(Collections.emptySet()));
    }

    @Test
    public void testActorWithEntityPrincipal(JudoRuntimeFixture runtimeFixture) {
        final EClass actorWithEntityPrincipalType = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".ActorWithEntityPrincipal").get();

        final Principal user1 = getPrincipal(runtimeFixture, actorWithEntityPrincipalType, user1Id);
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithEntityPrincipalType), "AllAs", user1), runtimeFixture.getIdProvider()), equalTo(allAs));
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithEntityPrincipalType), "XAs", user1), runtimeFixture.getIdProvider()), equalTo(xAs));
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithEntityPrincipalType), "MyAs", user1), runtimeFixture.getIdProvider()), equalTo(myAs));

        final Principal user2 = getPrincipal(runtimeFixture, actorWithEntityPrincipalType, user2Id);
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithEntityPrincipalType), "AllAs", user2), runtimeFixture.getIdProvider()), equalTo(allAs));
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithEntityPrincipalType), "XAs", user2), runtimeFixture.getIdProvider()), equalTo(xAs));
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithEntityPrincipalType), "MyAs", user2), runtimeFixture.getIdProvider()), equalTo(Collections.emptySet()));
    }

    @Test
    public void testActorWithUnmappedPrincipal(JudoRuntimeFixture runtimeFixture) {
        final EClass actorWithUnmappedPrincipalType = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".ActorWithUnmappedPrincipal").get();

        final Principal externalUser = getExternalPrincipal(runtimeFixture, actorWithUnmappedPrincipalType);
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithUnmappedPrincipalType), "AllAs", externalUser), runtimeFixture.getIdProvider()), equalTo(allAs));
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithUnmappedPrincipalType), "XAs", externalUser), runtimeFixture.getIdProvider()), equalTo(xAs));
    }

    @Test
    public void testActorWithoutPrincipal(JudoRuntimeFixture runtimeFixture) {
        final EClass actorWithoutPrincipalType = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".ActorWithoutPrincipal").get();

        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithoutPrincipalType), "AllAs", null), runtimeFixture.getIdProvider()), equalTo(allAs));
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(actorWithoutPrincipalType), "XAs", null), runtimeFixture.getIdProvider()), equalTo(xAs));
    }

    @Test
    public void testAnonymousWithUnmappedPrincipal(JudoRuntimeFixture runtimeFixture) {
        final EClass anonymousWithUnmappedPrincipalType = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".AnonymousWithUnmappedPrincipal").get();

        final Principal externalUser = getExternalPrincipal(runtimeFixture, anonymousWithUnmappedPrincipalType);
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(anonymousWithUnmappedPrincipalType), "AllAs", externalUser), runtimeFixture.getIdProvider()), equalTo(allAs));
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(anonymousWithUnmappedPrincipalType), "XAs", externalUser), runtimeFixture.getIdProvider()), equalTo(xAs));
    }

    @Test
    public void testAnonymousWithoutPrincipal(JudoRuntimeFixture runtimeFixture) {
        final EClass anonymousWithoutPrincipalType = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".AnonymousWithoutPrincipal").get();

        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(anonymousWithoutPrincipalType), "AllAs", null), runtimeFixture.getIdProvider()), equalTo(allAs));
        assertThat(EXTRACT_IDS.apply(getAccessElements(runtimeFixture.getDispatcher(), AsmUtils.getClassifierFQName(anonymousWithoutPrincipalType), "XAs", null), runtimeFixture.getIdProvider()), equalTo(xAs));
    }

    private JudoPrincipal getPrincipal(JudoRuntimeFixture runtimeFixture, EClass actorType, UUID id) {
        final Payload actor = runtimeFixture.getDao().getByIdentifier(actorType, id).get();

        return JudoPrincipal.builder().name(actor.getAs(String.class, EMAIL))
                .attributes(actor)
                .client(AsmUtils.getClassifierFQName(actorType))
                .build();
    }

    private JudoPrincipal getExternalPrincipal(JudoRuntimeFixture runtimeFixture, EClass actorType) {
        return JudoPrincipal.builder().name("ExternalUser")
                .attributes(Collections.emptyMap())
                .client(AsmUtils.getClassifierFQName(actorType))
                .build();
    }

    private Collection<Payload> getAccessElements(Dispatcher dispatcher, final String actorFqName, final String referenceName, final Principal principal) {
        final Map<String, Object> exchange = new HashMap<>();
        if (principal != null) {
            exchange.put(Dispatcher.PRINCIPAL_KEY, principal);
        }
        final Collection<Payload> result = ((Collection<Map<String, Object>>) dispatcher
                .callOperation(actorFqName + "#_list" + referenceName, exchange)
                .get("output")).stream()
                .map(p -> Payload.asPayload(p))
                .collect(Collectors.toList());

        log.debug("Result of {}._list{} by {}: {}", new Object[] {actorFqName, referenceName, principal != null ? principal.getName() : "ANONYMOUS", result});

        return result;
    }
}
