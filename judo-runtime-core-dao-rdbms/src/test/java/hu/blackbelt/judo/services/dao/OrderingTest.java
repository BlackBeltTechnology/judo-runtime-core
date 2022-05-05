package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.esm.measure.DurationType;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.structure.*;
import hu.blackbelt.judo.meta.esm.type.BooleanType;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.meta.esm.type.TimestampType;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.lang.Class;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.structure.MemberType.STORED;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class OrderingTest {

    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected String getModelName() {
        return MODEL_NAME;
    }

    @AfterEach
    public void teardown(final RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    void testComplexOrderBy(final RdbmsDaoFixture daoFixture, final RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();

        final EntityType user = newEntityTypeBuilder()
                .withName("User")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("age")
                        .withDataType(integerType)
                        .withRequired(false)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(user).withMapping(newMappingBuilder().withTarget(user).build()).build();

        final EntityType group = newEntityTypeBuilder()
                .withName("Group")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("label")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("(self.name + ' ' + (self.owner!isDefined() ? self.owner.name : '') + ' ' + (self.owner!isDefined() ? self.owner.age!asString() : ''))!trim()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("owner")
                        .withLower(0).withUpper(1)
                        .withTarget(user)
                        .withMemberType(MemberType.STORED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .build())
                .build();
        useEntityType(group).withMapping(newMappingBuilder().withTarget(group).build()).build();

        final TransferObjectType tester = newTransferObjectTypeBuilder()
                .withName("Tester")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("groups")
                        .withTarget(group)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Group!sort(g | g.label)")
                        .build())
                .build();

        final Model model = newModelBuilder()
                .withName(getModelName())
                .withElements(stringType, integerType, user, group, tester)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass userType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (DTO_PACKAGE + ".User").equals(AsmUtils.getClassifierFQName(c))).findAny().get();
        final EClass groupType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (DTO_PACKAGE + ".Group").equals(AsmUtils.getClassifierFQName(c))).findAny().get();
        final EClass testerType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (MODEL_NAME + ".Tester").equals(AsmUtils.getClassifierFQName(c))).findAny().get();
        final EReference groupsReference = testerType.getEAllReferences().stream().filter(r -> "groups".equals(r.getName())).findAny().get();


        final Payload user1 = daoFixture.getDao().create(userType, Payload.map(
                "name", "Gipsz Jakab",
                "age", 30
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());

        final Payload user2 = daoFixture.getDao().create(userType, Payload.map(
                "name", "Teszt Elek",
                "age", 35
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());

        final Payload group4 = daoFixture.getDao().create(groupType, Payload.map(
                "name", "Group4",
                "owner", user1
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());

        final Payload group3 = daoFixture.getDao().create(groupType, Payload.map(
                "name", "Group3"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());

        final Payload group2 = daoFixture.getDao().create(groupType, Payload.map(
                "name", "Group2",
                "owner", user2
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());

        final Payload group1 = daoFixture.getDao().create(groupType, Payload.map(
                "name", "Group1",
                "owner", user1
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());

        final List<Payload> groups = daoFixture.getDao().getAllReferencedInstancesOf(groupsReference, groupsReference.getEReferenceType());
        final List<String> groupLabels = groups.stream()
                .map(g -> g.getAs(String.class, "label"))
                .collect(Collectors.toList());

        assertThat(groupLabels, equalTo(Arrays.asList(
                "Group1 Gipsz Jakab 30",
                "Group2 Teszt Elek 35",
                "Group3",
                "Group4 Gipsz Jakab 30"
        )));
    }

    @Test
    void testSeekingByDerivedContainingConstant(final RdbmsDaoFixture daoFixture, final RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();

        final EntityType user = newEntityTypeBuilder()
                .withName("User")
                .withAttributes(newDataMemberBuilder()
                        .withName("firstName")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("lastName")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.lastName + ' ' + self.firstName")
                        .build())
                .build();
        final EntityType group = newEntityTypeBuilder()
                .withName("Group")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();

        final TwoWayRelationMember groupsOfUser = newTwoWayRelationMemberBuilder()
                .withName("groups")
                .withTarget(group)
                .withLower(0).withUpper(-1)
                .withRelationKind(RelationKind.ASSOCIATION)
                .withMemberType(MemberType.STORED)
                .build();
        final TwoWayRelationMember membersOfGroup = newTwoWayRelationMemberBuilder()
                .withName("members")
                .withTarget(user)
                .withLower(0).withUpper(-1)
                .withRelationKind(RelationKind.ASSOCIATION)
                .withMemberType(MemberType.STORED)
                .withPartner(groupsOfUser)
                .build();
        useTwoWayRelationMember(groupsOfUser)
                .withPartner(membersOfGroup)
                .build();

        useEntityType(user)
                .withMapping(newMappingBuilder().withTarget(user).build())
                .withRelations(groupsOfUser)
                .build();
        useEntityType(group)
                .withMapping(newMappingBuilder().withTarget(group).build())
                .withRelations(membersOfGroup)
                .build();

        final TransferObjectType access = newTransferObjectTypeBuilder()
                .withName("Access")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("allGroupMembers")
                        .withTarget(user)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withGetterExpression(getModelName() + "::Group.members")
                        .build())
                .build();

        final Model model = newModelBuilder()
                .withName(getModelName())
                .withElements(stringType, user, group, access)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass userType = (EClass) daoFixture.getAsmUtils().resolve(DTO_PACKAGE + ".User").get();
        final EClass groupType = (EClass) daoFixture.getAsmUtils().resolve(DTO_PACKAGE + ".Group").get();
        final EAttribute nameOfUserAttribute = daoFixture.getAsmUtils().resolveAttribute(DTO_PACKAGE + ".User#name").get();
        final EReference allGroupMembersReference = daoFixture.getAsmUtils().resolveReference(MODEL_NAME + ".Access#allGroupMembers").get();

        final Payload u1 = daoFixture.getDao().create(userType, Payload.map(
                "firstName", "Eszter",
                "lastName", "Vincs"
        ), null);
        final Payload u2 = daoFixture.getDao().create(userType, Payload.map(
                "firstName", "Jakab",
                "lastName", "Gipsz"
        ), null);
        final Payload u3 = daoFixture.getDao().create(userType, Payload.map(
                "firstName", "Elek",
                "lastName", "Teszt"
        ), null);

        final Payload g1 = daoFixture.getDao().create(groupType, Payload.map(
                "name", "Manager",
                "members", Arrays.asList(u1, u2, u3)
        ), null);
        final Payload g2 = daoFixture.getDao().create(groupType, Payload.map(
                "name", "Employee",
                "members", Arrays.asList(u1, u2, u3)
        ), null);

        final List<String> users = daoFixture.getDao().search(userType, DAO.QueryCustomizer.<UUID>builder()
                        .mask(ImmutableMap.of("lastName", true, "firstName", true))
                        .orderBy(DAO.OrderBy.builder()
                                .attribute(nameOfUserAttribute)
                                .descending(false)
                                .build())
                        .seek(DAO.Seek.builder()
                                .limit(2)
                                .reverse(false)
                                .build())
                        .build())
                .stream()
                .map(p -> p.getAs(String.class, "lastName") + " " + p.getAs(String.class, "firstName"))
                .collect(Collectors.toList());

        assertThat(users, equalTo(Arrays.asList(
                "Gipsz Jakab",
                "Teszt Elek"
        )));

        final List<String> allGroupMembers = daoFixture.getDao().searchReferencedInstancesOf(allGroupMembersReference, userType, DAO.QueryCustomizer.<UUID>builder()
                        .mask(ImmutableMap.of("lastName", true, "firstName", true))
                        .orderBy(DAO.OrderBy.builder()
                                .attribute(nameOfUserAttribute)
                                .descending(false)
                                .build())
                        .seek(DAO.Seek.builder()
                                .limit(2)
                                .reverse(false)
                                .build())
                        .build())
                .stream()
                .map(p -> p.getAs(String.class, "lastName") + " " + p.getAs(String.class, "firstName"))
                .collect(Collectors.toList());
        log.info("All members: {}", allGroupMembers);

        assertThat(allGroupMembers, equalTo(Arrays.asList(
                "Gipsz Jakab",
                "Teszt Elek"
        )));
    }

    private void createWatchSchemaAndRecords(final RdbmsDaoFixture daoFixture, final RdbmsDatasourceFixture datasourceFixture) {
        // Create Watch schema
        StringType stringType = newStringTypeBuilder().withName("string").withMaxLength(255).build();
        TimestampType timestampType = newTimestampTypeBuilder().withName("timestampType").withBaseUnit(DurationType.SECOND).build();
        BooleanType booleanType = newBooleanTypeBuilder().withName("boolean").build();

        EntityType watch = newEntityTypeBuilder()
                .withName("Watch")
                .withAttributes(
                        newDataMemberBuilder()
                                .withName("name")
                                .withRequired(true)
                                .withDataType(stringType)
                                .withMemberType(MemberType.STORED)
                                .build(),
                        newDataMemberBuilder()
                                .withName("autoTime")
                                .withDataType(timestampType)
                                .withMemberType(STORED)
                                .withDefaultExpression(getModelName() + "::timestampType!now()")
                                .build(),
                        newDataMemberBuilder()
                                .withName("time")
                                .withRequired(true)
                                .withDataType(timestampType)
                                .withMemberType(MemberType.STORED)
                                .build(),
                        newDataMemberBuilder()
                                .withName("isGolden")
                                .withRequired(false)
                                .withDataType(booleanType)
                                .withMemberType(MemberType.STORED)
                                .build()
                        )
                .build();
        watch.setMapping(newMappingBuilder().withTarget(watch).build());

        final Model model = newModelBuilder()
                .withName(getModelName())
                .withElements(timestampType, stringType, watch, booleanType)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized());

        // Create Watch records
        Class<UUID> idType = daoFixture.getIdProvider().getType();
        String idName = daoFixture.getIdProvider().getName();

        EClass watchEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Watch").get();

        var rolexName = "rolex";
        UUID rolexId = daoFixture.getDao()
                .create(watchEClass,
                        Payload.map(
                            "name", rolexName,
                            "time", OffsetDateTime.of(2021, 7, 28, 10, 30, 1, 111_000_000, ZoneOffset.UTC),
                            "isGolden", false
                        ),
                        DAO.QueryCustomizer.<UUID>builder().build())
                .getAs(idType, idName);
        log.debug("Watch '{}' created with id {}", rolexName, rolexId);

        var omegaName = "omega";
        UUID omegaId = daoFixture.getDao()
                .create(watchEClass,
                        Payload.map(
                            "name", omegaName,
                            "time", OffsetDateTime.of(2021, 7, 28, 10, 30, 1, 110_000_000, ZoneOffset.UTC),
                            "isGolden", null
                        ),
                        DAO.QueryCustomizer.<UUID>builder().build())
                .getAs(idType, idName);
        log.debug("Watch '{}' created with id {}", omegaName, omegaId);

        var bulgariName = "bulgari";
        UUID bulgariId = daoFixture.getDao()
                .create(watchEClass,
                        Payload.map(
                            "name", bulgariName,
                            "time", OffsetDateTime.of(2021, 7, 28, 10, 30, 1, 100_000_000, ZoneOffset.UTC),
                            "isGolden", true
                        ),
                        DAO.QueryCustomizer.<UUID>builder().build())
                .getAs(idType, idName);
        log.debug("Watch '{}' created with id {}", bulgariName, bulgariId);

        var hamiltonName = "hamilton";
        UUID hamiltonId = daoFixture.getDao()
                .create(watchEClass,
                        Payload.map(
                            "name", hamiltonName,
                            "time", OffsetDateTime.of(2021, 7, 28, 10, 30, 1, 0, ZoneOffset.UTC),
                            "isGolden", null
                        ),
                        DAO.QueryCustomizer.<UUID>builder().build())
                .getAs(idType, idName);
        log.debug("Watch '{}' created with id {}", hamiltonName, hamiltonId);
    }

    @Test
    @DisplayName("Test TimestampOrder")
    public void testTimestampOrder(final RdbmsDaoFixture daoFixture, final RdbmsDatasourceFixture datasourceFixture) {
        createWatchSchemaAndRecords(daoFixture, datasourceFixture);

        EClass watchEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Watch").get();
        EAttribute time = watchEClass.getEAllAttributes().stream().filter(a -> "time".equals(a.getName())).collect(Collectors.toList()).get(0);

        List<Payload> watches = daoFixture.getDao()
                .search(watchEClass,
                        DAO.QueryCustomizer.<UUID>builder()
                                .orderBy(DAO.OrderBy.builder()
                                                 .attribute(time)
                                                 .descending(false)
                                                 .build())
                                .build());

        log.debug(watches.stream().map(p -> {
            OffsetDateTime offsetDateTime = p.getAs(OffsetDateTime.class, "time");
            return "[" + p.getAs(String.class, "name") + " - " + offsetDateTime + " (nano: " + offsetDateTime.getNano() + ")]";
        }).collect(Collectors.joining(", ")));

        assertEquals(
                Arrays.asList("hamilton", "bulgari", "omega", "rolex"),
                watches.stream().map(p -> (String)p.get("name")).collect(Collectors.toList())
        );
    }

    @Test
    @DisplayName("testBooleanOrder")
    public void testBooleanOrder(final RdbmsDaoFixture daoFixture, final RdbmsDatasourceFixture datasourceFixture) {
        createWatchSchemaAndRecords(daoFixture, datasourceFixture);

        EClass watchEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Watch").get();
        EAttribute isGolden = watchEClass.getEAllAttributes().stream().filter(a -> "isGolden".equals(a.getName())).collect(Collectors.toList()).get(0);

        // ascending order
        List<Payload> watchesAsc = daoFixture.getDao()
                .search(watchEClass,
                        DAO.QueryCustomizer.<UUID>builder()
                                .orderBy(DAO.OrderBy.builder()
                                        .attribute(isGolden)
                                        .descending(false)
                                        .build())
                                .build());
        assertEquals(
                Arrays.asList(false, true, null, null),
                watchesAsc.stream().map(p -> (Boolean)p.get("isGolden")).collect(Collectors.toList())
        );

        // descending order
        List<Payload> watchesDesc = daoFixture.getDao()
                .search(watchEClass,
                        DAO.QueryCustomizer.<UUID>builder()
                                .orderBy(DAO.OrderBy.builder()
                                        .attribute(isGolden)
                                        .descending(true)
                                        .build())
                                .build());
        assertEquals(
                Arrays.asList(null, null, true, false),
                watchesDesc.stream().map(p -> (Boolean)p.get("isGolden")).collect(Collectors.toList())
        );
    }

    @Test
    @DisplayName("Test TimestampPaging")
    public void testTimestampPaging(final RdbmsDaoFixture daoFixture, final RdbmsDatasourceFixture datasourceFixture) {
        createWatchSchemaAndRecords(daoFixture, datasourceFixture);

        EClass watchEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Watch").get();
        EAttribute time = watchEClass.getEAllAttributes().stream().filter(a -> "time".equals(a.getName())).collect(Collectors.toList()).get(0);

        List<Payload> watches1to2 = daoFixture.getDao()
                .search(watchEClass,
                        DAO.QueryCustomizer.<UUID>builder()
                                .orderBy(DAO.OrderBy.builder()
                                                 .attribute(time)
                                                 .descending(false)
                                                 .build())
                                .seek(DAO.Seek.builder()
                                              .limit(2)
                                              .lastItem(null)
                                              .build())
                                .build());
        log.debug(watches1to2.stream().map(p -> {
            OffsetDateTime offsetDateTime = p.getAs(OffsetDateTime.class, "time");
            return "[" + p.getAs(String.class, "name") + " - " + offsetDateTime + " (nano: " + offsetDateTime.getNano() + ")]";
        }).collect(Collectors.joining(", ")));

        List<Payload> watches3to4 = daoFixture.getDao()
                .search(watchEClass,
                        DAO.QueryCustomizer.<UUID>builder()
                                .orderBy(DAO.OrderBy.builder()
                                                 .attribute(time)
                                                 .descending(false)
                                                 .build())
                                .seek(DAO.Seek.builder()
                                              .limit(2)
                                              .lastItem(watches1to2.get(watches1to2.size() - 1))
                                              .build())
                                .build());
        log.debug(watches3to4.stream().map(p -> {
            OffsetDateTime offsetDateTime = p.getAs(OffsetDateTime.class, "time");
            return "[" + p.getAs(String.class, "name") + " - " + offsetDateTime + " (nano: " + offsetDateTime.getNano() + ")]";
        }).collect(Collectors.joining(", ")));

        assertEquals(Arrays.asList("hamilton", "bulgari"),
                     watches1to2.stream().map(p -> (String)p.get("name")).collect(Collectors.toList()));
        assertEquals(Arrays.asList("omega", "rolex"),
                     watches3to4.stream().map(p -> (String)p.get("name")).collect(Collectors.toList()));
    }

}
