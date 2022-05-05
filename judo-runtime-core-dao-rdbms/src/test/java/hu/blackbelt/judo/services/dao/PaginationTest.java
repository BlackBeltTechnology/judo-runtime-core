package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.accesspoint.ActorType;
import hu.blackbelt.judo.meta.esm.measure.DurationType;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.meta.esm.structure.TransferObjectType;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static hu.blackbelt.judo.meta.esm.accesspoint.util.builder.AccesspointBuilders.newActorTypeBuilder;
import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class PaginationTest {

    public static final String MODEL_NAME = "M";

    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getEsmModel() {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();

        final Model model = newModelBuilder().withName(getModelName()).build();

        final EntityType list = newEntityTypeBuilder()
                .withName("List")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        list.setMapping(newMappingBuilder().withTarget(list).build());

        final EntityType item = newEntityTypeBuilder()
                .withName("Item")
                .withAttributes(newDataMemberBuilder()
                        .withName("number")
                        .withDataType(integerType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("topic")
                        .withDataType(stringType)
                        .withRequired(false)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("listName")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self!container(" + MODEL_NAME + "::List).name")
                        .build())
                .build();
        item.setMapping(newMappingBuilder().withTarget(item).build());

        useEntityType(list)
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("items")
                        .withTarget(item)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.STORED)
                        .withRelationKind(RelationKind.COMPOSITION)
                        .withCreateable(true)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("sortedItems")
                        .withTarget(item)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression("self.items!sort(i | i.number DESC)")
                        .build())
                .build();

        final TransferObjectType ap = newTransferObjectTypeBuilder()
                .withName("AP")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("lists")
                        .withTarget(list)
                        .withLower(0).withUpper(1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::List!sort(l | l.name)")
                        .build())
                .build();
        final ActorType actor = newActorTypeBuilder().withName("actor").withPrincipal(ap).build();
        useTransferObjectType(ap).withActorType(actor).build();

        model.getElements().addAll(Arrays.asList(
                stringType, integerType, booleanType,
                list, item, ap, actor
        ));
        return model;
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    public void testWindowing(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
        final EClass listType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".List").get();
        final EAttribute nameOfListAttribute = listType.getEAllAttributes().stream().filter(a -> "name".equals(a.getName())).findAny().get();
        final EReference sortedItemsReference = listType.getEAllReferences().stream().filter(a -> "sortedItems".equals(a.getName())).findAny().get();

        final EClass listItemType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Item").get();
        final EAttribute numberOfListItemAttribute = listItemType.getEAllAttributes().stream().filter(a -> "number".equals(a.getName())).findAny().get();
        final EAttribute listNameOfListItemAttribute = listItemType.getEAllAttributes().stream().filter(a -> "listName".equals(a.getName())).findAny().get();
        final EAttribute topicOfListItemAttribute = listItemType.getEAllAttributes().stream().filter(a -> "topic".equals(a.getName())).findAny().get();

        final EClass apType = daoFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".AP").get();
        final EReference apListsReference = apType.getEAllReferences().stream().filter(r -> "lists".equals(r.getName())).findAny().get();

        final Map<Character, UUID> listIds = new HashMap<>();
        for (char ch = 'A'; ch <= 'Z'; ch++) {
            final List<Payload> items = new ArrayList<>();
            for (int i = 0; i < 'Z' - ch + 1; i++) {
                final String topic;
                if (ch < 'E' && i < 3) {
                    topic = "Topic" + (char) (ch + i);
                } else {
                    topic = null;
                }
                items.add(Payload.map(
                        "number", i + 1,
                        "topic", topic
                ));
            }
            final Payload list = Payload.map(
                    "name", "List" + ch,
                    "items", items
            );

            final Payload payload = daoFixture.getDao().create(listType, list, DAO.QueryCustomizer.<UUID>builder()
                    .mask(Collections.emptyMap())
                    .build());
            final UUID id = payload.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());
            listIds.put(ch, id);
        }
        final Map<Character, UUID> orderedListIds = new LinkedHashMap<>();
        listIds.values().stream().map(value -> value.toString()).sorted().forEach(value -> {
            final UUID id = UUID.fromString(value);
            final Character key = listIds.entrySet().stream().filter(e -> Objects.equals(e.getValue(), id)).map(e -> e.getKey()).findAny().get();
            orderedListIds.put(key, id);
        });
        log.debug("List IDs: {}", orderedListIds);
        log.debug("Characters: {}", orderedListIds.keySet());

        final List<Payload> lists1To4 = daoFixture.getDao().search(listType, DAO.QueryCustomizer.<UUID>builder()
                .seek(DAO.Seek.builder()
                        .limit(4)
                        .build())
                .build());
        log.debug("Lists 1 to 4: {}", lists1To4.stream().map(l -> l.getAs(String.class, "name")).collect(Collectors.toList()));
        final List<UUID> lists1To4Ids = lists1To4.stream().map(l -> l.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toList());
        final List<UUID> expectedLists1To4Ids = orderedListIds.entrySet().stream()
                .limit(4)
                .map(e -> e.getValue()).collect(Collectors.toList());
        assertThat(lists1To4Ids, equalTo(expectedLists1To4Ids));

        final List<Payload> lists5To8 = daoFixture.getDao().search(listType, DAO.QueryCustomizer.<UUID>builder()
                .seek(DAO.Seek.builder()
                        .limit(4)
                        .lastItem(lists1To4.get(lists1To4Ids.size() - 1))
                        .build())
                .build());
        log.debug("Lists 5 to 8: {}", lists5To8.stream().map(l -> l.getAs(String.class, "name")).collect(Collectors.toList()));
        final List<UUID> lists5To8Ids = lists5To8.stream().map(l -> l.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toList());
        final List<UUID> expectedLists5To8Ids = orderedListIds.entrySet().stream()
                .skip(4)
                .limit(4)
                .map(e -> e.getValue()).collect(Collectors.toList());
        assertThat(lists5To8Ids, equalTo(expectedLists5To8Ids));

        final List<Payload> lists2Of4 = daoFixture.getDao().search(listType, DAO.QueryCustomizer.<UUID>builder()
                .filter("this.name == 'ListA' or this.name == 'ListB'")
                .seek(DAO.Seek.builder()
                        .limit(4)
                        .build())
                .build());
        log.debug("Lists 1 to 4 ['A', 'B']: {}", lists2Of4.stream().map(l -> l.getAs(String.class, "name")).collect(Collectors.toList()));
        final List<UUID> lists2Of4Ids = lists2Of4.stream().map(l -> l.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toList());
        final List<UUID> expectedLists2Of4Ids = orderedListIds.entrySet().stream()
                .filter(e -> e.getKey() == 'A' || e.getKey() == 'B')
                .limit(2)
                .map(e -> e.getValue()).collect(Collectors.toList());
        assertThat(lists2Of4Ids, equalTo(expectedLists2Of4Ids));

        final List<Payload> orderedLists1To4 = daoFixture.getDao().searchReferencedInstancesOf(apListsReference, apListsReference.getEReferenceType(), DAO.QueryCustomizer.<UUID>builder()
                .seek(DAO.Seek.builder()
                        .limit(4)
                        .build())
                .build());
        log.debug("Lists 1 to 4: {}", orderedLists1To4.stream().map(l -> l.getAs(String.class, "name")).collect(Collectors.toList()));
        final List<UUID> orderedLists1To4Ids = orderedLists1To4.stream().map(l -> l.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toList());
        final List<UUID> expectedOrderedLists1To4Ids = ImmutableList.of(
                orderedListIds.get('A'),
                orderedListIds.get('B'),
                orderedListIds.get('C'),
                orderedListIds.get('D')
        );
        assertThat(orderedLists1To4Ids, equalTo(expectedOrderedLists1To4Ids));

        final List<Payload> orderedLists5To8 = daoFixture.getDao().searchReferencedInstancesOf(apListsReference, apListsReference.getEReferenceType(), DAO.QueryCustomizer.<UUID>builder()
                .seek(DAO.Seek.builder()
                        .limit(4)
                        .lastItem(orderedLists1To4.get(orderedLists1To4.size() - 1))
                        .build())
                .build());
        log.debug("Lists 5 to 8: {}", orderedLists5To8.stream().map(l -> l.getAs(String.class, "name")).collect(Collectors.toList()));
        final List<UUID> orderedLists5To8Ids = orderedLists5To8.stream().map(l -> l.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toList());
        final List<UUID> expectedOrderedLists5To8Ids = ImmutableList.of(
                orderedListIds.get('E'),
                orderedListIds.get('F'),
                orderedListIds.get('G'),
                orderedListIds.get('H')
        );
        assertThat(orderedLists5To8Ids, equalTo(expectedOrderedLists5To8Ids));

        final List<Payload> orderedLists2Of4 = daoFixture.getDao().searchReferencedInstancesOf(apListsReference, apListsReference.getEReferenceType(), DAO.QueryCustomizer.<UUID>builder()
                .filter("this.name == 'ListA' or this.name == 'ListB'")
                .seek(DAO.Seek.builder()
                        .limit(4)
                        .build())
                .build());
        log.debug("Lists 1 to 4 ['A', 'B']: {}", orderedLists2Of4.stream().map(l -> l.getAs(String.class, "name")).collect(Collectors.toList()));
        final List<UUID> orderedLists2Of4Ids = orderedLists2Of4.stream().map(l -> l.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toList());
        final List<UUID> expectedOrderedLists2Of4Ids = ImmutableList.of(
                orderedListIds.get('A'),
                orderedListIds.get('B')
        );
        assertThat(orderedLists2Of4Ids, equalTo(expectedOrderedLists2Of4Ids));

        final List<Payload> abcLists1To4 = daoFixture.getDao().search(listType, DAO.QueryCustomizer.<UUID>builder()
                .orderBy(DAO.OrderBy.builder()
                        .attribute(nameOfListAttribute)
                        .descending(false)
                        .build())
                .seek(DAO.Seek.builder()
                        .limit(4)
                        .build())
                .build());
        log.debug("Lists 1 to 4 (ABC): {}", abcLists1To4.stream().map(l -> l.getAs(String.class, "name")).collect(Collectors.toList()));
        final List<UUID> abcLists1To4Ids = abcLists1To4.stream().map(l -> l.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toList());
        final List<UUID> expectedAbcLists1To4Ids = ImmutableList.of(
                orderedListIds.get('A'),
                orderedListIds.get('B'),
                orderedListIds.get('C'),
                orderedListIds.get('D')
        );
        assertThat(abcLists1To4Ids, equalTo(expectedAbcLists1To4Ids));

        final List<Payload> abcLists5To8 = daoFixture.getDao().search(listType, DAO.QueryCustomizer.<UUID>builder()
                .orderBy(DAO.OrderBy.builder()
                        .attribute(nameOfListAttribute)
                        .descending(false)
                        .build())
                .seek(DAO.Seek.builder()
                        .limit(4)
                        .lastItem(abcLists1To4.get(abcLists1To4.size() - 1))
                        .build())
                .build());
        log.debug("Lists 5 to 8 (ABC): {}", abcLists5To8.stream().map(l -> l.getAs(String.class, "name")).collect(Collectors.toList()));
        final List<UUID> abcLists5To8Ids = abcLists5To8.stream().map(l -> l.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toList());
        final List<UUID> expectedAbcLists5To8Ids = ImmutableList.of(
                orderedListIds.get('E'),
                orderedListIds.get('F'),
                orderedListIds.get('G'),
                orderedListIds.get('H')
        );
        assertThat(abcLists5To8Ids, equalTo(expectedAbcLists5To8Ids));

        final List<Payload> abcLists1To4Reverse = daoFixture.getDao().search(listType, DAO.QueryCustomizer.<UUID>builder()
                .orderBy(DAO.OrderBy.builder()
                        .attribute(nameOfListAttribute)
                        .descending(false)
                        .build())
                .seek(DAO.Seek.builder()
                        .limit(4)
                        .lastItem(abcLists5To8.get(0)).reverse(true)
                        .build())
                .build());
        log.debug("Lists 1 to 4 (ABC, reverse): {}", abcLists1To4Reverse.stream().map(l -> l.getAs(String.class, "name")).collect(Collectors.toList()));
        final List<UUID> abcLists1To4ReverseIds = abcLists1To4Reverse.stream().map(l -> l.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toList());
        assertThat(abcLists1To4ReverseIds, equalTo(expectedAbcLists1To4Ids));

        final List<Payload> zyxLists1To4 = daoFixture.getDao().search(listType, DAO.QueryCustomizer.<UUID>builder()
                .orderBy(DAO.OrderBy.builder()
                        .attribute(nameOfListAttribute)
                        .descending(true)
                        .build())
                .seek(DAO.Seek.builder()
                        .limit(4)
                        .build())
                .build());
        log.debug("Lists 1 to 4 (ZYX): {}", zyxLists1To4.stream().map(l -> l.getAs(String.class, "name")).collect(Collectors.toList()));
        final List<UUID> zyxLists1To4Ids = zyxLists1To4.stream().map(l -> l.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toList());
        final List<UUID> expectedZyxLists1To4Ids = ImmutableList.of(
                orderedListIds.get('Z'),
                orderedListIds.get('Y'),
                orderedListIds.get('X'),
                orderedListIds.get('W')
        );
        assertThat(zyxLists1To4Ids, equalTo(expectedZyxLists1To4Ids));

        final List<Payload> zyxLists5To8 = daoFixture.getDao().search(listType, DAO.QueryCustomizer.<UUID>builder()
                .orderBy(DAO.OrderBy.builder()
                        .attribute(nameOfListAttribute)
                        .descending(true)
                        .build())
                .seek(DAO.Seek.builder()
                        .limit(4)
                        .lastItem(zyxLists1To4.get(abcLists1To4.size() - 1))
                        .build())
                .build());
        log.debug("Lists 5 to 8 (ZYX): {}", zyxLists5To8.stream().map(l -> l.getAs(String.class, "name")).collect(Collectors.toList()));
        final List<UUID> zyxLists5To8Ids = zyxLists5To8.stream().map(l -> l.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toList());
        final List<UUID> expectedZyxLists5To8Ids = ImmutableList.of(
                orderedListIds.get('V'),
                orderedListIds.get('U'),
                orderedListIds.get('T'),
                orderedListIds.get('S')
        );
        assertThat(zyxLists5To8Ids, equalTo(expectedZyxLists5To8Ids));

        final List<Payload> zyxLists1To4Reverse = daoFixture.getDao().search(listType, DAO.QueryCustomizer.<UUID>builder()
                .orderBy(DAO.OrderBy.builder()
                        .attribute(nameOfListAttribute)
                        .descending(true)
                        .build())
                .seek(DAO.Seek.builder()
                        .limit(4)
                        .lastItem(zyxLists5To8.get(0))
                        .reverse(true)
                        .build())
                .build());
        log.debug("Lists 1 to 4 (ZYX, reverse): {}", zyxLists1To4Reverse.stream().map(l -> l.getAs(String.class, "name")).collect(Collectors.toList()));
        final List<UUID> zyxLists1To4ReverseIds = zyxLists1To4Reverse.stream().map(l -> l.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toList());
        assertThat(zyxLists1To4ReverseIds, equalTo(expectedZyxLists1To4Ids));

        final List<Payload> itemsList1To10 = daoFixture.getDao().search(listItemType, DAO.QueryCustomizer.<UUID>builder()
                .orderByList(Arrays.asList(
                        DAO.OrderBy.builder()
                                .attribute(numberOfListItemAttribute)
                                .descending(true)
                                .build(),
                        DAO.OrderBy.builder()
                                .attribute(listNameOfListItemAttribute)
                                .descending(false)
                                .build()))
                .seek(DAO.Seek.builder()
                        .limit(10)
                        .build())
                .build());
        final List<String> itemsList1To10FqNames = itemsList1To10.stream().map(l -> l.getAs(String.class, "listName") + "." + l.getAs(Integer.class, "number")).collect(Collectors.toList());
        log.debug("List items 1 to 10: {}", itemsList1To10FqNames);
        final List<String> expectedItemsList1To10FqNames = Arrays.asList("ListA.26", "ListA.25", "ListB.25", "ListA.24", "ListB.24", "ListC.24", "ListA.23", "ListB.23", "ListC.23", "ListD.23");
        assertThat(itemsList1To10FqNames, equalTo(expectedItemsList1To10FqNames));

        final List<Payload> itemsList11To20 = daoFixture.getDao().search(listItemType, DAO.QueryCustomizer.<UUID>builder()
                .orderByList(Arrays.asList(
                        DAO.OrderBy.builder()
                                .attribute(numberOfListItemAttribute)
                                .descending(true)
                                .build(),
                        DAO.OrderBy.builder()
                                .attribute(listNameOfListItemAttribute)
                                .descending(false)
                                .build()))
                .seek(DAO.Seek.builder()
                        .limit(10)
                        .lastItem(itemsList1To10.get(itemsList1To10.size() - 1))
                        .build())
                .build());
        final List<String> itemsList11To20FqNames = itemsList11To20.stream().map(l -> l.getAs(String.class, "listName") + "." + l.getAs(Integer.class, "number")).collect(Collectors.toList());
        log.debug("List items 11 to 20: {}", itemsList11To20FqNames);
        final List<String> expectedItemsList11To20FqNames = Arrays.asList("ListA.22", "ListB.22", "ListC.22", "ListD.22", "ListE.22", "ListA.21", "ListB.21", "ListC.21", "ListD.21", "ListE.21");
        assertThat(itemsList11To20FqNames, equalTo(expectedItemsList11To20FqNames));

        final List<Payload> itemsList1To10Reverse = daoFixture.getDao().search(listItemType, DAO.QueryCustomizer.<UUID>builder()
                .orderByList(Arrays.asList(
                        DAO.OrderBy.builder()
                                .attribute(numberOfListItemAttribute)
                                .descending(true)
                                .build(),
                        DAO.OrderBy.builder()
                                .attribute(listNameOfListItemAttribute)
                                .descending(false)
                                .build()))
                .seek(DAO.Seek.builder()
                        .limit(10)
                        .reverse(true)
                        .lastItem(itemsList11To20.get(0))
                        .build())
                .build());
        final List<String> itemsList1To10ReverseFqNames = itemsList1To10Reverse.stream().map(l -> l.getAs(String.class, "listName") + "." + l.getAs(Integer.class, "number")).collect(Collectors.toList());
        log.debug("List items 1 to 10 reverse: {}", itemsList1To10ReverseFqNames);
        assertThat(itemsList1To10ReverseFqNames, equalTo(expectedItemsList1To10FqNames));

        final List<Payload> itemsByTopic1To10 = daoFixture.getDao().search(listItemType, DAO.QueryCustomizer.<UUID>builder()
                .orderByList(Arrays.asList(
                        DAO.OrderBy.builder()
                                .attribute(topicOfListItemAttribute)
                                .descending(false)
                                .build(),
                        DAO.OrderBy.builder()
                                .attribute(numberOfListItemAttribute)
                                .descending(true)
                                .build(),
                        DAO.OrderBy.builder()
                                .attribute(listNameOfListItemAttribute)
                                .descending(false)
                                .build()))
                .seek(DAO.Seek.builder()
                        .limit(10)
                        .build())
                .build());
        final List<String> itemsByTopic1To10FqNames = itemsByTopic1To10.stream().map(l -> l.getAs(String.class, "listName") + "." + l.getAs(Integer.class, "number") + "#" + l.getAs(String.class, "topic")).collect(Collectors.toList());
        log.debug("List items by topic 1 to 10: {}", itemsByTopic1To10FqNames);
        final List<String> expectedItemsByTopic1To10FqNames = Arrays.asList("ListA.1#TopicA", "ListA.2#TopicB", "ListB.1#TopicB", "ListA.3#TopicC", "ListB.2#TopicC", "ListC.1#TopicC", "ListB.3#TopicD", "ListC.2#TopicD", "ListD.1#TopicD", "ListC.3#TopicE");
        assertThat(itemsByTopic1To10FqNames, equalTo(expectedItemsByTopic1To10FqNames));

        final List<Payload> itemsByTopic11To20 = daoFixture.getDao().search(listItemType, DAO.QueryCustomizer.<UUID>builder()
                .orderByList(Arrays.asList(
                        DAO.OrderBy.builder()
                                .attribute(topicOfListItemAttribute)
                                .descending(false)
                                .build(),
                        DAO.OrderBy.builder()
                                .attribute(numberOfListItemAttribute)
                                .descending(true)
                                .build(),
                        DAO.OrderBy.builder()
                                .attribute(listNameOfListItemAttribute)
                                .descending(false)
                                .build()))
                .seek(DAO.Seek.builder()
                        .limit(10)
                        .lastItem(itemsByTopic1To10.get(itemsByTopic1To10.size() - 1))
                        .build())
                .build());
        final List<String> itemsByTopic11To20FqNames = itemsByTopic11To20.stream().map(l -> l.getAs(String.class, "listName") + "." + l.getAs(Integer.class, "number") + "#" + l.getAs(String.class, "topic")).collect(Collectors.toList());
        log.debug("List items by topic 11 to 20: {}", itemsByTopic11To20FqNames);
        final List<String> expectedItemsByTopic11To20FqNames = Arrays.asList("ListD.2#TopicE", "ListD.3#TopicF", "ListA.26#null", "ListA.25#null", "ListB.25#null", "ListA.24#null", "ListB.24#null", "ListC.24#null", "ListA.23#null", "ListB.23#null");
        assertThat(itemsByTopic11To20FqNames, equalTo(expectedItemsByTopic11To20FqNames));

        final List<Payload> itemsByTopic21To30 = daoFixture.getDao().search(listItemType, DAO.QueryCustomizer.<UUID>builder()
                .orderByList(Arrays.asList(
                        DAO.OrderBy.builder()
                                .attribute(topicOfListItemAttribute)
                                .descending(false)
                                .build(),
                        DAO.OrderBy.builder()
                                .attribute(numberOfListItemAttribute)
                                .descending(true)
                                .build(),
                        DAO.OrderBy.builder()
                                .attribute(listNameOfListItemAttribute)
                                .descending(false)
                                .build()))
                .seek(DAO.Seek.builder()
                        .limit(10)
                        .lastItem(itemsByTopic11To20.get(itemsByTopic11To20.size() - 1))
                        .build())
                .build());
        final List<String> itemsByTopic21To30FqNames = itemsByTopic21To30.stream().map(l -> l.getAs(String.class, "listName") + "." + l.getAs(Integer.class, "number") + "#" + l.getAs(String.class, "topic")).collect(Collectors.toList());
        log.debug("List items by topic 21 to 30: {}", itemsByTopic21To30FqNames);
        final List<String> expectedItemsByTopic21To30FqNames = Arrays.asList("ListC.23#null", "ListD.23#null", "ListA.22#null", "ListB.22#null", "ListC.22#null", "ListD.22#null", "ListE.22#null", "ListA.21#null", "ListB.21#null", "ListC.21#null");
        assertThat(itemsByTopic21To30FqNames, equalTo(expectedItemsByTopic21To30FqNames));

        final List<Payload> itemsByTopic11To20Reverse = daoFixture.getDao().search(listItemType, DAO.QueryCustomizer.<UUID>builder()
                .orderByList(Arrays.asList(
                        DAO.OrderBy.builder()
                                .attribute(topicOfListItemAttribute)
                                .descending(false)
                                .build(),
                        DAO.OrderBy.builder()
                                .attribute(numberOfListItemAttribute)
                                .descending(true)
                                .build(),
                        DAO.OrderBy.builder()
                                .attribute(listNameOfListItemAttribute)
                                .descending(false)
                                .build()))
                .seek(DAO.Seek.builder()
                        .limit(10)
                        .reverse(true)
                        .lastItem(itemsByTopic21To30.get(0))
                        .build())
                .build());
        final List<String> itemsByTopic11To20ReverseFqNames = itemsByTopic11To20Reverse.stream().map(l -> l.getAs(String.class, "listName") + "." + l.getAs(Integer.class, "number") + "#" + l.getAs(String.class, "topic")).collect(Collectors.toList());
        log.debug("List items by topic 11 to 20 (reverse): {}", itemsByTopic11To20ReverseFqNames);
        assertThat(itemsByTopic11To20ReverseFqNames, equalTo(itemsByTopic11To20FqNames));

        final List<Payload> itemsByTopic1To10Reverse = daoFixture.getDao().search(listItemType, DAO.QueryCustomizer.<UUID>builder()
                .orderByList(Arrays.asList(
                        DAO.OrderBy.builder()
                                .attribute(topicOfListItemAttribute)
                                .descending(false)
                                .build(),
                        DAO.OrderBy.builder()
                                .attribute(numberOfListItemAttribute)
                                .descending(true)
                                .build(),
                        DAO.OrderBy.builder()
                                .attribute(listNameOfListItemAttribute)
                                .descending(false)
                                .build()))
                .seek(DAO.Seek.builder()
                        .limit(10)
                        .reverse(true)
                        .lastItem(itemsByTopic11To20.get(0))
                        .build())
                .build());
        final List<String> itemsByTopic1To10ReverseFqNames = itemsByTopic1To10Reverse.stream().map(l -> l.getAs(String.class, "listName") + "." + l.getAs(Integer.class, "number") + "#" + l.getAs(String.class, "topic")).collect(Collectors.toList());
        log.debug("List items by topic 1 to 10 (reverse): {}", itemsByTopic1To10ReverseFqNames);
        assertThat(itemsByTopic1To10ReverseFqNames, equalTo(itemsByTopic1To10FqNames));

        final List<Integer> integersFrom1To26 = IntStream.rangeClosed(1, 26).boxed().collect(Collectors.toList());
        final List<Integer> integersFrom26To1 = IntStream.rangeClosed(1, 26).boxed().sorted((i, j) -> j - i).collect(Collectors.toList());

        final List<Integer> sortedItemsOfListA = daoFixture.getDao().searchNavigationResultAt(listIds.get('A'), sortedItemsReference, null).stream()
                .map(i -> i.getAs(Integer.class, "number"))
                .collect(Collectors.toList());
        log.debug("Sorted items of ListA: {}", sortedItemsOfListA);
        assertThat(sortedItemsOfListA, equalTo(integersFrom26To1));

        final List<Integer> customSortedItemsOfListA = daoFixture.getDao().searchNavigationResultAt(listIds.get('A'), sortedItemsReference, DAO.QueryCustomizer.<UUID>builder()
                        .orderBy(DAO.OrderBy.builder()
                                .attribute(numberOfListItemAttribute)
                                .descending(false)
                                .build())
                        .build()).stream()
                .map(i -> i.getAs(Integer.class, "number"))
                .collect(Collectors.toList());
        log.debug("Custom sorted items of ListA: {}", customSortedItemsOfListA);
        assertThat(customSortedItemsOfListA, equalTo(integersFrom1To26));

        final List<Integer> customReverseSortedItemsOfListA = daoFixture.getDao().searchNavigationResultAt(listIds.get('A'), sortedItemsReference, DAO.QueryCustomizer.<UUID>builder()
                        .orderBy(DAO.OrderBy.builder()
                                .attribute(numberOfListItemAttribute)
                                .descending(true)
                                .build())
                        .build()).stream()
                .map(i -> i.getAs(Integer.class, "number"))
                .collect(Collectors.toList());
        log.debug("Custom reverse sorted items of ListA: {}", customReverseSortedItemsOfListA);
        assertThat(customReverseSortedItemsOfListA, equalTo(integersFrom26To1));
    }

    @Test
    public void testPaginationByTimestamp(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final TimestampType timestampType = newTimestampTypeBuilder().withName("Timestamp").withBaseUnit(DurationType.SECOND).build();

        final EntityType logEntry = newEntityTypeBuilder()
                .withName("LogEntry")
                .withAttributes(newDataMemberBuilder().withName("timestamp").withDataType(timestampType).withMemberType(MemberType.STORED).build())
                .withAttributes(newDataMemberBuilder().withName("message").withDataType(stringType).withMemberType(MemberType.STORED).build())
                .build();
        useEntityType(logEntry).withMapping(newMappingBuilder().withTarget(logEntry).build()).build();

        final Model model = newModelBuilder().withName(getModelName())
                .withElements(stringType, timestampType, logEntry)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass logEntryType = (EClass) daoFixture.getAsmUtils().resolve(DTO_PACKAGE + ".LogEntry").get();
        final EAttribute timestampOfLogEntryAttribute = daoFixture.getAsmUtils().resolveAttribute(DTO_PACKAGE + ".LogEntry#timestamp").get();

        final BiFunction<OffsetDateTime, String, UUID> create = (timestamp, message) -> daoFixture.getDao().create(logEntryType,
                        Payload.map("timestamp", timestamp, "message", message),
                        DAO.QueryCustomizer.<UUID>builder().withoutFeatures(true).build())
                .getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final UUID id1 = create.apply(OffsetDateTime.of(2021, 7, 29, 15, 7, 1, 111_000_000, ZoneOffset.UTC), "Message1");
        final UUID id2 = create.apply(OffsetDateTime.of(2021, 7, 29, 15, 7, 1, 110_000_000, ZoneOffset.UTC), "Message2");
        final UUID id3 = create.apply(OffsetDateTime.of(2021, 7, 29, 15, 7, 1, 100_000_000, ZoneOffset.UTC), "Message3");
        final UUID id4 = create.apply(OffsetDateTime.of(2021, 7, 29, 15, 7, 1, 0, ZoneOffset.UTC), "Message4");
        final UUID id5 = create.apply(OffsetDateTime.of(2021, 1, 29, 15, 7, 1, 111_000_000, ZoneOffset.UTC), "Message5");
        final UUID id6 = create.apply(OffsetDateTime.of(2021, 1, 29, 15, 7, 1, 110_000_000, ZoneOffset.UTC), "Message6");
        final UUID id7 = create.apply(OffsetDateTime.of(2021, 1, 29, 15, 7, 1, 100_000_000, ZoneOffset.UTC), "Message7");
        final UUID id8 = create.apply(OffsetDateTime.of(2021, 1, 29, 15, 7, 1, 0, ZoneOffset.UTC), "Message8");

        final Function<DAO.Seek, List<Payload>> search = seek -> daoFixture.getDao().search(logEntryType, DAO.QueryCustomizer.<UUID>builder()
                .orderBy(DAO.OrderBy.builder().attribute(timestampOfLogEntryAttribute).build())
                .seek(seek)
                .build());
        final Function<List<Payload>, List<UUID>> extractIds = result -> result.stream().map(e -> e.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toList());

        final List<Payload> logEntry87 = search.apply(DAO.Seek.builder().limit(2).build());
        final List<UUID> id87 = extractIds.apply(logEntry87);
        assertThat(id87, equalTo(Arrays.asList(id8, id7)));

        final List<Payload> logEntry65 = search.apply(DAO.Seek.builder().limit(2).lastItem(logEntry87.get(logEntry87.size() - 1)).build());
        final List<UUID> id65 = extractIds.apply(logEntry65);
        assertThat(id65, equalTo(Arrays.asList(id6, id5)));

        final List<Payload> logEntry43 = search.apply(DAO.Seek.builder().limit(2).lastItem(logEntry65.get(logEntry65.size() - 1)).build());
        final List<UUID> id43 = extractIds.apply(logEntry43);
        assertThat(id43, equalTo(Arrays.asList(id4, id3)));

        final List<Payload> logEntry21 = search.apply(DAO.Seek.builder().limit(2).lastItem(logEntry43.get(logEntry43.size() - 1)).build());
        final List<UUID> id21 = extractIds.apply(logEntry21);
        assertThat(id21, equalTo(Arrays.asList(id2, id1)));
    }
}
