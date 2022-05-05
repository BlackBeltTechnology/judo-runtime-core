package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.meta.psm.namespace.Model;
import hu.blackbelt.judo.services.core.UUIDIdentifierProvider;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static hu.blackbelt.judo.meta.psm.measure.util.builder.MeasureBuilders.newMeasureBuilder;
import static hu.blackbelt.judo.meta.psm.measure.util.builder.MeasureBuilders.newUnitBuilder;
import static hu.blackbelt.judo.meta.psm.namespace.util.builder.NamespaceBuilders.useModel;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class PrimitiveOperationTest {
    public static final String ENTITY_NAME = "Entity";
    public static final String FQNAME = "demo._default_transferobjecttypes.entities." + ENTITY_NAME;

    @AfterEach
    void purgeDatabase(RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    void testDateComparison(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) throws Exception {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity(ENTITY_NAME)
                //ATTRIBUTES
                .withAttribute("Date", "date1")
                .withAttribute("Date", "date2")
                .withAttribute("Date", "date3")
                .withAttribute("Date", "date4")
                .withAttribute("Date", "dateUndefined")

                //PROPERTIES: true scenarios
                .withProperty("Boolean", "isLower", "self.date2 < self.date3")
                .withProperty("Boolean", "isHigher", "self.date3 > self.date2")
                .withProperty("Boolean", "isEqual", "self.date2 == self.date2")
                .withProperty("Boolean", "isNotEqual", "self.date2 != self.date3")
                .withProperty("Boolean", "isLowerOrEqual", "self.date2 <= self.date3")
                .withProperty("Boolean", "isLowerOrEqual2", "self.date2 <= self.date2")
                .withProperty("Boolean", "isHigherOrEqual", "self.date3 >= self.date2")
                .withProperty("Boolean", "isHigherOrEqual2", "self.date3 >= self.date3")

                //PROPERTIES: false scenarios
                .withProperty("Boolean", "notLower", "self.date2 < self.date2")
                .withProperty("Boolean", "notHigher", "self.date3 > self.date3")
                .withProperty("Boolean", "notEqual", "self.date2 == self.date3")
                .withProperty("Boolean", "notNotEqual", "self.date2 != self.date2")
                .withProperty("Boolean", "notLowerOrEqual", "self.date2 <= self.date1")
                .withProperty("Boolean", "notHigherOrEqual", "self.date3 >= self.date4")

                //PROPERTIES: undefined scenarios
                .withProperty("Boolean", "isLowerUndefined", "self.date2 < self.dateUndefined")
                .withProperty("Boolean", "isHigherUndefined", "self.date3 > self.dateUndefined")
                .withProperty("Boolean", "isEqualUndefined", "self.date2 == self.dateUndefined")
                .withProperty("Boolean", "isNotEqualUndefined", "self.date2 != self.dateUndefined")
                .withProperty("Boolean", "isLowerOrEqualUndefined", "self.date2 <= self.dateUndefined")
                .withProperty("Boolean", "isHigherUndefinedOrEqualUndefined", "self.date3 >= self.dateUndefined");


        daoFixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
        Payload result = daoFixture.getDao().create(
                daoFixture.getAsmClass(FQNAME),
                Payload.map(
                        "date1", LocalDate.of(2020, 1, 1),
                        "date2", LocalDate.of(2020, 2, 1),
                        "date3", LocalDate.of(2020, 3, 1),
                        "date4", LocalDate.of(2020, 4, 1)
                ),
                null
        );

        assertTrue((Boolean) result.get("isLower"));
        assertTrue((Boolean) result.get("isHigher"));
        assertTrue((Boolean) result.get("isEqual"));
        assertTrue((Boolean) result.get("isNotEqual"));
        assertTrue((Boolean) result.get("isLowerOrEqual"));
        assertTrue((Boolean) result.get("isLowerOrEqual2"));
        assertTrue((Boolean) result.get("isHigherOrEqual"));
        assertTrue((Boolean) result.get("isHigherOrEqual2"));

        assertFalse((Boolean) result.get("notLower"));
        assertFalse((Boolean) result.get("notHigher"));
        assertFalse((Boolean) result.get("notEqual"));
        assertFalse((Boolean) result.get("notNotEqual"));
        assertFalse((Boolean) result.get("notLowerOrEqual"));
        assertFalse((Boolean) result.get("notHigherOrEqual"));

        assertNull(result.get("isLowerUndefined"));
        assertNull(result.get("isHigherUndefinedUndefined"));
        assertNull(result.get("isEqualUndefined"));
        assertNull(result.get("isNotEqualUndefined"));
        assertNull(result.get("isLowerOrEqualUndefined"));
        assertNull(result.get("isHigherUndefinedOrEqualUndefined"));
    }

    @Test
    void testTimestampComparison(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) throws Exception {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity(ENTITY_NAME)
                //ATTRIBUTES
                .withAttribute("Timestamp", "ts8")
                .withAttribute("Timestamp", "ts9")
                .withAttribute("Timestamp", "ts10")
                .withAttribute("Timestamp", "ts11")
                .withAttribute("Timestamp", "tsUndefined")

                //PROPERTIES: true scenarios
                .withProperty("Boolean", "isLower", "self.ts9 < self.ts10")
                .withProperty("Boolean", "isHigher", "self.ts10 > self.ts9")
                .withProperty("Boolean", "isEqual", "self.ts9 == self.ts9")
                .withProperty("Boolean", "isNotEqual", "self.ts9 != self.ts10")
                .withProperty("Boolean", "isLowerOrEqual", "self.ts9 <= self.ts10")
                .withProperty("Boolean", "isLowerOrEqual2", "self.ts9 <= self.ts9")
                .withProperty("Boolean", "isHigherOrEqual", "self.ts10 >= self.ts9")
                .withProperty("Boolean", "isHigherOrEqual2", "self.ts10 >= self.ts10")

                //PROPERTIES: false scenarios
                .withProperty("Boolean", "notLower", "self.ts9 < self.ts9")
                .withProperty("Boolean", "notHigher", "self.ts10 > self.ts10")
                .withProperty("Boolean", "notEqual", "self.ts9 == self.ts10")
                .withProperty("Boolean", "notNotEqual", "self.ts9 != self.ts9")
                .withProperty("Boolean", "notLowerOrEqual", "self.ts9 <= self.ts8")
                .withProperty("Boolean", "notHigherOrEqual", "self.ts10 >= self.ts11")

                //PROPERTIES: undefined scenarios
                .withProperty("Boolean", "isLowerUndefined", "self.ts9 < self.tsUndefined")
                .withProperty("Boolean", "isHigherUndefined", "self.ts10 > self.tsUndefined")
                .withProperty("Boolean", "isEqualUndefined", "self.ts9 == self.tsUndefined")
                .withProperty("Boolean", "isNotEqualUndefined", "self.ts9 != self.tsUndefined")
                .withProperty("Boolean", "isLowerOrEqualUndefined", "self.ts9 <= self.tsUndefined")
                .withProperty("Boolean", "isHigherOrEqualUndefined", "self.ts10 >= self.tsUndefined");

        daoFixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
        Payload result = daoFixture.getDao().create(
                daoFixture.getAsmClass(FQNAME),
                Payload.map(
                        "ts8", OffsetDateTime.of(2020, 1, 1, 8, 11, 12, 0, ZoneOffset.ofHours(1)),
                        "ts9", OffsetDateTime.of(2020, 1, 1, 9, 11, 12, 0, ZoneOffset.ofHours(1)),
                        "ts10", OffsetDateTime.of(2020, 1, 1, 10, 11, 12, 0, ZoneOffset.ofHours(1)),
                        "ts11", OffsetDateTime.of(2020, 1, 1, 11, 11, 12, 0, ZoneOffset.ofHours(1))
                ),
                null
        );

        assertTrue((Boolean) result.get("isLower"));
        assertTrue((Boolean) result.get("isHigher"));
        assertTrue((Boolean) result.get("isEqual"));
        assertTrue((Boolean) result.get("isNotEqual"));
        assertTrue((Boolean) result.get("isLowerOrEqual"));
        assertTrue((Boolean) result.get("isLowerOrEqual2"));
        assertTrue((Boolean) result.get("isHigherOrEqual"));
        assertTrue((Boolean) result.get("isHigherOrEqual2"));

        assertFalse((Boolean) result.get("notLower"));
        assertFalse((Boolean) result.get("notHigher"));
        assertFalse((Boolean) result.get("notEqual"));
        assertFalse((Boolean) result.get("notNotEqual"));
        assertFalse((Boolean) result.get("notLowerOrEqual"));
        assertFalse((Boolean) result.get("notHigherOrEqual"));

        assertNull(result.get("isLowerUndefined"));
        assertNull(result.get("isHigherUndefined"));
        assertNull(result.get("isEqualUndefined"));
        assertNull(result.get("isNotEqualUndefined"));
        assertNull(result.get("isLowerOrEqualUndefined"));
        assertNull(result.get("isHigherOrEqualUndefined"));
    }

    @Test
    void testLogicalOperators(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {

        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity(ENTITY_NAME)
                .withAttribute("Boolean", "boolTrue")
                .withAttribute("Boolean", "boolFalse")
                .withAttribute("Boolean", "boolUndefined")
                //AND
                .withProperty("Boolean", "trueAndTrue", "self.boolTrue and self.boolTrue")
                .withProperty("Boolean", "trueAndFalse", "self.boolTrue and self.boolFalse")
                .withProperty("Boolean", "falseAndTrue", "self.boolFalse and self.boolTrue")
                .withProperty("Boolean", "falseAndFalse", "self.boolFalse and self.boolFalse")

                .withProperty("Boolean", "trueAndUndefined", "self.boolTrue and self.boolUndefined")
                .withProperty("Boolean", "undefinedAndTrue", "self.boolUndefined and self.boolTrue")
                .withProperty("Boolean", "undefinedAndFalse", "self.boolUndefined and self.boolFalse")
                .withProperty("Boolean", "falseAndUndefined", "self.boolUndefined and self.boolFalse")
                .withProperty("Boolean", "undefinedAndUndefined", "self.boolUndefined and self.boolUndefined")
                //XOR
                .withProperty("Boolean", "trueXorTrue", "self.boolTrue xor self.boolTrue")
                .withProperty("Boolean", "trueXorFalse", "self.boolTrue xor self.boolFalse")
                .withProperty("Boolean", "falseXorTrue", "self.boolFalse xor self.boolTrue")
                .withProperty("Boolean", "falseXorFalse", "self.boolFalse xor self.boolFalse")

                .withProperty("Boolean", "trueXorUndefined", "self.boolTrue xor self.boolUndefined")
                .withProperty("Boolean", "falseXorUndefined", "self.boolFalse xor self.boolUndefined")
                .withProperty("Boolean", "undefinedXorUndefined", "self.boolUndefined xor self.boolUndefined")
                .withProperty("Boolean", "undefinedXorTrue", "self.boolUndefined xor self.boolTrue")
                .withProperty("Boolean", "undefinedXorFalse", "self.boolUndefined xor self.boolFalse")
                //OR
                .withProperty("Boolean", "trueOrTrue", "self.boolTrue or self.boolTrue")
                .withProperty("Boolean", "trueOrFalse", "self.boolTrue or self.boolFalse")
                .withProperty("Boolean", "falseOrTrue", "self.boolFalse or self.boolTrue")
                .withProperty("Boolean", "falseOrFalse", "self.boolFalse or self.boolFalse")

                .withProperty("Boolean", "trueOrUndefined", "self.boolTrue or self.boolUndefined")
                .withProperty("Boolean", "undefinedOrTrue", "self.boolUndefined or self.boolTrue")
                .withProperty("Boolean", "undefinedOrUndefined", "self.boolUndefined or self.boolUndefined")
                .withProperty("Boolean", "falseOrUndefined", "self.boolFalse or self.boolUndefined")
                .withProperty("Boolean", "undefinedOrFalse", "self.boolUndefined or self.boolFalse")
                //IMPLIES
                .withProperty("Boolean", "trueImpliesTrue", "self.boolTrue implies self.boolTrue")
                .withProperty("Boolean", "trueImpliesFalse", "self.boolTrue implies self.boolFalse")
                .withProperty("Boolean", "falseImpliesTrue", "self.boolFalse implies self.boolTrue")
                .withProperty("Boolean", "falseImpliesFalse", "self.boolFalse implies self.boolFalse")

                .withProperty("Boolean", "undefinedImpliesFalse", "self.boolUndefined implies self.boolFalse")
                .withProperty("Boolean", "undefinedImpliesTrue", "self.boolUndefined implies self.boolTrue")
                .withProperty("Boolean", "undefinedImpliesUndefined", "self.boolUndefined implies self.boolUndefined")
                .withProperty("Boolean", "falseImpliesUndefined", "self.boolFalse implies self.boolUndefined")
                .withProperty("Boolean", "trueImpliesUndefined", "self.boolTrue implies self.boolUndefined")
                //NOT
                .withProperty("Boolean", "notTrue", "not self.boolTrue")
                .withProperty("Boolean", "notFalse", "not self.boolFalse")
                .withProperty("Boolean", "notUndefined", "not self.boolUndefined")
        ;

        daoFixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
        Payload result = daoFixture.getDao().create(
                daoFixture.getAsmClass(FQNAME),
                Payload.map(
                        "boolTrue", "true",
                        "boolFalse", "false"
                ),
                null
        );

        //AND
        assertTrue((Boolean) result.get("trueAndTrue"));
        assertFalse((Boolean) result.get("trueAndFalse"));
        assertFalse((Boolean) result.get("falseAndTrue"));
        assertFalse((Boolean) result.get("falseAndFalse"));

        assertNull(result.get("trueAndUndefined"));
        assertFalse((Boolean) result.get("undefinedAndFalse"));
        assertNull(result.get("undefinedAndUndefined"));
        assertNull(result.get("undefinedAndTrue"));
        assertFalse((Boolean) result.get("falseAndUndefined"));
        //XOR
        assertFalse((Boolean) result.get("trueXorTrue"));
        assertTrue((Boolean) result.get("trueXorFalse"));
        assertTrue((Boolean) result.get("falseXorTrue"));
        assertFalse((Boolean) result.get("falseXorFalse"));

        assertNull(result.get("trueXorUndefined"));
        assertNull(result.get("undefinedXorFalse"));
        assertNull(result.get("undefinedXorUndefined"));
        assertNull(result.get("undefinedXorTrue"));
        assertNull(result.get("falseXorUndefined"));
        //OR
        assertTrue((Boolean) result.get("trueOrTrue"));
        assertTrue((Boolean) result.get("trueOrFalse"));
        assertTrue((Boolean) result.get("falseOrTrue"));
        assertFalse((Boolean) result.get("falseOrFalse"));

        assertTrue((Boolean) result.get("trueOrUndefined"));
        assertNull(result.get("undefinedOrFalse"));
        assertNull(result.get("undefinedOrUndefined"));
        assertTrue((Boolean) result.get("undefinedOrTrue"));
        assertNull(result.get("falseOrUndefined"));
        //IMPLIES
        assertTrue((Boolean) result.get("trueImpliesTrue"));
        assertFalse((Boolean) result.get("trueImpliesFalse"));
        assertTrue((Boolean) result.get("falseImpliesTrue"));
        assertTrue((Boolean) result.get("falseImpliesFalse"));

        assertNull(result.get("trueImpliesUndefined"));
        assertNull(result.get("undefinedImpliesFalse"));
        assertNull(result.get("undefinedImpliesUndefined"));
        assertTrue((Boolean) result.get("undefinedImpliesTrue"));
        assertTrue((Boolean) result.get("falseImpliesUndefined"));
        //NOT
        assertFalse((Boolean) result.get("notTrue"));
        assertTrue((Boolean) result.get("notFalse"));
        assertNull(result.get("notUndefined"));
    }

    @Test
    void testEnumComparison(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity(ENTITY_NAME)
                .withAttribute("Country", "enum1")
                .withAttribute("Country", "enum1_2")
                .withAttribute("Country", "enum2")
                .withAttribute("Country", "enum3")
                .withProperty("Boolean", "enumEquals", "self.enum1 == self.enum1_2")
                .withProperty("Boolean", "enumNotEquals", "self.enum2 != self.enum3")
        ;

        daoFixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
        Payload result = daoFixture.getDao().create(
                daoFixture.getAsmClass(FQNAME),
                Payload.map(
                        "enum1", "1", //demo::types::Countries#HU
                        "enum1_2", "1", //demo::types::Countries#HU
                        "enum2", "2", //demo::types::Countries#SK
                        "enum3", "3" //demo::types::Countries#RO
                ),
                null
        );

        assertTrue((Boolean) result.get("enumEquals"));
        assertTrue((Boolean) result.get("enumNotEquals"));
    }

    @Test
    void testStringOperations(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity(ENTITY_NAME)
                .withAttribute("String", "name")
                .withAttribute("String", "undefined")
                .withAttribute("Integer", "intUndefined")
                .withAttribute("String", "smallApple")
                .withAttribute("String", "bigApple")
                .withAttribute("String", "tree")
                //equal, not equal
                .withProperty("Boolean", "nameEquals", "self.name == self.smallApple")
                .withProperty("Boolean", "nameEqualsFalse", "self.name == self.bigApple")
                .withProperty("Boolean", "nameNotEquals", "self.name != self.tree")
                .withProperty("Boolean", "nameEqualsUndefinedNull", "self.name == self.undefined")
                .withProperty("Boolean", "nameNotEqualsUndefinedNull", "self.name != self.undefined")
                .withProperty("Boolean", "undefinedEqualsUndefined", "self.undefined == self.undefined")
                //before, after (lexicographically)
                .withProperty("Boolean", "aheadFalse", "self.name < self.name")
                .withProperty("Boolean", "aheadUndefined", "self.name < self.undefined")
                .withProperty("Boolean", "ahead", "self.name < 'pear'")
                // TODO - test collation
                .withProperty("Boolean", "behind", "'apricot' > self.name")
                .withProperty("Boolean", "behindUndefined", "self.undefined > self.name")
                .withProperty("Boolean", "behindFalse", "self.name > self.name")
                //concat
                .withProperty("String", "concat", "self.name + self.tree")
                .withProperty("String", "concatUndefined", "self.name + self.undefined")
                .withProperty("String", "concatEmpty", "'' + self.name")
                //length
                .withProperty("Integer", "length", "self.name!length()")
                .withProperty("Integer", "lengthEmpty", "''!length()")
                .withProperty("Integer", "lengthUndefined", "self.undefined!length()")
                //first, last
                .withProperty("String", "first2", "self.name!first(2)")
                .withProperty("String", "first2Empty", "'  apple'!first(2)")
                .withProperty("String", "first6", "self.name!first(6)")
                .withProperty("String", "last1", "self.name!last(1)")
                .withProperty("String", "last6", "self.name!last(6)")
                .withProperty("String", "lastUndefined", "self.name!last(self.intUndefined)")
                //position
                .withProperty("Integer", "positionValid", "self.name!position('p')")
                .withProperty("Integer", "positionNotFound", "self.name!position('o')")
                .withProperty("Integer", "positionOnUndefined", "self.undefined!position('o')")
                .withProperty("Integer", "positionOfUndefined", "self.name!position(self.undefined)")
                //lowercase, uppercase
                .withProperty("String", "lowerCase", "self.bigApple!lowerCase()")
                .withProperty("String", "lowerCaseEmpty", "''!lowerCase()")
                .withProperty("String", "lowerCaseUndefined", "self.undefined!lowerCase()")
                .withProperty("String", "upperCase", "self.smallApple!upperCase()")
                .withProperty("String", "upperCaseEmpty", "''!upperCase()")
                .withProperty("String", "upperCaseUndefined", "self.undefined!upperCase()")
                //matches
                .withProperty("Boolean", "matches", "self.name!matches('.*pl.')")
                .withProperty("Boolean", "matchesFalse", "self.name!matches('.*qwe.*')")
                .withProperty("Boolean", "matchesWithUndefined", "self.name!matches(self.undefined)")
                .withProperty("Boolean", "matchesOnUndefined", "self.undefined!matches('.*qwe.*')")
                //like
                .withProperty("Boolean", "like", "self.name!like('%pl_')")
                .withProperty("Boolean", "likeFalse", "self.name!matches('%qwe%')")
                .withProperty("Boolean", "likeWithUndefined", "self.name!like(self.undefined)")
                .withProperty("Boolean", "likeOnUndefined", "self.undefined!matches('%qwe%')")
//                .withProperty("Boolean", "ilike", "self.name!ilike('%pL_')")
//                .withProperty("Boolean", "ilikeFalse", "self.name!ilike('%qWE%')")
//                .withProperty("Boolean", "ilikeWithUndefined", "self.name!ilike(self.undefined)")
//                .withProperty("Boolean", "ilikeOnUndefined", "self.undefined!ilike('%qWE%')")
                //replace
                .withProperty("String", "replace", "self.name!replace('le', 'endix')")
                .withProperty("String", "replaceSubstringNotPresent", "self.name!replace('qwe', 'endix')")
                .withProperty("String", "replaceOnUndefined", "self.undefined!replace('le', 'endix')")
                .withProperty("String", "replaceDefinedWithUndefined", "self.name!replace('pl', self.undefined)")
                .withProperty("String", "replaceUndefinedWithDefined", "self.name!replace(self.undefined, 'endix')")
                .withProperty("String", "replaceUndefinedWithUndefined", "self.name!replace(self.undefined, self.undefined)")
                //trim
                .withProperty("String", "trim", "'    apple   '!trim()")
                .withProperty("String", "trimWithoutWhitespace", "'apple'!trim()")
                .withProperty("String", "trimEmpty", "''!trim()")
                .withProperty("String", "trimWhitespace", "'    '!trim()")
                .withProperty("String", "trimUndefined", "self.undefined!trim()")
        ;
        daoFixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
        Payload result = daoFixture.getDao().create(
                daoFixture.getAsmClass(FQNAME),
                Payload.map(
                        "tree", "tree",
                        "name", "apple",
                        "smallApple", "apple",
                        "bigApple", "APPLE"
                ),
                null
        );

        //equal, not equal
        assertTrue((Boolean) result.get("nameEquals"));
        assertFalse((Boolean) result.get("nameEqualsFalse"));
        assertTrue((Boolean) result.get("nameNotEquals"));
        assertNull(result.get("nameEqualsUndefinedNull"));
        assertNull(result.get("nameNotEqualsUndefinedNull"));
        assertNull(result.get("undefinedEqualsUndefined"));
        //before, after (lexicographically)
        assertTrue((Boolean) result.get("ahead"));
        assertFalse((Boolean) result.get("aheadFalse"));
        assertNull(result.get("aheadUndefined"));
        assertTrue((Boolean) result.get("behind"));
        assertFalse((Boolean) result.get("behindFalse"));
        assertNull(result.get("behindUndefined"));
        //concat
        assertEquals("appletree", result.get("concat"));
        assertNull(result.get("concatUndefined"));
        assertEquals("apple", result.get("concatEmpty"));
        //length
        assertEquals(5, result.get("length"));
        assertEquals(0, result.get("lengthEmpty"));
        assertNull(result.get("lengthUndefined"));
        //first, last
        assertEquals("ap", result.get("first2"));
        assertEquals("  ", result.get("first2Empty"));
        assertEquals("apple", result.get("first6"));
        assertEquals("e", result.get("last1"));
        assertEquals("apple", result.get("last6"));
        assertNull(result.get("lastUndefined"));
        //position
        assertEquals(2, result.get("positionValid"));
        assertEquals(0, result.get("positionNotFound"));
        assertNull(result.get("positionOnUndefined"));
        assertNull(result.get("positionOfUndefined"));
        //lowercase, uppercase
        assertEquals("apple", result.get("lowerCase"));
        assertEquals("", result.get("lowerCaseEmpty"));
        assertNull(result.get("lowerCaseUndefined"));
        assertEquals("APPLE", result.get("upperCase"));
        assertEquals("", result.get("upperCaseEmpty"));
        assertNull(result.get("upperCaseUndefined"));
        //matches
        assertTrue((Boolean) result.get("matches"));
        assertFalse((Boolean) result.get("matchesFalse"));
        assertNull(result.get("matchesWithUndefined"));
        assertNull(result.get("matchesOnUndefined"));
        //like
        assertTrue((Boolean) result.get("like"));
        assertFalse((Boolean) result.get("likeFalse"));
        assertNull(result.get("likeWithUndefined"));
        assertNull(result.get("likeOnUndefined"));
//        assertTrue((Boolean) result.get("ilike"));
//        assertFalse((Boolean) result.get("ilikeFalse"));
//        assertNull(result.get("ilikeWithUndefined"));
//        assertNull(result.get("ilikeOnUndefined"));
        //replace
        assertEquals("appendix", result.get("replace"));
        assertEquals("apple", result.get("replaceSubstringNotPresent"));
        assertNull(result.get("replaceOnUndefined"));
        assertNull(result.get("replaceDefinedWithUndefined"));
        assertNull(result.get("replaceUndefinedWithDefined"));
        assertNull(result.get("replaceUndefinedWithUndefined"));
        //trim
        assertEquals("apple", result.get("trim"));
        assertEquals("apple", result.get("trimWithoutWhitespace"));
        assertNull(result.get("trimUndefined"));
        assertEquals("", result.get("trimEmpty"));
        assertEquals("", result.get("trimWhitespace"));
    }

    @Test
    void testAsString(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity(ENTITY_NAME)
                .withAttribute("Gps", "location")
                .withProperty("String", "integerAsString", "123!asString()")
                .withProperty("String", "decimalAsString", "3.1415!asString()")
                .withProperty("String", "stringAsString", "'apple'!asString()")
                .withProperty("String", "dateAsString", "`2021-02-26`!asString()")
                .withProperty("String", "timestampAsString", "`2019-01-02T03:04:05.678+06:00`!asString()")
                .withProperty("String", "booleanAsString", "true!asString()")
                .withProperty("String", "enumAsString", "demo::types::Countries#RO!asString()")
                .withProperty("String", "customAsString", "self.location!asString()")
                .withProperty("String", "measuredAsString", "20 [m]!asString()")
                .withProperty("String", "measured2AsString", "6.28318530718 [m]!asString()")
        ;
        Model model = modelBuilder.build();
        useModel(model)
                .withElements(newMeasureBuilder()
                        .withName("Length")
                        .withUnits(newUnitBuilder()
                                .withName("metre")
                                .withSymbol("m")
                                .withRateDividend(1.0)
                                .withRateDivisor(1.0)
                                .build())
                        .build())
                .build();
        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
        Payload result = daoFixture.getDao().create(
                daoFixture.getAsmClass(FQNAME),
                Payload.empty(),
                null
        );
        assertEquals("123", result.get("integerAsString"));
        assertEquals("3.1415", result.get("decimalAsString"));
        assertEquals("apple", result.get("stringAsString"));
        assertEquals("2021-02-26", result.get("dateAsString"));
//        assertEquals(OffsetDateTime.of(2019, 1, 2, 3, 4, 5, 678000000, ZoneOffset.ofHours(6)).atZoneSameInstant(ZoneOffset.UTC),
//                OffsetDateTime.parse((String) result.get("timestampAsString")).atZoneSameInstant(ZoneOffset.UTC)); // TODO - not supported by hsqldb
        assertEquals("true", result.getAs(String.class, "booleanAsString").toLowerCase());
        assertEquals("RO", result.get("enumAsString"));
        //assertEquals("123,456", result.get("customAsString"));
        assertEquals("20", new BigDecimal((String) result.get("measuredAsString")).stripTrailingZeros().toPlainString());
        assertEquals("6.28318530718", new BigDecimal((String) result.get("measured2AsString")).stripTrailingZeros().toPlainString());
    }

    @Test
    void testMetricMass(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity(ENTITY_NAME)
                .withAttribute("MassStoredInKilograms", "kilograms")
                .withAttribute("MassStoredInGrams", "grams")
                .withProperty("Double", "differentUnitsRatio", "30[kg]/500[g]")
                .withProperty("Boolean", "massLessThan", "self.grams < self.kilograms")
                .withProperty("Boolean", "massEquals", "self.kilograms * 30 == self.grams")
                .withProperty("Boolean", "massEquals2", "self.kilograms == self.grams / 30 * 1000")
                .withProperty("Boolean", "massEqualsLiteral", "self.kilograms * 30 == 30000[g]");

        daoFixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
        Payload result = daoFixture.getDao().create(
                daoFixture.getAsmClass(FQNAME),
                Payload.map(
                        "kilograms", "1", //[demo::measures::Mass#kg]
                        "grams", "30" //[demo::measures::Mass#g]
                ),
                null
        );

        assertFalse(result.getAs(Boolean.class, "massEquals"));
        assertTrue(result.getAs(Boolean.class, "massEquals2"));
        assertTrue(result.getAs(Boolean.class, "massLessThan"));
        assertTrue(result.getAs(Boolean.class, "massEqualsLiteral"));
        assertThat(result.getAs(Double.class, "differentUnitsRatio"), is(60.0));
    }

    @Test
    public void testPrecedence(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity(ENTITY_NAME)
                .withAttribute("Double", "value3")
                .withAttribute("Double", "value7")
                .withAttribute("String", "string")
                .withAttribute("String", "apple")
                .withProperty("Double", "normalPrecedent", "self.value3 + self.value7 * self.value3")
                .withProperty("Double", "withParentheses", "(self.value3 + self.value7) * self.value3")
                .withProperty("String", "concatTrim", "(self.apple + (' ' + self.string))!trim()")
                .withProperty("Boolean", "impliesOrXorAnd", "true implies false or false xor true and true")
        ;

        daoFixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        Payload result = daoFixture.getDao().create(
                daoFixture.getAsmClass(FQNAME),
                Payload.map(
                        "value3", 3,
                        "value7", 7,
                        "string", "string",
                        "apple", "apple"
                ),
                null
        );

        assertEquals(24.0, result.get("normalPrecedent"));
        assertEquals(30.0, result.get("withParentheses"));
        assertEquals("apple string", result.get("concatTrim"));
        assertTrue((Boolean) result.get("impliesOrXorAnd"));
    }

    @Test
    void testNumericOperations(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity(ENTITY_NAME)
                .withAttribute("Integer", "one")
                .withAttribute("Integer", "ten")
                .withAttribute("Integer", "undefined")
                .withAttribute("Double", "doubleUndefined")
                //homogeneous relations
                .withProperty("Boolean", "lesserThan", "self.one < self.ten")
                .withProperty("Boolean", "lesserThanUndefined", "self.one < self.undefined")
                .withProperty("Boolean", "greaterThanFalse", "0 > self.one")
                .withProperty("Boolean", "greaterThanUndefined", "0 > self.undefined")
                .withProperty("Boolean", "numericEquals", "1.0 == self.one")
                .withProperty("Boolean", "numericEqualsUndefined", "self.one == self.undefined")
                .withProperty("Boolean", "numericUndefinedEqualsUndefined", "self.undefined == self.undefined")
                .withProperty("Boolean", "numericUndefinedNotEqualsUndefined", "self.undefined != self.undefined")
                .withProperty("Boolean", "numericNotEquals", "0.9999 != self.one")
                .withProperty("Boolean", "numericNotEqualsUndefined", "self.one != self.undefined")
                .withProperty("Boolean", "lesserOrEqualsFalse", "9 <= 8")
                .withProperty("Boolean", "lesserOrEquals", "self.ten <= self.ten")
                .withProperty("Boolean", "lesserOrEqualsUndefined", "self.undefined <= self.undefined")
                .withProperty("Boolean", "greaterOrEqualsUndefined", "self.undefined >= self.undefined")
                .withProperty("Boolean", "greaterOrEqualsFalse", "self.one >= self.ten")
                //arithmetic
                .withProperty("Integer", "addition", "self.one + 2")
                .withProperty("Integer", "additionUndefined", "self.one + self.undefined")
                .withProperty("Integer", "subtraction", "2 - 3")
                .withProperty("Integer", "subtractionUndefined", "self.undefined - 3")
                .withProperty("Double", "multiplication", "2 * 2 * 3.14")
                .withProperty("Double", "multiplicationUndefined", "self.undefined * 2 * 3.14")
                .withProperty("Double", "division", "9.0 / 2")
                .withProperty("Double", "divisionScale4", "1.0000 / 3")
                .withProperty("Double", "divisionOfUndefined", "self.doubleUndefined / 2")
                .withProperty("Double", "divisionByUndefined", "9.0 / self.undefined")
                .withProperty("Integer", "div", "9 div 2")
                .withProperty("Integer", "divOfUndefined", "self.undefined div 2")
                .withProperty("Integer", "divByUndefined", "9 div self.undefined")
                .withProperty("Integer", "mod", "9 mod 2")
                .withProperty("Integer", "modOfUndefined", "self.undefined mod self.one")
                .withProperty("Integer", "modByUndefined", "self.ten mod self.undefined")
                .withProperty("Integer", "opposite", "self.ten + -self.ten")
                .withProperty("Integer", "oppositeOfZero", "0 + -0")
                .withProperty("Integer", "oppositeOfUndefined", "-self.undefined")
                //ROUND
                .withProperty("Integer", "roundZero", "0.0!round()")
                .withProperty("Integer", "round1", "1.23!round()")
                .withProperty("Integer", "round2", "7.89!round()")
                .withProperty("Integer", "round3", "2.50!round()")
                .withProperty("Integer", "round4", "-2.5!round()")
                .withProperty("Integer", "round5", "-1.23!round()")
                .withProperty("Integer", "round6", "-7.83!round()")
                .withProperty("Integer", "roundDefined", "self.doubleUndefined!round()")
        ;

        daoFixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
        Payload result = daoFixture.getDao().create(
                daoFixture.getAsmClass(FQNAME),
                Payload.map(
                        "one", "1",
                        "ten", "10"
                ),
                null
        );

        assertTrue((Boolean) result.get("lesserThan"));
        assertNull(result.get("lesserThanUndefined"));
        assertFalse((Boolean) result.get("greaterThanFalse"));
        assertNull(result.get("greaterThanUndefined"));
        assertTrue((Boolean) result.get("numericEquals"));
        assertNull(result.get("numericEqualsUndefined"));
        assertTrue((Boolean) result.get("numericNotEquals"));
        assertNull(result.get("numericNotEqualsUndefined"));
        assertNull(result.get("numericUndefinedEqualsUndefined"));
        assertNull(result.get("numericUndefinedNotEqualsUndefined"));
        assertFalse((Boolean) result.get("lesserOrEqualsFalse"));
        assertTrue((Boolean) result.get("lesserOrEquals"));
        assertNull(result.get("lesserOrEqualsUndefined"));
        assertFalse((Boolean) result.get("greaterOrEqualsFalse"));
        assertNull(result.get("greaterOrEqualsUndefined"));

        assertEquals(3, result.get("addition"));
        assertNull(result.get("additionUndefined"));
        assertEquals(-1, result.get("subtraction"));
        assertNull(result.get("subtractionUndefined"));
        assertEquals(12.56, result.get("multiplication"));
        assertNull(result.get("multiplicationUndefined"));
        assertEquals(4.5, result.get("division"));
        assertEquals(0.3333, result.get("divisionScale4"));
        assertNull(result.get("divisionByUndefined"));
        assertNull(result.get("divisionOfUndefined"));
        assertEquals(4, result.get("div"));
        assertNull(result.get("divOfUndefined"));
        assertNull(result.get("divByUndefined"));
        assertEquals(1, result.get("mod"));
        assertNull(result.get("modOfUndefined"));
        assertNull(result.get("modByUndefined"));
        assertEquals(0, result.get("opposite"));
        assertEquals(0, result.get("oppositeOfZero"));
        assertNull(result.get("oppositeOfUndefined"));

        assertEquals(0, result.get("roundZero"));
        assertEquals(1, result.get("round1"));
        assertEquals(8, result.get("round2"));
        assertEquals(3, result.get("round3"));
        assertEquals(-3, result.get("round4"));
        assertEquals(-1, result.get("round5"));
        assertEquals(-8, result.get("round6"));
        assertNull(result.get("roundUndefined"));
    }

    @Test
    void testSetNullValueRemovedFromPayload(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity(ENTITY_NAME)
                .withAttribute("String", "name")
                .withAttribute("String", "description")
        ;
        daoFixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        Payload savedEntity = daoFixture.getDao().create(
                daoFixture.getAsmClass(FQNAME),
                Payload.map(
                        "name", "apple",
                        "description", "10"
                ),
                null
        );
        final UUID id = savedEntity.getAs(new UUIDIdentifierProvider().getType(), new UUIDIdentifierProvider().getName());
        savedEntity.put("description", null);

        daoFixture.getDao().update(
                daoFixture.getAsmClass(FQNAME),
                savedEntity,
                null
        );

        Payload updateEntity = daoFixture.getDao().getByIdentifier(daoFixture.getAsmClass(FQNAME), id).get();

        assertNull(updateEntity.getAs(String.class, "description"));
    }

    @Test
    void testSetNullValueDirectly(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity(ENTITY_NAME)
                .withAttribute("String", "name")
                .withAttribute("String", "description")
        ;
        daoFixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        Payload savedEntity = daoFixture.getDao().create(
                daoFixture.getAsmClass(FQNAME),
                Payload.map(
                        "name", "apple",
                        "description", "10"
                ),
                null
        );
        final UUID id = savedEntity.getAs(new UUIDIdentifierProvider().getType(), new UUIDIdentifierProvider().getName());
        savedEntity.put("description", null);

        daoFixture.getDao().update(
                daoFixture.getAsmClass(FQNAME),
                savedEntity,
                null
        );

        Payload updateEntity = daoFixture.getDao().getByIdentifier(daoFixture.getAsmClass(FQNAME), id).get();

        assertNull(updateEntity.getAs(String.class, "description"));
    }
}
