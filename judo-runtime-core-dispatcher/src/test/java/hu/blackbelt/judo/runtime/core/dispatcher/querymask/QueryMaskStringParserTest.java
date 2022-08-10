package hu.blackbelt.judo.runtime.core.dispatcher.querymask;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 * 
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 * 
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import com.google.common.collect.ImmutableMap;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.eclipse.emf.ecore.util.builder.EcoreBuilders.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class QueryMaskStringParserTest {

    @Test
    void testParser() {
        final EDataType stringType = newEDataTypeBuilder()
                .withName("EString")
                .withInstanceClassName(String.class.getName())
                .build();
        final EDataType integerType = newEDataTypeBuilder()
                .withName("EIntegerObject")
                .withInstanceClassName(Integer.class.getName())
                .build();

        final EClass subSubType = newEClassBuilder()
                .withName("SubSubType")
                .withEStructuralFeatures(newEAttributeBuilder()
                        .withName("subSubStringAttribute")
                        .withEType(stringType)
                        .build())
                .withEStructuralFeatures(newEAttributeBuilder()
                        .withName("subSubIntegerAttribute")
                        .withEType(integerType)
                        .build())
                .build();

        final EClass subType = newEClassBuilder()
                .withName("SubType")
                .withEStructuralFeatures(newEAttributeBuilder()
                        .withName("subStringAttribute")
                        .withEType(stringType)
                        .build())
                .withEStructuralFeatures(newEAttributeBuilder()
                        .withName("subIntegerAttribute")
                        .withEType(integerType)
                        .build())
                .withEStructuralFeatures(newEReferenceBuilder()
                        .withName("subSingleReference")
                        .withLowerBound(0).withUpperBound(1)
                        .withEType(subSubType)
                        .build())
                .withEStructuralFeatures(newEReferenceBuilder()
                        .withName("subManyReference")
                        .withLowerBound(0).withUpperBound(-1)
                        .withEType(subSubType)
                        .build())
                .build();

        final EClass rootType = newEClassBuilder()
                .withName("Root")
                .withEStructuralFeatures(newEAttributeBuilder()
                        .withName("stringAttribute")
                        .withEType(stringType)
                        .build())
                .withEStructuralFeatures(newEAttributeBuilder()
                        .withName("integerAttribute")
                        .withEType(integerType)
                        .build())
                .withEStructuralFeatures(newEReferenceBuilder()
                        .withName("singleReference")
                        .withLowerBound(0).withUpperBound(1)
                        .withEType(subType)
                        .build())
                .withEStructuralFeatures(newEReferenceBuilder()
                        .withName("manyReference")
                        .withLowerBound(0).withUpperBound(-1)
                        .withEType(subType)
                        .build())
                .build();

        newEPackageBuilder()
                .withName("Package")
                .withEClassifiers(stringType, integerType, rootType, subType, subSubType)
                .build();

        assertThat(QueryMaskStringParser.parseQueryMask(rootType, null), nullValue());

        final String emptyMaskString = "{}";
        final Map<String, Object> emptyMask = QueryMaskStringParser.parseQueryMask(rootType, emptyMaskString);
        assertThat(emptyMask, equalTo(Collections.emptyMap()));

        final String singleAttributeMaskString = "{stringAttribute}";
        final Map<String, Object> singleAttributeMask = QueryMaskStringParser.parseQueryMask(rootType, singleAttributeMaskString);
        assertThat(singleAttributeMask, equalTo(ImmutableMap.of(
                "stringAttribute", true
        )));

        final String allAttributesMaskString = "{" +
                "    stringAttribute," +
                "    integerAttribute" +
                "}";
        final Map<String, Object> allAttributesMask = QueryMaskStringParser.parseQueryMask(rootType, allAttributesMaskString);
        assertThat(allAttributesMask, equalTo(ImmutableMap.of(
                "stringAttribute", true,
                "integerAttribute", true
        )));

        final String allAttributesAndReferenceWithoutFeaturesMaskString = "{" +
                "    stringAttribute," +
                "    integerAttribute," +
                "    singleReference {}," +
                "    manyReference {}" +
                "}";
        final Map<String, Object> allAttributesAndReferenceWithoutFeaturesMask = QueryMaskStringParser.parseQueryMask(rootType, allAttributesAndReferenceWithoutFeaturesMaskString);
        assertThat(allAttributesAndReferenceWithoutFeaturesMask, equalTo(ImmutableMap.of(
                "stringAttribute", true,
                "integerAttribute", true,
                "singleReference", ImmutableMap.of(),
                "manyReference", ImmutableMap.of()
        )));

        final String fullMaskString = "{" +
                "    stringAttribute," +
                "    integerAttribute," +
                "    singleReference {" +
                "        subStringAttribute," +
                "        subIntegerAttribute," +
                "        subSingleReference {" +
                "            subSubStringAttribute," +
                "            subSubIntegerAttribute" +
                "        }," +
                "        subManyReference {" +
                "            subSubStringAttribute," +
                "            subSubIntegerAttribute" +
                "        }" +
                "    }," +
                "    manyReference {" +
                "        subStringAttribute," +
                "        subIntegerAttribute," +
                "        subSingleReference {" +
                "            subSubStringAttribute," +
                "            subSubIntegerAttribute" +
                "        }," +
                "        subManyReference {" +
                "            subSubStringAttribute," +
                "            subSubIntegerAttribute" +
                "        }" +
                "    }" +
                "}";
        final Map<String, Object> fullMask = QueryMaskStringParser.parseQueryMask(rootType, fullMaskString);
        assertThat(fullMask, equalTo(ImmutableMap.of(
                "stringAttribute", true,
                "integerAttribute", true,
                "singleReference", ImmutableMap.of(
                        "subStringAttribute", true,
                        "subIntegerAttribute", true,
                        "subSingleReference", ImmutableMap.of(
                                "subSubStringAttribute", true,
                                "subSubIntegerAttribute", true
                        ),
                        "subManyReference", ImmutableMap.of(
                                "subSubStringAttribute", true,
                                "subSubIntegerAttribute", true
                        )
                ),
                "manyReference", ImmutableMap.of(
                        "subStringAttribute", true,
                        "subIntegerAttribute", true,
                        "subSingleReference", ImmutableMap.of(
                                "subSubStringAttribute", true,
                                "subSubIntegerAttribute", true
                        ),
                        "subManyReference", ImmutableMap.of(
                                "subSubStringAttribute", true,
                                "subSubIntegerAttribute", true
                        )
                )
        )));
    }
}
