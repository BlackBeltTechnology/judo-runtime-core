package hu.blackbelt.judo.runtime.core.dispatcher;

/*-
 * #%L
 * JUDO Runtime Core :: Dispatcher
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set
 * forth in the Eclipse Public License, v. 2.0 are satisfied:
 * GNU General Public License, version 2
 * with the GNU Classpath exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.asm.support.AsmModelResourceSupport;
import hu.blackbelt.mapper.api.Coercer;
import org.eclipse.emf.ecore.EAnnotation;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.eclipse.emf.ecore.util.builder.EcoreBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Characterization tests pinning which response keys {@link ResponseConverter} keeps or removes.
 */
class ResponseConverterTest {

    private EClass type;
    private AsmModel asmModel;
    private Coercer coercer;
    private ResponseConverter converter;

    @BeforeEach
    void setUp() {
        final EDataType stringType = newEDataTypeBuilder()
                .withName("String").withInstanceClassName("java.lang.String").build();

        final EAttribute name = newEAttributeBuilder().withName("name").withEType(stringType).build();
        // "name" declares "_name_default" as its default-value carrier: that feature must never be returned
        final EAnnotation defaultAnnotation = newEAnnotationBuilder()
                .withSource(AsmUtils.getAnnotationUri("default"))
                .build();
        defaultAnnotation.getDetails().put(AsmUtils.EXTENDED_METADATA_DETAILS_VALUE_KEY, "_name_default");
        name.getEAnnotations().add(defaultAnnotation);
        final EAttribute nameDefault = newEAttributeBuilder().withName("_name_default").withEType(stringType).build();
        final EAttribute code = newEAttributeBuilder().withName("code").withEType(stringType).build();

        type = newEClassBuilder().withName("Partner")
                .withEStructuralFeatures(name, nameDefault, code)
                .build();

        asmModel = AsmModel.buildAsmModel()
                .resourceSet(AsmModelResourceSupport.createAsmResourceSet())
                .build();
        asmModel.getAsmModelResourceSupport().addContent(newEPackageBuilder()
                .withName("test").withNsPrefix("test").withNsURI("http://blackbelt.hu/test/responseconverter")
                .withEClassifiers(stringType, type)
                .build());

        coercer = mock(Coercer.class);
        when(coercer.coerce(any(), anyString())).thenAnswer(inv -> inv.getArgument(0));

        converter = converterFor(type, asmModel);
    }

    private ResponseConverter converterFor(final EClass transferObjectType, final AsmModel model) {
        return ResponseConverter.builder()
                .transferObjectType(transferObjectType)
                .asmModel(model)
                .coercer(coercer)
                .keepProperty("__identifier")
                .build();
    }

    private Payload convert(final Map<String, Object> input) {
        return converter.convert(new HashMap<>(input)).orElseThrow();
    }

    @Test
    void keepsDeclaredFeatures() {
        final Payload result = convert(Map.of("name", "Acme", "code", "C1"));

        assertThat(result.get("name"), equalTo("Acme"));
        assertThat(result.get("code"), equalTo("C1"));
    }

    @Test
    void removesUnknownKeys() {
        final Payload result = convert(Map.of("name", "Acme", "unknown", "x"));

        assertThat(result.containsKey("unknown"), is(false));
        assertThat(result.get("name"), equalTo("Acme"));
    }

    @Test
    void removesNullValues() {
        final Map<String, Object> input = new HashMap<>();
        input.put("name", "Acme");
        input.put("code", null);

        final Payload result = convert(input);

        assertThat(result.containsKey("code"), is(false));
    }

    @Test
    void keepsKeepPropertiesEvenWhenNotAFeature() {
        final Payload result = convert(Map.of("name", "Acme", "__identifier", "id-1"));

        assertThat(result.get("__identifier"), equalTo("id-1"));
    }

    @Test
    void removesNullKeepProperty() {
        final Map<String, Object> input = new HashMap<>();
        input.put("name", "Acme");
        input.put("__identifier", null);

        final Payload result = convert(input);

        assertThat(result.containsKey("__identifier"), is(false));
    }

    @Test
    void removesFeatureReferencedAsDefaultOfAnotherFeature() {
        final Payload result = convert(Map.of("name", "Acme", "_name_default", "Fallback"));

        assertThat(result.containsKey("_name_default"), is(false));
        assertThat(result.get("name"), equalTo("Acme"));
    }

    @Test
    void appliesSameRulesOnRepeatedConversions() {
        convert(Map.of("name", "First", "unknown", "x"));
        final Payload second = convert(Map.of("name", "Second", "_name_default", "y", "unknown", "z", "code", "C2"));

        assertThat(second.keySet(), containsInAnyOrder("name", "code"));
    }

    @Test
    void appliesTheNestedTypesOwnRulesToEmbeddedPayloads() {
        final EDataType stringType = newEDataTypeBuilder()
                .withName("String").withInstanceClassName("java.lang.String").build();
        final EAttribute street = newEAttributeBuilder().withName("street").withEType(stringType).build();
        street.getEAnnotations().add(defaultAnnotation("_street_default"));
        final EClass address = newEClassBuilder().withName("Address")
                .withEStructuralFeatures(street,
                        newEAttributeBuilder().withName("_street_default").withEType(stringType).build())
                .build();
        final EReference headquarters = newEReferenceBuilder().withName("headquarters").withEType(address).build();
        headquarters.getEAnnotations().add(newEAnnotationBuilder()
                .withSource(AsmUtils.getAnnotationUri("embedded")).build());
        final EClass partner = newEClassBuilder().withName("Partner")
                .withEStructuralFeatures(newEAttributeBuilder().withName("name").withEType(stringType).build(), headquarters)
                .build();
        final AsmModel model = AsmModel.buildAsmModel()
                .resourceSet(AsmModelResourceSupport.createAsmResourceSet())
                .build();
        model.getAsmModelResourceSupport().addContent(newEPackageBuilder()
                .withName("nested").withNsPrefix("nested").withNsURI("http://blackbelt.hu/test/responseconverter/nested")
                .withEClassifiers(stringType, address, partner)
                .build());

        final Map<String, Object> nested = new HashMap<>();
        nested.put("street", "Main");
        nested.put("_street_default", "Fallback");
        nested.put("name", "not an Address feature");
        nested.put("unknown", "x");
        final Map<String, Object> input = new HashMap<>();
        input.put("name", "Acme");
        input.put("headquarters", nested);

        final Payload result = converterFor(partner, model).convert(input).orElseThrow();

        assertThat(result.keySet(), containsInAnyOrder("name", "headquarters"));
        assertThat(result.getAsPayload("headquarters").keySet(), containsInAnyOrder("street"));
    }

    private static EAnnotation defaultAnnotation(final String companionName) {
        final EAnnotation annotation = newEAnnotationBuilder()
                .withSource(AsmUtils.getAnnotationUri("default"))
                .build();
        annotation.getDetails().put(AsmUtils.EXTENDED_METADATA_DETAILS_VALUE_KEY, companionName);
        return annotation;
    }
}
