package hu.blackbelt.judo.runtime.core.validator;

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

import com.google.common.collect.ImmutableList;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dao.api.ValidationResult;
import org.eclipse.emf.ecore.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static hu.blackbelt.judo.runtime.core.validator.Validator.*;
import static org.eclipse.emf.ecore.util.builder.EcoreBuilders.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PrecisionValidatorTest {
    private static final String PRECISION_CONSTRAINT_NAME = "precision";
    private static final String SCALE_CONSTRAINT_NAME = "scale";
    private static final String BIG_DECIMAL_ATTRIBUTE = "bigDecimalAttr";
    private static final String FLOAT_ATTRIBUTE = "floatAttr";
    private static final String DOUBLE_ATTRIBUTE = "doubleAttr";

    private EClass eClass;

    private PrecisionValidator validator;

    @BeforeEach
    void init() {
        EcorePackage ecore = EcorePackage.eINSTANCE;

        EPackage ePackage = newEPackageBuilder()
                .withName("TestEpackage")
                .withNsPrefix("test")
                .withNsURI("http:///com.example.test.ecore")
                .build();

        final EAnnotation bigDecimalAttrAnnotation = newEAnnotationBuilder()
                .withSource("http://blackbelt.hu/judo/meta/ExtendedMetadata/constraints")
                .build();
        bigDecimalAttrAnnotation.getDetails().put(PRECISION_CONSTRAINT_NAME, "6");
        bigDecimalAttrAnnotation.getDetails().put(SCALE_CONSTRAINT_NAME, "2");

        final EAnnotation doubleAnnotation = newEAnnotationBuilder()
                .withSource("http://blackbelt.hu/judo/meta/ExtendedMetadata/constraints")
                .build();
        doubleAnnotation.getDetails().put(PRECISION_CONSTRAINT_NAME, "6");
        doubleAnnotation.getDetails().put(SCALE_CONSTRAINT_NAME, "2");

        final EAnnotation floatAnnotation = newEAnnotationBuilder()
                .withSource("http://blackbelt.hu/judo/meta/ExtendedMetadata/constraints")
                .build();
        floatAnnotation.getDetails().put(PRECISION_CONSTRAINT_NAME, "6");
        floatAnnotation.getDetails().put(SCALE_CONSTRAINT_NAME, "2");

        eClass = newEClassBuilder()
                .withName("PrecisionValidatorTestClass")
                .withEStructuralFeatures(
                        ImmutableList.of(
                                newEAttributeBuilder()
                                        .withName(BIG_DECIMAL_ATTRIBUTE)
                                        .withEType(ecore.getEBigDecimal())
                                        .withEAnnotations(bigDecimalAttrAnnotation)
                                        .build(),
                                newEAttributeBuilder()
                                        .withName(DOUBLE_ATTRIBUTE)
                                        .withEType(ecore.getEDouble())
                                        .withEAnnotations(doubleAnnotation)
                                        .build(),
                                newEAttributeBuilder()
                                        .withName(FLOAT_ATTRIBUTE)
                                        .withEType(ecore.getEFloat())
                                        .withEAnnotations(floatAnnotation)
                                        .build()
                        )
                )
                .build();

        ePackage.getEClassifiers().add(eClass);

        validator = new PrecisionValidator();
    }

    @Test
    void testValidateBigDecimalWithoutScale() {
        BigDecimal value = BigDecimal.valueOf(1234);
        Map<String, Object> raw = new HashMap<>() {{
            put(BIG_DECIMAL_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(BIG_DECIMAL_ATTRIBUTE), value, raw);

        assertEquals(0, results.size());
    }

    @Test
    void testValidateBigDecimalWithScale() {
        BigDecimal value = BigDecimal.valueOf(1234.56);
        Map<String, Object> raw = new HashMap<>() {{
            put(BIG_DECIMAL_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(BIG_DECIMAL_ATTRIBUTE), value, raw);

        assertEquals(0, results.size());
    }

    @Test
    void testValidateBigDecimalPrecisionOverflow() {
        BigDecimal value = BigDecimal.valueOf(1234567);
        Map<String, Object> raw = new HashMap<>() {{
            put(BIG_DECIMAL_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(BIG_DECIMAL_ATTRIBUTE), value, raw);
        ValidationResult result = results.stream().findFirst().get();

        assertEquals(1, results.size());
        assertEquals(ERROR_PRECISION_VALIDATION_FAILED, result.getCode());
        assertEquals("TestEpackage_PrecisionValidatorTestClass_bigDecimalAttr", result.getDetails().get(FEATURE_KEY));
        assertEquals(6, result.getDetails().get(PRECISION_CONSTRAINT_NAME));
        assertEquals(value, result.getDetails().get(VALUE_KEY));
    }

    @Test
    void testValidateBigDecimalWholeNumberWithPrecisionOverflow() {
        BigDecimal value = BigDecimal.valueOf(1234560);

        Map<String, Object> raw = new HashMap<>() {{
            put(BIG_DECIMAL_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(BIG_DECIMAL_ATTRIBUTE), value, raw);

        assertEquals(1, results.size());
        assertEquals(1, (int) results.stream().filter(r -> r.getCode().equals(ERROR_PRECISION_VALIDATION_FAILED)).count());
    }

    @Test
    void testValidateBigDecimalScaleOverflow() {
        BigDecimal value = BigDecimal.valueOf(1.23456);
        Map<String, Object> raw = new HashMap<>() {{
            put(BIG_DECIMAL_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(BIG_DECIMAL_ATTRIBUTE), value, raw);
        ValidationResult result = results.stream().findFirst().get();

        assertEquals(1, results.size());
        assertEquals(ERROR_SCALE_VALIDATION_FAILED, result.getCode());
        assertEquals("TestEpackage_PrecisionValidatorTestClass_bigDecimalAttr", result.getDetails().get(FEATURE_KEY));
        assertEquals(2, result.getDetails().get(SCALE_CONSTRAINT_NAME));
        assertEquals(value, result.getDetails().get(VALUE_KEY));
    }

    @Test
    void testValidateBigDecimalPrecisionAndScaleError() {
        BigDecimal value = BigDecimal.valueOf(12.3456789);
        Map<String, Object> raw = new HashMap<>() {{
            put(BIG_DECIMAL_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(BIG_DECIMAL_ATTRIBUTE), value, raw);

        assertEquals(2, results.size());
        assertEquals(1, (int) results.stream().filter(r -> r.getCode().equals(ERROR_PRECISION_VALIDATION_FAILED)).count());
        assertEquals(1, (int) results.stream().filter(r -> r.getCode().equals(ERROR_SCALE_VALIDATION_FAILED)).count());
    }

    @Test
    void testValidateFloatWithoutScale() {
        Float value = 1234f;
        Map<String, Object> raw = new HashMap<>() {{
            put(FLOAT_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(FLOAT_ATTRIBUTE), value, raw);

        assertEquals(0, results.size());
    }

    @Test
    void testValidateFloatWithScale() {
        Float value = 1234.56f;
        Map<String, Object> raw = new HashMap<>() {{
            put(FLOAT_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(FLOAT_ATTRIBUTE), value, raw);

        assertEquals(0, results.size());
    }


    @Test
    void testValidateFloatWithPrecisionOverflow() {
        Float value = 1234567f;
        Map<String, Object> raw = new HashMap<>() {{
            put(FLOAT_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(BIG_DECIMAL_ATTRIBUTE), value, raw);

        assertEquals(1, results.size());
        assertEquals(1, (int) results.stream().filter(r -> r.getCode().equals(ERROR_PRECISION_VALIDATION_FAILED)).count());
    }

    @Test
    void testValidateFloatWholeNumberWithPrecisionOverflow() {
        Float value = 1234560f;
        Map<String, Object> raw = new HashMap<>() {{
            put(FLOAT_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(FLOAT_ATTRIBUTE), value, raw);

        assertEquals(1, results.size());
        assertEquals(1, (int) results.stream().filter(r -> r.getCode().equals(ERROR_PRECISION_VALIDATION_FAILED)).count());
    }

    @Test
    void testValidateFloatWithScaleOverflow() {
        Float value = 1.23456f;
        Map<String, Object> raw = new HashMap<>() {{
            put(FLOAT_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(FLOAT_ATTRIBUTE), value, raw);

        assertEquals(1, results.size());
        assertEquals(1, (int) results.stream().filter(r -> r.getCode().equals(ERROR_SCALE_VALIDATION_FAILED)).count());
    }

    @Test
    void testValidateFloatWithPrecisionAndScaleOverflow() {
        Float value = 1234.567f;
        Map<String, Object> raw = new HashMap<>() {{
            put(FLOAT_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(FLOAT_ATTRIBUTE), value, raw);

        assertEquals(2, results.size());
        assertEquals(1, (int) results.stream().filter(r -> r.getCode().equals(ERROR_PRECISION_VALIDATION_FAILED)).count());
        assertEquals(1, (int) results.stream().filter(r -> r.getCode().equals(ERROR_SCALE_VALIDATION_FAILED)).count());
    }

    @Test
    void testValidateDoubleWithoutScale() {
        Double value = 1234.0;
        Map<String, Object> raw = new HashMap<>() {{
            put(DOUBLE_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(DOUBLE_ATTRIBUTE), value, raw);

        assertEquals(0, results.size());
    }

    @Test
    void testValidateDoubleWithScale() {
        Double value = 1234.56;
        Map<String, Object> raw = new HashMap<>() {{
            put(DOUBLE_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(DOUBLE_ATTRIBUTE), value, raw);

        assertEquals(0, results.size());
    }


    @Test
    void testValidateDoubleWithPrecisionOverflow() {
        Double value = Double.valueOf(1234567);
        Map<String, Object> raw = new HashMap<>() {{
            put(DOUBLE_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(DOUBLE_ATTRIBUTE), value, raw);

        assertEquals(1, results.size());
        assertEquals(1, (int) results.stream().filter(r -> r.getCode().equals(ERROR_PRECISION_VALIDATION_FAILED)).count());
    }

    @Test
    void testValidateDoubleWholeNumberWithPrecisionOverflow() {
        Double value = Double.valueOf(1234560);
        Map<String, Object> raw = new HashMap<>() {{
            put(DOUBLE_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(DOUBLE_ATTRIBUTE), value, raw);

        assertEquals(1, results.size());
        assertEquals(1, (int) results.stream().filter(r -> r.getCode().equals(ERROR_PRECISION_VALIDATION_FAILED)).count());
    }

    @Test
    void testValidateDoubleLargeNumberWithPrecisionOverflow() {
        Double value = Double.valueOf(1234560.0);
        Map<String, Object> raw = new HashMap<>() {{
            put(DOUBLE_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(DOUBLE_ATTRIBUTE), value, raw);

        assertEquals(1, results.size());
        assertEquals(1, (int) results.stream().filter(r -> r.getCode().equals(ERROR_PRECISION_VALIDATION_FAILED)).count());
    }

    @Test
    void testValidateDoubleWithScaleOverflow() {
        Double value = Double.valueOf(1.23456);
        Map<String, Object> raw = new HashMap<>() {{
            put(DOUBLE_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(DOUBLE_ATTRIBUTE), value, raw);

        assertEquals(1, results.size());
        assertEquals(1, (int) results.stream().filter(r -> r.getCode().equals(ERROR_SCALE_VALIDATION_FAILED)).count());
    }

    @Test
    void testValidateDoubleWithPrecisionAndScaleOverflow() {
        Double value = Double.valueOf(1234.567);

        Map<String, Object> raw = new HashMap<>() {{
            put(DOUBLE_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(DOUBLE_ATTRIBUTE), value, raw);

        assertEquals(2, results.size());
        assertEquals(1, (int) results.stream().filter(r -> r.getCode().equals(ERROR_PRECISION_VALIDATION_FAILED)).count());
        assertEquals(1, (int) results.stream().filter(r -> r.getCode().equals(ERROR_SCALE_VALIDATION_FAILED)).count());
    }

}
