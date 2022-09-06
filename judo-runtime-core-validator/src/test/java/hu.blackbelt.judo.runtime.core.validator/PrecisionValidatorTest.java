package hu.blackbelt.judo.runtime.core.validator;

import com.google.common.collect.ImmutableList;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dao.api.ValidationResult;
import org.eclipse.emf.ecore.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.runtime.core.validator.Validator.*;
import static org.eclipse.emf.ecore.util.builder.EcoreBuilders.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PrecisionValidatorTest {
    private static String DOUBLE_ATTRIBUTE = "doubleAttr";

    private EcorePackage ecore;
    private EPackage ePackage;
    private EClass eClass;
    private EAnnotation doubleAnnotation;

    private PrecisionValidator validator;

    @BeforeEach
    void init() {
        ecore = EcorePackage.eINSTANCE;

        ePackage = newEPackageBuilder()
                .withName("TestEpackage")
                .withNsPrefix("test")
                .withNsURI("http:///com.example.test.ecore")
                .build();

        doubleAnnotation = newEAnnotationBuilder()
                .withSource("http://blackbelt.hu/judo/meta/ExtendedMetadata/constraints")
                .build();
        doubleAnnotation.getDetails().put("precision", "6");
        doubleAnnotation.getDetails().put("scale", "2");

        eClass = newEClassBuilder()
                .withName("PrecisionValidatorTestClass")
                .withEStructuralFeatures(
                        ImmutableList.of(
                                newEAttributeBuilder()
                                        .withName(DOUBLE_ATTRIBUTE)
                                        .withEType(ecore.getEDouble())
                                        .withEAnnotations(doubleAnnotation)
                                        .build()
                        )
                )
                .build();

        ePackage.getEClassifiers().add(eClass);

        validator = new PrecisionValidator();
    }

    @Test
    void testValidateDecimalWithoutScale() {
        BigDecimal value = BigDecimal.valueOf(1234);
        Map<String, Object> raw = new HashMap<>() {{
            put(DOUBLE_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(DOUBLE_ATTRIBUTE), value, raw);

        assertEquals(0, results.size());
    }

    @Test
    void testValidateDecimalPrecisionOverflow() {
        BigDecimal value = BigDecimal.valueOf(1234567);
        Map<String, Object> raw = new HashMap<>() {{
            put(DOUBLE_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(DOUBLE_ATTRIBUTE), value, raw);
        ValidationResult result = results.stream().findFirst().get();

        assertEquals(1, results.size());
        assertEquals(ERROR_PRECISION_VALIDATION_FAILED, result.getCode());
        assertEquals("TestEpackage_PrecisionValidatorTestClass_doubleAttr", result.getDetails().get(FEATURE_KEY));
        assertEquals(6, result.getDetails().get("precision"));
        assertEquals(value, result.getDetails().get(VALUE_KEY));
    }

    @Test
    void testValidateDecimalScaleOverflow() {
        BigDecimal value = BigDecimal.valueOf(1.23456);
        Map<String, Object> raw = new HashMap<>() {{
            put(DOUBLE_ATTRIBUTE, value);
        }};
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature(DOUBLE_ATTRIBUTE), value, raw);
        ValidationResult result = results.stream().findFirst().get();

        assertEquals(1, results.size());
        assertEquals(ERROR_SCALE_VALIDATION_FAILED, result.getCode());
        assertEquals("TestEpackage_PrecisionValidatorTestClass_doubleAttr", result.getDetails().get(FEATURE_KEY));
        assertEquals(5, result.getDetails().get("scale"));
        assertEquals(value, result.getDetails().get(VALUE_KEY));
    }

    @Test
    void testValidateDecimalPrecisionAndScaleError() {
        BigDecimal value = BigDecimal.valueOf(12.3456789);
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
