package hu.blackbelt.judo.runtime.core.validator;

import com.google.common.collect.ImmutableList;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dao.api.ValidationResult;
import org.eclipse.emf.ecore.*;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.eclipse.emf.ecore.util.builder.EcoreBuilders.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PrecisionValidatorTest {

    @Test
    void testValidateDecimal() {
        final EcorePackage ecore = EcorePackage.eINSTANCE;

        final EAnnotation doubleAnnotation = newEAnnotationBuilder()
                .withSource("http://blackbelt.hu/judo/meta/ExtendedMetadata/constraints")
                .build();
        doubleAnnotation.getDetails().put("precision", "16");
        doubleAnnotation.getDetails().put("scale", "4");

        final EClass eClass = newEClassBuilder()
                .withName("TestNumericTypesClass")
                .withEStructuralFeatures(
                        ImmutableList.of(
                                newEAttributeBuilder()
                                        .withName("doubleAttr")
                                        .withEType(ecore.getEDouble())
                                        .withEAnnotations(doubleAnnotation)
                                        .build()
                        )
                )
                .build();

        PrecisionValidator validator = new PrecisionValidator();
        Map<String, Object> raw = new HashMap<String, Object>();
        raw.put("TScaled", Double.valueOf(123456));
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature("doubleAttr"), Double.valueOf(123456), payload);

        assertEquals(0, results.size());
    }
}
