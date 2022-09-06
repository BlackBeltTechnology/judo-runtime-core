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

        final EPackage ePackage = newEPackageBuilder()
                .withName("TestEpackage")
                .withNsPrefix("test")
                .withNsURI("http:///com.example.test.ecore")
                .build();

        final EAnnotation bigDecimalAttrAnnotation = newEAnnotationBuilder()
                .withSource("http://blackbelt.hu/judo/meta/ExtendedMetadata/constraints")
                .build();
        bigDecimalAttrAnnotation.getDetails().put("precision", "64");
        bigDecimalAttrAnnotation.getDetails().put("scale", "20");

        final EAnnotation bigIntegerAttrAnnotation = newEAnnotationBuilder()
                .withSource("http://blackbelt.hu/judo/meta/ExtendedMetadata/constraints")
                .build();
        bigIntegerAttrAnnotation.getDetails().put("precision", "18");

        final EAnnotation javaMathBigIntegerAttrAnnotation = newEAnnotationBuilder()
                .withSource("http://blackbelt.hu/judo/meta/ExtendedMetadata/constraints")
                .build();
        javaMathBigIntegerAttrAnnotation.getDetails().put("precision", "18");

        final EAnnotation javaMathBigDecimalAttrAnnotation = newEAnnotationBuilder()
                .withSource("http://blackbelt.hu/judo/meta/ExtendedMetadata/constraints")
                .build();
        javaMathBigDecimalAttrAnnotation.getDetails().put("precision", "64");
        javaMathBigDecimalAttrAnnotation.getDetails().put("scale", "20");

        final EClass eClass = newEClassBuilder()
                .withName("TestNumericTypesClass")
                .withEStructuralFeatures(
                        ImmutableList.of(
                                newEAttributeBuilder()
                                        .withName("bigDecimalAttr")
                                        .withEType(ecore.getEBigDecimal())
                                        .withEAnnotations(bigDecimalAttrAnnotation)
                                        .build(),
                                newEAttributeBuilder()
                                        .withName("bigInteger")
                                        .withEType(ecore.getEBigInteger())
                                        .withEAnnotations(bigIntegerAttrAnnotation)
                                        .build(),
                                newEAttributeBuilder()
                                        .withName("doubleAttr")
                                        .withEType(ecore.getEDouble())
                                        .build(),
                                newEAttributeBuilder()
                                        .withName("floatAttr")
                                        .withEType(ecore.getEFloat())
                                        .build(),
                                newEAttributeBuilder()
                                        .withName("intAttr")
                                        .withEType(ecore.getEInt())
                                        .build(),
                                newEAttributeBuilder()
                                        .withName("longAttr")
                                        .withEType(ecore.getELong())
                                        .build()
                        )
                )
                .build();

        PrecisionValidator validator = new PrecisionValidator();
        Map<String, Object> raw = new HashMap<String, Object>();
        raw.put("TScaled", "123456");
        Payload payload = Payload.asPayload(raw);

        Collection<ValidationResult> results = validator.validateValue(payload, eClass.getEStructuralFeature("bigDecimalAttr"), "123456", payload);

        assertEquals(2, results.size());
    }
}
