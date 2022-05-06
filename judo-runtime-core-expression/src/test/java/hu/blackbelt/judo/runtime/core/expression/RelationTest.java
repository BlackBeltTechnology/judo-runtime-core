package hu.blackbelt.judo.runtime.core.expression;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.asm.support.AsmModelResourceSupport;
import hu.blackbelt.judo.meta.expression.builder.jql.asm.AsmJqlExtractor;
import hu.blackbelt.judo.runtime.core.expression.MappedTransferObjectTypeBindings;
import hu.blackbelt.judo.runtime.core.expression.TransferObjectTypeBindingsCollector;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.*;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.eclipse.emf.ecore.util.builder.EcoreBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.*;

@Slf4j
@DisplayName("Relation related tests")
public class RelationTest {

    private TransferObjectTypeBindingsCollector transferObjectTypeBindingsCollector;
    private AsmUtils asmUtils;

    @BeforeEach
    public void setUp() {
        final ResourceSet resourceSet = AsmModelResourceSupport.createAsmResourceSet();
        final Resource resource = resourceSet.createResource(URI.createURI("asm:test"));

        asmUtils = new AsmUtils(resourceSet);

        final EDataType stringType = newEDataTypeBuilder()
                .withName("String")
                .withInstanceClassName("java.lang.String")
                .build();

        final EClass a = newEClassBuilder()
                .withName("A")
                .withEStructuralFeatures(newEAttributeBuilder().withName("name").withEType(stringType).build())
                .build();

        final EClass b = newEClassBuilder()
                .withName("B")
                .withEStructuralFeatures(Arrays.asList(
                        newEAttributeBuilder().withName("name").withEType(stringType).build(),
                        newEReferenceBuilder().withName("a").withEType(a).withLowerBound(0).withUpperBound(1).withContainment(true).build(),
                        newEReferenceBuilder().withName("as").withEType(a).withLowerBound(0).withUpperBound(-1).build()
                )).build();

        final EAttribute nameOfADto = newEAttributeBuilder().withName("name").withEType(stringType).build();
        final EClass aDTO = newEClassBuilder()
                .withName("ADto")
                .withEStructuralFeatures(nameOfADto)
                .build();

        final EAttribute nameOfBDto = newEAttributeBuilder().withName("name").withEType(stringType).build();
        final EReference aOfBDto = newEReferenceBuilder().withName("a").withEType(aDTO).withLowerBound(0).withUpperBound(1).withContainment(true).build();
        final EReference asOfBDto = newEReferenceBuilder().withName("aList").withEType(aDTO).withLowerBound(0).withUpperBound(-1).withContainment(true).build();
        final EClass bDTO = newEClassBuilder()
                .withName("BDto")
                .withEStructuralFeatures(Arrays.asList(nameOfBDto, aOfBDto, asOfBDto)).build();

        final EPackage root = newEPackageBuilder()
                .withName("demo")
                .withEClassifiers(Arrays.asList(stringType, a, b, aDTO, bDTO))
                .build();

        resource.getContents().add(root);

        asmUtils.addExtensionAnnotation(a, "entity", Boolean.TRUE.toString());
        asmUtils.addExtensionAnnotation(b, "entity", Boolean.TRUE.toString());
        asmUtils.addExtensionAnnotation(aDTO, "mappedEntityType", AsmUtils.getClassifierFQName(a));
        asmUtils.addExtensionAnnotation(bDTO, "mappedEntityType", AsmUtils.getClassifierFQName(b));
        asmUtils.addExtensionAnnotation(nameOfADto, "binding", "name");
        asmUtils.addExtensionAnnotation(nameOfBDto, "binding", "name");
        asmUtils.addExtensionAnnotation(aOfBDto, "binding", "a");
        asmUtils.addExtensionAnnotation(asOfBDto, "binding", "as");

        final AsmJqlExtractor extractor = new AsmJqlExtractor(resourceSet, null, URI.createURI("expression:test"));
        transferObjectTypeBindingsCollector = new TransferObjectTypeBindingsCollector(resourceSet, extractor.extractExpressions());
    }

    @AfterEach
    public void tearDown() {
        transferObjectTypeBindingsCollector = null;
        asmUtils = null;
    }

    @Test
    @DisplayName("Test containments")
    public void testContainment() {
        final EClass transferObjectType = asmUtils.getClassByFQName("demo.BDto").get();
        final MappedTransferObjectTypeBindings transferObjectGraph = transferObjectTypeBindingsCollector.getTransferObjectGraph(transferObjectType).get();

        assertThat(transferObjectGraph.getReferences(), hasSize(2));
        assertThat(transferObjectGraph.getReferences().stream().filter(e -> e.getKey().isContainment()).collect(Collectors.toList()), hasSize(2));
    }
}
