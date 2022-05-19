package hu.blackbelt.judo.services.query;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.builder.jql.asm.AsmJqlExtractor;
import hu.blackbelt.judo.meta.expression.support.ExpressionModelResourceSupport;
import hu.blackbelt.judo.meta.query.Select;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.mapper.impl.DefaultCoercer;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static hu.blackbelt.judo.meta.asm.runtime.AsmModel.LoadArguments.asmLoadArgumentsBuilder;
import static hu.blackbelt.judo.meta.asm.runtime.AsmModel.loadAsmModel;
import static hu.blackbelt.judo.meta.expression.support.ExpressionModelResourceSupport.SaveArguments.expressionSaveArgumentsBuilder;

@Slf4j
public class RecursiveCompositionTest {

    private QueryFactory queryFactory;
    private AsmUtils asmUtils;

    @BeforeEach
    public void setUp() throws Exception {
        final ResourceSet asmResourceSet = loadAsmModel(asmLoadArgumentsBuilder().uri(URI.createFileURI(new File("src/test/models/recursive-composition-asm.model").getAbsolutePath()))
                .name("test")
                .validateModel(false)
                .build()).getResourceSet();
        asmUtils = new AsmUtils(asmResourceSet);

        final AsmJqlExtractor extractor = new AsmJqlExtractor(asmResourceSet, null, URI.createURI("expression:test"));
        final ResourceSet expressionResourceSet = extractor.extractExpressions();
        ExpressionModelResourceSupport.expressionModelResourceSupportBuilder()
                .resourceSet(expressionResourceSet)
                .build()
                .saveExpression(expressionSaveArgumentsBuilder()
                        .file(new File("target/test-classes/recursive-composition-expressions.model"))
                        .build());
        queryFactory = new QueryFactory(asmResourceSet, expressionResourceSet, new DefaultCoercer());
    }

    @AfterEach
    public void tearDown() {
        queryFactory = null;
        asmUtils = null;
    }

    @Test
    public void testGetQuery() {
        final EClass transferObjectType = asmUtils.getClassByFQName("Test.P").get();
        final Select select = queryFactory.getQuery(transferObjectType).get();
        if (log.isDebugEnabled()) {
            log.debug("QUERY:\n{}", select);
        }
    }
}
