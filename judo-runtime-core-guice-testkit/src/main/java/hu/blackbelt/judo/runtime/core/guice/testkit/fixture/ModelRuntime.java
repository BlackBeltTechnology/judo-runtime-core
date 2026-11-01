package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import hu.blackbelt.judo.meta.expression.builder.jql.JqlExpressionBuilderConfig;
import hu.blackbelt.judo.meta.expression.builder.jql.asm.AsmJqlExtractor;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import hu.blackbelt.mapper.impl.DefaultCoercer;
import org.eclipse.emf.common.util.URI;

import java.util.concurrent.ConcurrentHashMap;

/**
 * The model-derived, read-only part of a test runtime: the loaded models, the coercer and the
 * {@link QueryFactory} built from them.
 *
 * <p>Building the QueryFactory dominates the runtime bootstrap on large models, and it depends
 * only on the model. With {@code judo.testkit.shareModel=true} one instance is therefore shared
 * JVM-wide per {@link SingletonModelKey} (JNG-6421). Everything stateful — DataSource, Liquibase
 * schema, Injector, transaction manager — stays in {@link CachedRuntime} / the per-class fixture.
 */
final class ModelRuntime {

    final JudoModelLoader modelLoader;
    final ExtendableCoercer coercer;
    final QueryFactory queryFactory;

    ModelRuntime(JudoModelLoader modelLoader, ExtendableCoercer coercer, QueryFactory queryFactory) {
        this.modelLoader = modelLoader;
        this.coercer = coercer;
        this.queryFactory = queryFactory;
    }

    /** Builds the coercer and the QueryFactory for the given model. */
    static ModelRuntime build(JudoModelLoader modelLoader) {
        ExtendableCoercer coercer = new DefaultCoercer();

        JqlExpressionBuilderConfig jqlExpressionBuilderConfig = new JqlExpressionBuilderConfig();
        jqlExpressionBuilderConfig.setResolveOnlyCurrentLambdaScope(false);

        final AsmJqlExtractor asmJqlExtractor = new AsmJqlExtractor(
            modelLoader.getAsmModel().getResourceSet(),
            modelLoader.getMeasureModel().getResourceSet(),
            URI.createURI("expr:" + modelLoader.getAsmModel().getName()),
            jqlExpressionBuilderConfig
        );

        QueryFactory queryFactory = new QueryFactory(modelLoader.getAsmModel().getResourceSet(),
                modelLoader.getMeasureModel().getResourceSet(), asmJqlExtractor.extractExpressions(), coercer,
                new ConcurrentHashMap<>());
        return new ModelRuntime(modelLoader, coercer, queryFactory);
    }
}
