package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.meta.expression.builder.jql.JqlExpressionBuilderConfig;
import hu.blackbelt.judo.meta.expression.builder.jql.asm.AsmJqlExtractor;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.query.CustomJoinDefinition;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.mapper.api.Coercer;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EReference;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNullElse;

public class QueryFactoryProvider implements Provider<QueryFactory> {

    public static final String QUERY_FACTORY_CUSTOM_JOIN_DEFINITIONS = "queryFactoryCustomJoinDefinitions";

    @Inject
    JudoModelLoader models;

    @Inject
    Coercer coercer;

    @Inject(optional = true)
    @Named(QUERY_FACTORY_CUSTOM_JOIN_DEFINITIONS)
    @Nullable
    EMap<EReference, CustomJoinDefinition> customJoinDefinitions;

    @Override
    public QueryFactory get() {

        JqlExpressionBuilderConfig jqlExpressionBuilderConfig = new JqlExpressionBuilderConfig();
        jqlExpressionBuilderConfig.setResolveOnlyCurrentLambdaScope(false);

        final AsmJqlExtractor asmJqlExtractor = new AsmJqlExtractor(models.getAsmModel().getResourceSet(),
                models.getMeasureModel().getResourceSet(), URI.createURI("expr:" + models.getAsmModel().getName()), jqlExpressionBuilderConfig);

        QueryFactory queryFactory = new QueryFactory(
                models.getAsmModel().getResourceSet(),
                models.getMeasureModel().getResourceSet(),
                asmJqlExtractor.extractExpressions(),
                coercer,
                requireNonNullElse(customJoinDefinitions, ECollections.asEMap(new ConcurrentHashMap<>())));

        return queryFactory;
    }
}
