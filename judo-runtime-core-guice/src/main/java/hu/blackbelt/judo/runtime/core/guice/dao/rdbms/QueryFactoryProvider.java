package hu.blackbelt.judo.runtime.core.guice.dao.rdbms;

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

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.meta.expression.builder.jql.JqlExpressionBuilderConfig;
import hu.blackbelt.judo.meta.expression.builder.jql.asm.AsmJqlExtractor;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.guice.JudoConfigurationQualifiers;
import hu.blackbelt.judo.runtime.core.query.CustomJoinDefinition;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.runtime.core.query.QueryModelLoadOrBuild;
import hu.blackbelt.mapper.api.Coercer;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EReference;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNullElse;

public class QueryFactoryProvider implements Provider<QueryFactory> {

    @Inject
    JudoModelLoader models;

    @Inject
    Coercer coercer;

    @Inject(optional = true)
    @JudoConfigurationQualifiers.QueryFactoryCustomJoinDefinitions
    @Nullable
    Map<EReference, CustomJoinDefinition> customJoinDefinitions;

    @Inject(optional = true)
    @JudoConfigurationQualifiers.QueryModelMode
    @Nullable
    String queryModelMode;

    @Override
    public QueryFactory get() {
        // the build path is unchanged: JQL extraction with resolveOnlyCurrentLambdaScope=false
        final Supplier<QueryFactory> build = () -> {
            JqlExpressionBuilderConfig jqlExpressionBuilderConfig = new JqlExpressionBuilderConfig();
            jqlExpressionBuilderConfig.setResolveOnlyCurrentLambdaScope(false);

            final AsmJqlExtractor asmJqlExtractor = new AsmJqlExtractor(models.getAsmModel().getResourceSet(),
                    models.getMeasureModel().getResourceSet(), URI.createURI("expr:" + models.getAsmModel().getName()), jqlExpressionBuilderConfig);

            return new QueryFactory(
                    models.getAsmModel().getResourceSet(),
                    models.getMeasureModel().getResourceSet(),
                    asmJqlExtractor.extractExpressions(),
                    coercer,
                    requireNonNullElse(customJoinDefinitions, new ConcurrentHashMap<>()));
        };

        return QueryModelLoadOrBuild.queryModelLoadOrBuild()
                .asmModel(models.getAsmModel())
                .asmDigest(models.getAsmDigest())
                .measureModel(models.getMeasureModel())
                .measureDigest(models.getMeasureDigest())
                .expressionModel(models.getExpressionModel())
                .expressionDigest(models.getExpressionDigest())
                .persistedQueryModel(models.getPersistedQueryModel())
                .customJoinDefinitions(customJoinDefinitions)
                .mode(queryModelMode)
                .buildQueryFactory(build)
                .build()
                .get();
    }
}
