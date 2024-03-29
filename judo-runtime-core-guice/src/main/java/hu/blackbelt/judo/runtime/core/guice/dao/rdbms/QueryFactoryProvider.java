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
import com.google.inject.name.Named;
import hu.blackbelt.judo.meta.expression.builder.jql.JqlExpressionBuilderConfig;
import hu.blackbelt.judo.meta.expression.builder.jql.asm.AsmJqlExtractor;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.query.CustomJoinDefinition;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.mapper.api.Coercer;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EReference;

import javax.annotation.Nullable;
import java.util.Map;
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
    Map<EReference, CustomJoinDefinition> customJoinDefinitions;

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
                requireNonNullElse(customJoinDefinitions, new ConcurrentHashMap<>()));

        return queryFactory;
    }
}
