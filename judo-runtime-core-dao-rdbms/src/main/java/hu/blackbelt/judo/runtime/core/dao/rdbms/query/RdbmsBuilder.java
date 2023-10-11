package hu.blackbelt.judo.runtime.core.dao.rdbms.query;

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

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.rdbmsRules.Rules;
import hu.blackbelt.judo.runtime.core.dao.rdbms.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.RdbmsMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor.*;
import hu.blackbelt.mapper.api.Coercer;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class RdbmsBuilder<ID> {

    @Getter
    private final RdbmsResolver rdbmsResolver;

    @Getter
    private final RdbmsParameterMapper<ID> parameterMapper;

    @Getter
    private final IdentifierProvider<ID> identifierProvider;

    @Getter
    private final Coercer coercer;

    @Getter
    private final AtomicInteger constantCounter = new AtomicInteger(0);

    @Getter
    private final AncestorNameFactory ancestorNameFactory;

    @Getter
    private final DescendantNameFactory descendantNameFactory;

    @Getter
    private final  VariableResolver variableResolver;

    @Getter
    private final  RdbmsModel rdbmsModel;

    @Getter
    private final AsmUtils asmUtils;

    private final Map<Class<?>, RdbmsMapper<?>> mappers;

    private final Rules rules;

    @Getter
    private final Dialect dialect;


    private final AncestorJoinsProcessor ancestorJoinsProcessor;
    private final CastJoinProcessor castJoinProcessor;
    private final ContainerJoinProcessor containerJoinProcessor;
    private final CustomJoinProcessor customJoinProcessor;
    private final FilterJoinProcessor filterJoinProcessor;
    private final SimpleJoinProcessor simpleJoinProcessor;
    private final SubSelectJoinProcessor subSelectJoinProcessor;

    private final ThreadLocal<Map<String, Collection<? extends RdbmsField>>> CONSTANT_FIELDS = new ThreadLocal<>();

    @Builder
    public RdbmsBuilder(
            @NonNull RdbmsResolver rdbmsResolver,
            @NonNull RdbmsParameterMapper<ID> parameterMapper,
            @NonNull IdentifierProvider<ID> identifierProvider,
            @NonNull Coercer coercer,
            @NonNull AncestorNameFactory ancestorNameFactory,
            @NonNull DescendantNameFactory descendantNameFactory,
            @NonNull VariableResolver variableResolver,
            @NonNull RdbmsModel rdbmsModel,
            @NonNull AsmUtils asmUtils,
            @NonNull MapperFactory<ID> mapperFactory,
            @NonNull Dialect dialect) {
        this.rdbmsResolver = rdbmsResolver;
        this.parameterMapper = parameterMapper;
        this.identifierProvider = identifierProvider;
        this.coercer = coercer;
        this.ancestorNameFactory = ancestorNameFactory;
        this.descendantNameFactory = descendantNameFactory;
        this.variableResolver = variableResolver;
        this.rdbmsModel = rdbmsModel;
        this.asmUtils = asmUtils;
        this.dialect = dialect;
        this.mappers = mapperFactory.getMappers(this);
        this.rules = rdbmsModel.getResource().getContents().stream()
                .filter(Rules.class::isInstance)
                .map(Rules.class::cast)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Rules not found in RDBMS model"));

        this.simpleJoinProcessor = SimpleJoinProcessor.builder()
                .rdbmsResolver(rdbmsResolver)
                .rules(rules)
                .ancestorNameFactory(ancestorNameFactory)
                .build();

        this.ancestorJoinsProcessor = AncestorJoinsProcessor.builder()
                .ancestorNameFactory(ancestorNameFactory)
                .descendantNameFactory(descendantNameFactory)
                .rdbmsResolver(rdbmsResolver)
                .build();

        this.castJoinProcessor = CastJoinProcessor.builder()
                .rdbmsResolver(rdbmsResolver)
                .build();

        this.containerJoinProcessor = ContainerJoinProcessor.builder()
                .rdbmsResolver(rdbmsResolver)
                .build();

        this.customJoinProcessor = CustomJoinProcessor.builder()
                .rdbmsResolver(rdbmsResolver)
                .asmUtils(asmUtils)
                .build();

        this.filterJoinProcessor = FilterJoinProcessor.builder()
                .rdbmsResolver(rdbmsResolver)
                .build();


        this.subSelectJoinProcessor = SubSelectJoinProcessor.builder()
                .rdbmsResolver(rdbmsResolver)
                .build();
    }

    @SuppressWarnings("unchecked")
    public Stream<RdbmsField> mapFeatureToRdbms(final ParameterType value, final RdbmsBuilderContext builderContext) {
        return mappers.entrySet().stream()
                .filter(c -> c.getKey().isAssignableFrom(value.getClass()))
                .flatMap(mapper -> ((RdbmsMapper<ParameterType>) mapper.getValue()).map(value, builderContext));
    }

    public String getTableName(final EClass type) {
        return rdbmsResolver.rdbmsTable(type).getSqlName();
    }

    public String getColumnName(final EAttribute attribute) {
        return rdbmsResolver.rdbmsField(attribute).getSqlName();
    }

    public String getAncestorPostfix(final EClass clazz) {
        return ancestorNameFactory.getAncestorPostfix(clazz);
    }

    public String getDescendantPostfix(final EClass clazz) {
        return descendantNameFactory.getDescendantPostfix(clazz);
    }


    /**
     * Resolve logical JOIN and return RDBMS JOIN definition.
     *
     * @param params
     * @return RDBMS JOIN definition(s)
     */
    @SuppressWarnings("unchecked")
    public List<RdbmsJoin> processJoin(JoinProcessParameters params) {
        final Node join = params.getJoin();
        final boolean withoutFeatures = params.isWithoutFeatures();
        final Map<String, Object> mask = params.getMask();
        final RdbmsBuilderContext builderContext = params.getBuilderContext();

        final RdbmsBuilder<?> rdbmsBuilder = builderContext.rdbmsBuilder;
        final EMap<Node, EList<EClass>> ancestors = builderContext.ancestors;
        final SubSelect parentIdFilterQuery = builderContext.parentIdFilterQuery;
        final Map<String, Object> queryParameters = builderContext.queryParameters;


        if (join instanceof ReferencedJoin) {
            return simpleJoinProcessor.process(SimpleJoinProcessorParameters.builder()
                    .join((ReferencedJoin) join)
                    .reference(((ReferencedJoin)  join).getReference())
                    .opposite(((ReferencedJoin)  join).getReference().getEOpposite())
                    .builderContext(builderContext)
                    .build());
        } else if (join instanceof ContainerJoin) {
            return containerJoinProcessor.process(
                    (ContainerJoin) join,
                    builderContext);
        } else if (join instanceof CastJoin) {
            return castJoinProcessor.process(join,
                    builderContext);
        } else if (join instanceof SubSelectJoin) {
            return subSelectJoinProcessor.process(
                    (SubSelectJoin) join,
                    withoutFeatures,
                    mask,
                    builderContext);
        } else if (join instanceof CustomJoin) {
            final CustomJoin customJoin = (CustomJoin) join;
            return customJoinProcessor.process(
                    customJoin,
                    builderContext);
        } else {
            throw new IllegalStateException("Invalid JOIN");
        }
    }


    public void addAncestorJoins(final Collection<RdbmsJoin> joins, final Node node, final EMap<Node, EList<EClass>> ancestors) {
        ancestorJoinsProcessor.addAncestorJoins(joins, node, ancestors);
    }

    public void addFilterJoinsAndConditions(FilterJoinProcessorParameters params, RdbmsBuilderContext builderContext) {
        filterJoinProcessor.process(params, builderContext);
    }

    public List<RdbmsJoin> processSimpleJoin(SimpleJoinProcessorParameters params) {
        return simpleJoinProcessor.process(params);
    }
    public ThreadLocal<Map<String, Collection<? extends RdbmsField>>> getConstantFields() {
        return CONSTANT_FIELDS;
    }
}
