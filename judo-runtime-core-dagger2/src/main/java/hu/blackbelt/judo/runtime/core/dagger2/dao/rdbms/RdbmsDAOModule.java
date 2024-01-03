package hu.blackbelt.judo.runtime.core.dagger2.dao.rdbms;

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

import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsDAOImpl;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.ModifyStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.SelectStatementExecutor;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import javax.inject.Inject;
import javax.inject.Named;

import static java.util.Objects.requireNonNullElse;

@SuppressWarnings("rawtypes")
@Module
public class RdbmsDAOModule {
    public static final String RDBMS_DAO_OPTIMISTIC_LOCK_ENABLED = "rdbmsDaoOptimisticLockEnabled";
    public static final String RDBMS_DAO_MARK_SELECTED_RANGE_ITEMS = "rdbmsDaoMarkSelectedRangeItems";


    // Force execute liquibase database creation
/*
    @SuppressWarnings("unused")
    @Inject
    RdbmsInit init;

    @Inject
    AsmModel asmModel;

    @Inject
    DataSource dataSource;

    @Inject
    IdentifierProvider identifierProvider;

    @Inject
    Context context;

    @Inject
    MetricsCollector metricsCollector;

    @Inject
    InstanceCollector instanceCollector;

    @Inject
    ModifyStatementExecutor modifyStatementExecutor;

    @Inject
    SelectStatementExecutor selectStatementExecutor;

    @Inject
    QueryFactory queryFactory;

    @Inject // (optional = true)
    @Named(RDBMS_DAO_OPTIMISTIC_LOCK_ENABLED)
    @Nullable
    Boolean optimisticLockEnabled;

    @Inject // (optional = true)
    @Named(RDBMS_DAO_MARK_SELECTED_RANGE_ITEMS)
    @Nullable
    Boolean markSelectedRangeItems;
    */
    @SuppressWarnings("unchecked")
    @JudoApplicationScope
    @Provides

    public DAO providesDAO(
            RdbmsInit init,
            AsmModel asmModel,
            DataSource dataSource,
            IdentifierProvider identifierProvider,
            Context context,
            MetricsCollector metricsCollector,
            InstanceCollector instanceCollector,
            ModifyStatementExecutor modifyStatementExecutor,
            SelectStatementExecutor selectStatementExecutor,
            QueryFactory queryFactory,
            @Named(RDBMS_DAO_OPTIMISTIC_LOCK_ENABLED) @Nullable Boolean optimisticLockEnabled,
            @Named(RDBMS_DAO_MARK_SELECTED_RANGE_ITEMS) @Nullable Boolean markSelectedRangeItems) {
        RdbmsDAOImpl.RdbmsDAOImplBuilder builder =  RdbmsDAOImpl.builder()
                .dataSource(dataSource)
                .context(context)
                .asmModel(asmModel)
                .identifierProvider(identifierProvider)
                .instanceCollector(instanceCollector)
                .metricsCollector(metricsCollector)
                .optimisticLockEnabled(requireNonNullElse(optimisticLockEnabled, true))
                .markSelectedRangeItems(requireNonNullElse(markSelectedRangeItems, false))
                .selectStatementExecutor(selectStatementExecutor)
                .modifyStatementExecutor(modifyStatementExecutor)
                .queryFactory(queryFactory)
                ;

        return builder.build();
    }
}
