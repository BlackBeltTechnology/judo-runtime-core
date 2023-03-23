package hu.blackbelt.judo.runtime.core.bootstrap.hsqldb;

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

import com.google.inject.*;
import hu.blackbelt.epsilon.runtime.execution.api.Log;
import hu.blackbelt.epsilon.runtime.execution.impl.BufferedSlf4jLogger;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.support.AsmModelResourceSupport;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.expression.support.ExpressionModelResourceSupport;
import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.meta.liquibase.support.LiquibaseModelResourceSupport;
import hu.blackbelt.judo.meta.liquibase.util.builder.databaseChangeLogBuilder;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.measure.support.MeasureModelResourceSupport;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.rdbms.support.RdbmsModelResourceSupport;
import hu.blackbelt.judo.meta.rdbmsDataTypes.support.RdbmsDataTypesModelResourceSupport;
import hu.blackbelt.judo.meta.rdbmsNameMapping.support.RdbmsNameMappingModelResourceSupport;
import hu.blackbelt.judo.meta.rdbmsRules.support.RdbmsTableMappingRulesModelResourceSupport;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoDefaultModule;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.JudoHsqldbModules;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.util.builder.EPackageBuilder;
import org.junit.jupiter.api.*;

import java.util.HashMap;

import static hu.blackbelt.judo.tatami.asm2rdbms.ExcelMappingModels2Rdbms.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class JudoDefaultHsqldbModuleTest {

    @SuppressWarnings("rawtypes")
    @Inject
    DAO dao;

    @SuppressWarnings("rawtypes")
    @Inject
    Sequence sequence;

    @Inject
    Dispatcher dispatcher;

    Injector injector;

    @BeforeEach
    void init() throws Exception {


        AsmModel asmModel = AsmModel.buildAsmModel()
                .resourceSet(AsmModelResourceSupport.createAsmResourceSet())
                .build();

        asmModel.getAsmModelResourceSupport().addContent(EPackageBuilder.create()
                .withName("judo").withNsPrefix("judo").withNsURI("http://blackbelt.hu/test/judo/judo").build());

        RdbmsModel rdbmsModel = RdbmsModel.buildRdbmsModel()
                .resourceSet(RdbmsModelResourceSupport.createRdbmsResourceSet())
                .build();


        // The RDBMS model resources have to know the mapping models
        RdbmsNameMappingModelResourceSupport.registerRdbmsNameMappingMetamodel(rdbmsModel.getResourceSet());
        RdbmsDataTypesModelResourceSupport.registerRdbmsDataTypesMetamodel(rdbmsModel.getResourceSet());
        RdbmsTableMappingRulesModelResourceSupport.registerRdbmsTableMappingRulesMetamodel(rdbmsModel.getResourceSet());
        try (Log bufferedLog = new BufferedSlf4jLogger(log)) {
            injectExcelMappings(rdbmsModel, bufferedLog, calculateExcelMapping2RdbmsTransformationScriptURI(), calculateExcelMappingModelURI(), "hsqldb");
        }

        MeasureModel measureModel = MeasureModel.buildMeasureModel()
                .name(asmModel.getName())
                .resourceSet(MeasureModelResourceSupport.createMeasureResourceSet())
                .build();

        ExpressionModel expressionModel = ExpressionModel.buildExpressionModel()
                .name(asmModel.getName())
                .resourceSet(ExpressionModelResourceSupport.createExpressionResourceSet())
                .build();

        LiquibaseModel liquibaseModel = LiquibaseModel.buildLiquibaseModel()
                .name(asmModel.getName())
                .resourceSet(LiquibaseModelResourceSupport.createLiquibaseResourceSet())
                .build();

        liquibaseModel.getResource().getContents().add(databaseChangeLogBuilder.create().build());

        Asm2RdbmsTransformationTrace asm2rdbms = Asm2RdbmsTransformationTrace.asm2RdbmsTransformationTraceBuilder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .trace(new HashMap<>())
                .build();

        injector = Guice.createInjector(
                JudoHsqldbModules.builder().build(),
                new JudoDefaultModule(this,
                        JudoModelLoader.builder()
                            .asmModel(asmModel)
                            .rdbmsModel(rdbmsModel)
                            .measureModel(measureModel)
                            .expressionModel(expressionModel)
                            .liquibaseModel(liquibaseModel)
                            .asm2rdbms(asm2rdbms)
                            .build()));

        log.info("DAO: " + dao);
        log.info("Sequence: " + sequence);
        log.info("dispatcher: " + dispatcher);
    }

    @AfterEach
    void tearDown() {
    }


    @Test
    void test() {
        assertTrue(true);
    }
}
