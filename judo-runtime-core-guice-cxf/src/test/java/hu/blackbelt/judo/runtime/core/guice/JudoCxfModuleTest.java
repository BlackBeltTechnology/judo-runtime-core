package hu.blackbelt.judo.runtime.core.guice;

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
import com.google.inject.Module;
import com.google.inject.util.Modules;
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
import hu.blackbelt.judo.runtime.core.guice.dao.rdbms.hsqldb.JudoHsqldbModules;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.JudoCxfModules;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.providers.CxfJaxrsServerProvider;
import hu.blackbelt.judo.runtime.core.jetty.guice.JettyContainer;
import hu.blackbelt.judo.runtime.core.jetty.guice.JudoJettyModules;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.util.builder.EPackageBuilder;
import org.junit.jupiter.api.*;
import java.util.HashMap;

import static hu.blackbelt.judo.tatami.asm2rdbms.ExcelMappingModels2Rdbms.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class JudoCxfModuleTest {

    @SuppressWarnings("rawtypes")
    @Inject
    JettyContainer jettyContainer;

    @Inject
    CxfJaxrsServerProvider.ServerHolder serverHolder;

    Injector injector;

    @SuppressWarnings({ "rawtypes", "resource" })
    @BeforeEach
    void init() throws Exception {

        Module judoModule = JudoDefaultModule.builder()
                .injectModulesTo(this).judoModelLoader(JudoModelLoader.empty()).build();

        Module sqlModule = JudoHsqldbModules.builder().build();

        Module jettyModule = JudoJettyModules.builder().build();

        Module cxfModule = JudoCxfModules.builder().build();

        injector = Guice.createInjector(Modules.combine(judoModule, sqlModule, jettyModule, cxfModule));

    }

    @AfterEach
    public void teardown() throws Exception {
        jettyContainer.stop();
    }

    @Test
    void test() {
        assertTrue(true);
    }
}
