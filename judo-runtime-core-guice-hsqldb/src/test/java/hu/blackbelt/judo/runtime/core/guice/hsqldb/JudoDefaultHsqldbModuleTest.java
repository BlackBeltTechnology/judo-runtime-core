package hu.blackbelt.judo.runtime.core.guice.hsqldb;

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

import com.google.common.base.Stopwatch;
import com.google.inject.*;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.guice.JudoDefaultModule;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.guice.dao.rdbms.hsqldb.JudoHsqldbModule;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;

import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class JudoDefaultHsqldbModuleTest {

    Injector injector;

    @BeforeEach
    void init() throws Exception {
        Stopwatch timer = Stopwatch.createStarted();

        Module hsqlDbModule = JudoHsqldbModule.builder()
                .build();

        Module judoModule = JudoDefaultModule.builder()
                .injectModulesTo(this)
                .judoModelLoader(
                        JudoModelLoader.empty())
                .build();

        Module application = Modules.combine(judoModule, hsqlDbModule);

        injector = Guice.createInjector(application);
        log.info("Init: ⏱ " + timer.elapsed().toMillis()+ " ms");

    }

    @AfterEach
    void tearDown() {
    }


    @Test
    void test() {
        assertNotNull(injector.getInstance(DAO.class));
    }
}
