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

import com.google.common.base.Stopwatch;
import com.google.inject.*;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import hu.blackbelt.judo.runtime.core.guice.dao.rdbms.hsqldb.JudoHsqldbModule;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.JudoCxfModule;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.providers.CxfJaxrsServerProvider;
import hu.blackbelt.judo.runtime.core.jetty.guice.JettyContainer;
import hu.blackbelt.judo.runtime.core.jetty.guice.JudoJettyModules;
import lombok.extern.slf4j.Slf4j;
import org.apache.cxf.jaxrs.client.WebClient;
import org.junit.jupiter.api.*;

import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class JudoCxfModuleTest {

    Injector injector;

    int port;
    String restUri;

    @Inject
    CxfJaxrsServerProvider.ServerHolder serverHolder;

    @SuppressWarnings({ "rawtypes", "resource" })
    @BeforeEach
    void init() throws Exception {
        Stopwatch timer = Stopwatch.createStarted();
        int port = new ServerSocket(0).getLocalPort();

        Module judoModule = JudoDefaultModule.builder()
                .judoModelLoader(JudoModelLoader.empty()).build();

        Module sqlModule = JudoHsqldbModule.builder().build();

        Module jettyModule = JudoJettyModules.builder()
                .jettyServerPort(port)
                .build();

        Module cxfModule = JudoCxfModule.builder().build();
        Module calcModule = new CalcModule();

        injector = Guice.createInjector(Modules.combine(judoModule, sqlModule, jettyModule, cxfModule, calcModule));
        injector.injectMembers(cxfModule);
        injector.injectMembers(this);

        restUri = "http://localhost:" + port + "/api/Calc";
        log.info("Init: " + (timer.elapsed().getNano() / 1024 / 1024) + "ms");
    }

    @AfterEach
    void destroy() {
        injector.getInstance(CxfJaxrsServerProvider.ServerHolder.class).getServers().values().stream().forEach(s -> s.destroy());
        injector.getInstance(JettyContainer.class).stop();
    }

    @Test
    public void testAddCall() {
        int a = 122;
        int b = 34;
        WebClient plainAddClient = WebClient.create(restUri);
        plainAddClient.path("calc/add").path(a + "/" + b).accept("application/json");
        String res = plainAddClient.get(String.class);
        assertEquals("156.0", res);
    }

    /*
    @AfterEach
    public void teardown() throws Exception {
        jettyContainer.stop();
    } */

}
