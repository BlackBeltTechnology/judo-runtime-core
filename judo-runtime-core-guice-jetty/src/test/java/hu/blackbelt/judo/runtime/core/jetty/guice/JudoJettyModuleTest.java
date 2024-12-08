package hu.blackbelt.judo.runtime.core.jetty.guice;

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
import com.google.inject.Guice;
import com.google.inject.Injector;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class JudoJettyModuleTest {
    Injector injector;

    @SuppressWarnings({ "rawtypes", "resource" })
    @BeforeEach
    void init() throws Exception {
        Stopwatch timer = Stopwatch.createStarted();
        int port = new ServerSocket(0).getLocalPort();
        JudoJettyModules jettyModules = JudoJettyModules.builder()
                .jettyServerPort(port)
                .jettyContextPath("/")
                .build();

        injector = Guice.createInjector(jettyModules);

        injector.injectMembers(jettyModules);
        log.info("Init: " + (timer.elapsed().getNano() / 1024 / 1024) + "ms");
    }

    @AfterEach
    public void teardown() throws Exception {
        injector.getInstance(JettyContainer.class).stop();
    }

    @Test
    void test() {
        assertEquals("/", injector.getInstance(JettyContainer.class).getContextPath());
    }
}
