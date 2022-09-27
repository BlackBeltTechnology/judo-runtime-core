package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb;

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

import com.atomikos.jdbc.AtomikosNonXADataSourceBean;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.hsqldb.server.Server;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class HsqldbAtomikosNonXADataSourceProvider implements Provider<DataSource> {

    @Inject(optional = true)
    @Nullable
    private Server server;

    @Override
    public DataSource get() {
        File outDir = null;
        File logDir = null;
        String jdbcUrl = null;
        String databaseName = null;

        if (server != null) {
            outDir = new File(server.getDatabasePath(0, true), "atomikos-out");
            logDir = new File(server.getDatabasePath(0, true), "atomikos-log");
            databaseName = server.getDatabaseName(0, true);
            jdbcUrl = "jdbc:hsqldb:" + "hsql://" +
                    "localhost" + ":" + server.getPort() + "/" + databaseName;
        } else {
            databaseName = Long.toString(System.currentTimeMillis());
            jdbcUrl = "jdbc:hsqldb:mem:" + databaseName + ";DB_CLOSE_DELAY=-1";
            try {
                outDir = Files.createTempFile("atomikos-" + databaseName, ".out").toFile();
                outDir.deleteOnExit();
                logDir = Files.createTempFile("atomikos-" + databaseName, ".log").toFile();
                logDir.deleteOnExit();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        System.setProperty("com.atomikos.icatch.registered", "true");
        System.setProperty("com.atomikos.icatch.output_dir", outDir.getAbsolutePath());
        System.setProperty("com.atomikos.icatch.log_base_dir", logDir.getAbsolutePath());

        AtomikosNonXADataSourceBean ds = new AtomikosNonXADataSourceBean();
        ds.setUniqueResourceName("hsqldb/" + databaseName);
        ds.setDriverClassName("org.hsqldb.jdbcDriver");
        ds.setUrl(jdbcUrl);
        ds.setUser("sa");
        ds.setPassword("");
        ds.setPoolSize(10);
        ds.setLocalTransactionMode(true);

        return ds;
    }
}
