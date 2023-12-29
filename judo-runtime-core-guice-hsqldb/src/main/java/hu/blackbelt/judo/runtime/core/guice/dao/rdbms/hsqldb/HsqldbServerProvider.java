package hu.blackbelt.judo.runtime.core.guice.dao.rdbms.hsqldb;

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
import org.hsqldb.server.Server;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;

public class HsqldbServerProvider implements Provider<Server> {

    public static final String HSQLDB_SERVER_DATABASE_NAME = "hsqldbServerDatabaseName";
    public static final String HSQLDB_SERVER_DATABASE_PATH = "hsqldbServerDatabasePath";
    public static final String HSQLDB_SERVER_PORT = "hsqldbServerPort";


    @Inject(optional = true)
    @Named(HSQLDB_SERVER_DATABASE_NAME)
    @Nullable
    private String databaseName;

    @Inject(optional = true)
    @Named(HSQLDB_SERVER_DATABASE_PATH)
    @Nullable
    private File databasePath;

    @Inject(optional = true)
    @Named(HSQLDB_SERVER_PORT)
    @Nullable
    private Integer port;

    @Override
    public Server get() {

        String dbName = databaseName;
        if (dbName == null) {
            dbName = Long.toString(System.currentTimeMillis());
        }

        File dbPath = databasePath;
        if (dbPath == null) {
            File f = null;
            try {
                f = Files.createTempFile("hsqlDb-" + dbName, ".out").toFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            f.deleteOnExit();
            dbPath = f;
        }

        Integer dbPort = port;
        if (port == null || port <= 0) {
            try (ServerSocket serverSocket = new ServerSocket(0)) {
                dbPort = serverSocket.getLocalPort();
            } catch (IOException e) {
                throw new RuntimeException("Port is not available");
            }
        }

        Server server = new Server();
        //server.setLogWriter(new PrintWriter(new LogStream(log, LogStream.Level.INFO)));
        //server.setErrWriter(new PrintWriter(new LogStream(log, LogStream.Level.ERROR)));
        server.setTrace(false);
        server.setSilent(true);
        server.setDatabaseName(0, dbName);
        server.setDatabasePath(0, dbPath.getAbsolutePath());
        server.setPort(dbPort);
        server.setNoSystemExit(true);
        server.start();
        return server;
    }
}
