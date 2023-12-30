package hu.blackbelt.judo.runtime.core.guice.dao.rdbms.postgresql;

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
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class PostgresqlDataSourceProvider implements Provider<DataSource> {

    public static final String POSTGRESQL_PORT = "postgresqlPort";
    public static final String POSTGRESQL_HOST = "postgresqlHost";
    public static final String POSTGRESQL_USER = "postgresqlUser";
    public static final String POSTGRESQL_PASSWORD = "postgresqlPassword";
    public static final String POSTGRESQL_DATABASENAME = "postgresqlDatabaseName";


    @Inject
    @Named(POSTGRESQL_PORT)
    public Integer port = 5432;

    @Inject
    @Named(POSTGRESQL_HOST)
    public String host = "localhost";

    @Inject
    @Named(POSTGRESQL_USER)
    public String user;

    @Inject
    @Named(POSTGRESQL_PASSWORD)
    public String password;

    @Inject
    @Named(POSTGRESQL_DATABASENAME)
    public String databaseName;


    @Override
    public DataSource get() {


        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl("jdbc:postgresql://" + host + ":" + port + "/" + databaseName);
        ds.setUser(user);
        ds.setPassword(password);
        return ds;
    }
}
