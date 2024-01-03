package hu.blackbelt.judo.runtime.core.dagger2.dao.rdbms.hsqldb;

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
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.dagger2.database.DatabaseScope;
import org.hsqldb.jdbc.JDBCDataSource;
import org.hsqldb.jdbc.JDBCPool;
import org.hsqldb.server.Server;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.sql.DataSource;

@Module
public class HsqldbDataSourceModule {


    @JudoApplicationScope
    @Provides
    public DataSource providesDataSource(@Nullable Server server) {
        String jdbcUrl = null;
        String databaseName = null;

        if (server != null) {
            databaseName = server.getDatabaseName(0, true);
            jdbcUrl = "jdbc:hsqldb:" + "hsql://" +
                    "localhost" + ":" + server.getPort() + "/" + databaseName;
        } else {
            databaseName = Long.toString(System.currentTimeMillis());
            jdbcUrl = "jdbc:hsqldb:mem:" + databaseName + ";DB_CLOSE_DELAY=-1";
        }

        boolean pooled = true;

        final DataSource ds;
        final JDBCPool pool;
        if (pooled) {
            pool = new JDBCPool();
            pool.setURL(jdbcUrl);
            ds = pool;
        } else {
            final JDBCDataSource simple = new JDBCDataSource();
            simple.setURL(jdbcUrl);
            ds = simple;
            pool = null;
        }
        return ds;
    }
}
