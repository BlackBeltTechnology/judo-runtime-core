package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb;

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.hsqldb.jdbc.JDBCPool;
import org.hsqldb.server.Server;

import javax.sql.DataSource;

public class HsqldbDataSourceProvider implements Provider<DataSource> {

    @Inject(optional = true)
    private Server server;

    @Override
    public DataSource get() {
        String jdbcUrl = null;

        if (server != null) {
            String databaseName = server.getDatabaseName(0, true);
            jdbcUrl = "jdbc:hsqldb:" + "hsql://" +
                    "localhost" + ":" + server.getPort() + "/" + databaseName;
        } else {
            jdbcUrl = "jdbc:hsqldb:mem:".concat("judo");

        }

        final JDBCPool pool;
        pool = new JDBCPool();
        pool.setURL(jdbcUrl);
        return pool;
    }
}
