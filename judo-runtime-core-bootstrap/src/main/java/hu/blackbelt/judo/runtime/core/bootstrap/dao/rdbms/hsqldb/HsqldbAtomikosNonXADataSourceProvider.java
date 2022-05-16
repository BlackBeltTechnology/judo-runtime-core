package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb;

import com.atomikos.jdbc.AtomikosNonXADataSourceBean;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.hsqldb.server.Server;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class HsqldbAtomikosNonXADataSourceProvider implements Provider<DataSource> {

    @Inject(optional = true)
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
        AtomikosNonXADataSourceBean ds = new AtomikosNonXADataSourceBean();
        ds.setPoolSize(10);
        ds.setLocalTransactionMode(true);
        System.setProperty("com.atomikos.icatch.output_dir", outDir.getAbsolutePath());
        System.setProperty("com.atomikos.icatch.log_base_dir", logDir.getAbsolutePath());

        ds.setUniqueResourceName("hsqldb-" + server.getDatabaseName(0, true));
        ds.setDriverClassName("org.hsqldb.jdbcDriver");
        // ds.setUrl("jdbc:hsqldb:mem:memdb");
        ds.setUrl(jdbcUrl);
        ds.setUser("sa");
        ds.setPassword("");

        return ds;
    }
}
