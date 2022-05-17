package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb;

import static hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.HsqldbRdbmsSequenceProvider.RDBMS_SEQUENCE_CREATE_IF_NOT_EXISTS;
import static hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.HsqldbRdbmsSequenceProvider.RDBMS_SEQUENCE_INCREMENT;
import static hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.HsqldbRdbmsSequenceProvider.RDBMS_SEQUENCE_START;
import static hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.HsqldbServerProvider.HSQLDB_SERVER_DATABASE_NAME;
import static hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.HsqldbServerProvider.HSQLDB_SERVER_DATABASE_PATH;
import static hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.HsqldbServerProvider.HSQLDB_SERVER_PORT;

import java.io.File;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import com.google.inject.util.Providers;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.SimpleLiquibaseExecutorProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.postgresql.PostgresqlRdbmsInitProvider;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;
import liquibase.database.Database;
import lombok.Builder;
import org.hsqldb.server.Server;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.AtomikosUserTransactionManagerProvider;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbDialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;

public class JudoHsqldbModules extends AbstractModule {

    private Boolean runServer = false;
    private String databaseName = "judo";
    private File databasePath = new File(".", "judo.db");
    private Integer port = 31001;


    public static class JudoHsqldbModulesBuilder {
        private Boolean runServer = false;
        private String databaseName = "judo";
        private File databasePath = new File(".", "judo.db");
        private Integer port = 31001;
    }
    @Builder
    private JudoHsqldbModules(Boolean runServer, String databaseName, File databasePath, Integer port) {
        this.runServer = runServer;
        this.databaseName = databaseName;
        this.databasePath = databasePath;
        this.port = port;
    }
    protected void configure() {

        // HSQLDB
        bind(Dialect.class).toInstance(new HsqldbDialect());
        if (runServer) {
            bind(Server.class).toProvider(HsqldbServerProvider.class).in(Singleton.class);
        } else {
            bind(Server.class).toProvider(Providers.of(null)).in(Singleton.class);
        }
        bind(String.class).annotatedWith(Names.named(HSQLDB_SERVER_DATABASE_NAME)).toInstance(databaseName);
        bind(File.class).annotatedWith(Names.named(HSQLDB_SERVER_DATABASE_PATH)).toInstance(databasePath);
        bind(Integer.class).annotatedWith(Names.named(HSQLDB_SERVER_PORT)).toInstance(31001);

        bind(MapperFactory.class).toProvider(HsqldbMapperFactoryProvider.class).in(Singleton.class);
        bind(RdbmsParameterMapper.class).toProvider(HsqldbRdbmsParameterMapperProvider.class).in(Singleton.class);

        // Datasource        
        bind(DataSource.class).toProvider(HsqldbAtomikosNonXADataSourceProvider.class).in(Singleton.class);

        bind(Sequence.class).toProvider(HsqldbRdbmsSequenceProvider.class).in(Singleton.class);
        bind(Long.class).annotatedWith(Names.named(RDBMS_SEQUENCE_START)).toInstance(1L);
        bind(Long.class).annotatedWith(Names.named(RDBMS_SEQUENCE_INCREMENT)).toInstance(1L);
        bind(Boolean.class).annotatedWith(Names.named(RDBMS_SEQUENCE_CREATE_IF_NOT_EXISTS)).toInstance(true);

        bind(TransactionManager.class).toProvider(AtomikosUserTransactionManagerProvider.class).in(Singleton.class);

        bind(SimpleLiquibaseExecutor.class).toProvider(SimpleLiquibaseExecutorProvider.class).in(Singleton.class);
        bind(RdbmsInit.class).toProvider(HsqldbRdbmsInitProvider.class).in(Singleton.class);
    }
}
