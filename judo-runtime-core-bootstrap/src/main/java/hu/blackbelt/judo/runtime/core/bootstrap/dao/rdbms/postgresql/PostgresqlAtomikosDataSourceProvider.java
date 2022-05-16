package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.postgresql;

import com.atomikos.jdbc.AtomikosNonXADataSourceBean;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import net.jmob.guice.conf.core.InjectConfig;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Properties;

public class PostgresqlAtomikosDataSourceProvider implements Provider<DataSource> {

	//@Inject
	//PostgresqlConfiguration configuration;
	
    public static final String POSTGRESQL_PORT = "postgresqlPort";
    public static final String POSTGRESQL_HOST = "postgresqlHost";
    public static final String POSTGRESQL_USER = "postgresqlUser";
    public static final String POSTGRESQL_PASSWORD = "postgresqlPassword";
    public static final String POSTGRESQL_DATABASENAME = "postgresqlDatabaseName";
    public static final String POSTGRESQL_POOLSIZE = "postgresqlPoolsize";

        
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

    @Inject
    @Named(POSTGRESQL_POOLSIZE)
    public Integer poolSize = 10;

    
    @Override
    public DataSource get() {
        
    	File outDir = null;
        File logDir = null;

        try {
            outDir = Files.createTempFile("atomikos-" + databaseName, ".out").toFile();
            outDir.deleteOnExit();
            logDir = Files.createTempFile("atomikos-" + databaseName, ".log").toFile();
            logDir.deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.setProperty("com.atomikos.icatch.registered", "true");
        System.setProperty("com.atomikos.icatch.output_dir", outDir.getAbsolutePath());
        System.setProperty("com.atomikos.icatch.log_base_dir", logDir.getAbsolutePath());

        /*
        
        ds.setUniqueResourceName("hsqldb-" + server.getDatabaseName(0, true));
        ds.setDriverClassName("org.hsqldb.jdbcDriver");
        // ds.setUrl("jdbc:hsqldb:mem:memdb");
        ds.setUrl(jdbcUrl);
        ds.setUser("sa");
        ds.setPassword("");
       	*/
        
        AtomikosNonXADataSourceBean ds = new AtomikosNonXADataSourceBean(); 
        ds.setUniqueResourceName("postgres"); 
        ds.setDriverClassName("org.postgresql.Driver"); 
        ds.setLocalTransactionMode(true);
        
        ds.setUrl("jdbc:postgresql://" + host + ":" + port + "/" + databaseName);
        ds.setUser(user);
        ds.setPassword(password);

        
        System.out.println("==============> " 
        + "jdbc:postgresql://" + host + ":" + port + "/" + databaseName);
        
        /*
        Properties p = new Properties(); 
        p.setProperty ( "user" , "java" ); 
        p.setProperty ( "password" , "java" ); 
        p.setProperty ( "serverName" , "localhost" ); 
        p.setProperty ( "portNumber" , "5432" ); 
        p.setProperty ( "databaseName" , "java" ); 
        ds.set ( p );  
        */
        
        
        ds.setPoolSize(10);
        
        return ds;
    }
}
