package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import org.hsqldb.server.Server;

import java.io.File;

public class HsqldbServerProvider implements Provider<Server> {

    static class HsqldbServerOptional {
        @Inject(optional=true) public Server value = null;
    }

    public static final String HSQLDB_SERVER_DATABASE_NAME = "hsqldbServerDatabaseName";
    public static final String HSQLDB_SERVER_DATABASE_PATH = "hsqldbServerDatabasePath";
    public static final String HSQLDB_SERVER_PORT = "hsqldbServerPort";
    private final String databaseName;

    private final File databasePath;

    private final Integer port;

    @Inject
    public HsqldbServerProvider(
            @Named(HSQLDB_SERVER_DATABASE_NAME) String databaseName,
            @Named(HSQLDB_SERVER_DATABASE_PATH) File databasePath,
            @Named(HSQLDB_SERVER_PORT) Integer port) {
        this.port = port;
        this.databaseName = databaseName;
        this.databasePath = databasePath;

    }

    @Override
    public Server get() {

        /*
        int port = 0;
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            port = serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Port is not available");
        } */
        Server server = new Server();
        //server.setLogWriter(new PrintWriter(new LogStream(log, LogStream.Level.INFO)));
        //server.setErrWriter(new PrintWriter(new LogStream(log, LogStream.Level.ERROR)));
        server.setTrace(false);
        server.setSilent(true);
        server.setDatabaseName(0, databaseName);
        server.setDatabasePath(0, databasePath.getAbsolutePath());
        server.setPort(port);
        server.setNoSystemExit(true);
        server.start();
        return server;
    }
}
