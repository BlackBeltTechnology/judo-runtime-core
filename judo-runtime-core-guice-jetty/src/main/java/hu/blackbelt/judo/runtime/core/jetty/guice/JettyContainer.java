package hu.blackbelt.judo.runtime.core.jetty.guice;

import com.google.inject.Inject;
import com.google.inject.Injector;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class JettyContainer {
    private Server webServer;

    @Getter
    private ServletContextHandler servletContextHandler;

    @Getter
    @Inject(optional = true)
    @JettyConfigurations.JettyServerPort
    @Nullable
    private Integer port;

    @Getter
    @Inject(optional = true)
    @JettyConfigurations.JettyServerContextPath
    @Nullable
    private String contextPath;

    public static class JettyContainerBuilder {
        private Integer port = 8181;
        private String contextPath = "/";
    }

    @Builder
    public JettyContainer(int port, String contextPath) {
        this.port = port;
        this.contextPath = contextPath;
        start();
    }

    public void start() {
        try {
            if (port <= 0) {
                port = 8080;
            }
            webServer = new Server();
            webServer.setConnectors(assembleConnectors(port, webServer));
            servletContextHandler= new ServletContextHandler(ServletContextHandler.SESSIONS);
            servletContextHandler.setContextPath(contextPath);
            servletContextHandler.setSessionHandler(new SessionHandler());
            webServer.setHandler(servletContextHandler);
            webServer.start();
            log.info(String.format("HTTP Server now running on http://localhost:%s", port));

        } catch (Exception e) {
            log.error("Unable to start webserver", e);
            stop();
            log.error("The webServer was not started.");
        }
    }

    public void stop() {
        if (webServer != null) {
            if (!webServer.isRunning()) {
                return;
            }
            try {
                while (!webServer.isStopped()) {
                    webServer.stop();
                }
            } catch (Exception e) {
                log.error("Unable to stop the running server.", e);
            }
        }
    }

    protected Connector[] assembleConnectors(final int port, final Server server) {
        if (port == 0) {
            throw new IllegalArgumentException("The arguments is null.");
        }
        final List<Connector> connectors = new ArrayList<>();
        final ServerConnector httpConnector = new ServerConnector(server);
        httpConnector.setPort(port);
        connectors.add(httpConnector);
        if (connectors.isEmpty()) {
            throw new RuntimeException("No controllers defined, even though they were expected to be.");
        }
        return connectors.toArray(new Connector[connectors.size()]);
    }
}