package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice;

import lombok.extern.slf4j.Slf4j;
import org.apache.cxf.Bus;
import org.apache.cxf.BusFactory;
import org.apache.cxf.bus.CXFBusFactory;
import org.apache.cxf.transport.servlet.CXFServlet;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.ws.Endpoint;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class JettyContainer {

//    private static final String SLASH = "/";
//    private static final String CONTEXT_PATH = SLASH;
//    private static final String WEB_SERVICE_URI = "api";
//    private static final String WS_PATH_SPEC = "/" + WEB_SERVICE_URI + "/*";

    private Server webServer;
    private final ServletContextHandler applicationContext = new ServletContextHandler(ServletContextHandler.SESSIONS);

    public ServletContextHandler start(int port, String contextPath, String webServiceUri) {
        try {
            if (port <= 0) {
                port = 8080;
            }
            webServer = new Server();
            webServer.setConnectors(assembleConnectors(port, webServer));
            applicationContext.setContextPath(contextPath);
            applicationContext.setSessionHandler(new SessionHandler());
            webServer.setHandler(applicationContext);
            webServer.start();
            log.info(String.format("Server now running on http://localhost:%s", port));
            log.info(String.format("Access REST-services on anywhereBut(http://localhost:%s/%s/*)", port, webServiceUri));
            return applicationContext;

        } catch (Exception e) {
            log.error("Unable to start webserver", e);
            stop();
            log.error("The webServer was not started.");
        }
        return null;
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

    /*
    protected void addCxf(final ServletContextHandler applicationContext) {
        System.setProperty(BusFactory.BUS_FACTORY_PROPERTY_NAME, CXFBusFactory.class.getName());
        bus = BusFactory.getDefaultBus(true);
        final CXFServlet cxfServlet = new CXFServlet();
        cxfServlet.setBus(bus);
        final ServletHolder cxfServletHolder = new ServletHolder(cxfServlet);
        cxfServletHolder.setName(WEB_SERVICE_URI);
        cxfServletHolder.setForcedPath(WEB_SERVICE_URI);
        applicationContext.addServlet(cxfServletHolder, WS_PATH_SPEC);
        log.info("Found request listners. Adding to the context.");
        BusFactory.setDefaultBus(bus);
//        initializeSoapServices();
    }
    */

    /*
    private void initializeSoapServices() {
        final ValidationService kdaIamWebService = new ValidationService();
        publishService(kdaIamWebService);
    }

    private void publishService(final AbstractWebService service) {
        Endpoint.publish(String.format("%s%s", SLASH, ENDPOINT_URI), service);
    }
     */

}