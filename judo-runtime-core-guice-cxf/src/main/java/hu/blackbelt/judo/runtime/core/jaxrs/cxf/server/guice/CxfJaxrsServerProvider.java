package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice;

import com.google.inject.Inject;
import com.google.inject.Provider;
import liquibase.pro.packaged.J;
import lombok.extern.slf4j.Slf4j;
import org.apache.cxf.Bus;
import org.apache.cxf.BusFactory;
import org.apache.cxf.endpoint.EndpointImpl;
import org.apache.cxf.endpoint.Server;
import org.apache.cxf.ext.logging.LoggingFeature;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.jaxrs.utils.ResourceUtils;
import org.apache.cxf.metrics.MetricsFeature;
import org.apache.cxf.transport.servlet.CXFServlet;
import org.apache.cxf.transport.servlet.ServletController;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.filter.DelegatingFilterProxy;

import javax.annotation.Nullable;
import javax.servlet.DispatcherType;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import javax.ws.rs.ext.RuntimeDelegate;
import java.util.*;

@Slf4j
public class CxfJaxrsServerProvider implements Provider<CxfJaxrsServerProvider.ServerHolder> {

    public static class ServerHolder {
        Map<Application, Server> servers = new HashMap<>();
        public Map<Application, Server> getServers() {
            return servers;
        }
    }

    private static final String APPLICATION_PATH = "applicationPath";
    private static final String SKIP_DEFAULT_JSON_PROVIDER_REGISTRATION_KEY = "skip.default.json.provider.registration";
    private static final String WADL_SERVICE_DESCRIPTION_AVAILABLE_KEY = "wadl.service.description.available";

    @Inject(optional = true)
    @CxfConfigurations.CxfJaxRsServerPort
    @Nullable
    private Integer cxfJaxRsServerPort;

    @Inject(optional = true)
    @CxfConfigurations.CxfJaxRsServerUrl
    @Nullable
    private String cxfJaxRsServerUrl;

    @Inject(optional = true)
    @CxfConfigurations.CxfJaxRsServerPath
    @Nullable
    private String cxfJaxRsServerPath;

    @Inject(optional = true)
    @CxfConfigurations.CxfSkipDefaultJsonProviderRegistration
    @Nullable
    private Boolean skipDefaultJsonProviderRegistration = false;

    @Inject(optional = true)
    @CxfConfigurations.CxfWadlServiceDescriptionAvailable
    @Nullable
    private Boolean wadlServiceDescriptionAvailable = true;

    @Inject(optional = true)
    @CxfConfigurations.CxfMetricsEnabled
    @Nullable
    private Boolean metricsEnabled = true;

    @Inject(optional = true)
    @CxfConfigurations.CxfLoggingEnabled
    @Nullable
    private Boolean loggingEnabled = true;


    @Inject
    private Set<Application> applications;

    @Override
    public ServerHolder get() {

        /*
        // Register and map the dispatcher servlet
        final ServletHolder servletHolder = new ServletHolder( new CXFServlet() );
        final ServletContextHandler context = new ServletContextHandler();
        context.setContextPath( "/" );
        context.addServlet( servletHolder, "/rest/*" );
        context.addEventListener( new ContextLoaderListener() );

        context.setInitParameter( "contextClass", AnnotationConfigWebApplicationContext.class.getName() );
        context.setInitParameter( "contextConfigLocation", AppConfig.class.getName() );

        // Add Spring Security Filter by the name
        context.addFilter(
                new FilterHolder( new DelegatingFilterProxy( "springSecurityFilterChain" ) ),
                "/*", EnumSet.allOf( DispatcherType.class )
        );

        Server server = new Server( 8080 );
        server.setHandler( context );
        server.start();

        */
        ServerHolder serverHolder = new ServerHolder();
        JettyContainer jettyContainer = new JettyContainer();
        ServletContextHandler servletContextHandler = jettyContainer.start(cxfJaxRsServerPort);
        Bus bus = BusFactory.getThreadDefaultBus();
        setupCxfBus(bus);

        //RuntimeDelegate delegate = RuntimeDelegate.getInstance();
        for (Application application : applications) {
            final Set<Class<?>> classes = application.getClasses();
            final Set<Object> singletons = application.getSingletons();

            if ((classes == null || classes.isEmpty()) && (singletons == null || singletons.isEmpty())) {
                log.warn("No resource classes found, do not start JAX-RS application");
                return null;
            }

            final JAXRSServerFactoryBean serverFactory = ResourceUtils.createApplication(application, false, false, false, bus);


            //delegate.createEndpoint(application, JAXRSServerFactoryBean.class);
            String applicationPath = getApplicationPath(application);

            /*
            serverFactory.setAddress((cxfJaxRsServerUrl == null ? "http://localhost" : cxfJaxRsServerUrl)
                    + ":"
                    + (cxfJaxRsServerPort == null ? "8080" : cxfJaxRsServerPort.toString())
                    + (cxfJaxRsServerPath == null ? "/api" : cxfJaxRsServerPath)
                    + "/"
                    + applicationPath);
            */

            /*
            final CxfContext cxfContext;
            if (properties != null && properties.containsKey(BasicApplication.CONTEXT_PROPERTY_KEY)) {
                cxfContext = (CxfContext) properties.get(BasicApplication.CONTEXT_PROPERTY_KEY);
            } else if (properties != null && properties.containsKey(CONTEXT_KEY)) {
                final Object ctx = properties.get(CONTEXT_KEY);
                cxfContext = (ctx instanceof CxfContext) ? (CxfContext) ctx : null;
                if (ctx != null) {
                    cxfContext.getBus().setExtension(new BundleDelegatingClassLoader(applicationBundle), ClassLoader.class);
                } else {
                    log.error("CXF Context is null, but set");
                }
            } else {
                cxfContext = null;
            }
            if (cxfContext != null) {
                serverFactory.setBus(cxfContext.getBus());

                if (log.isTraceEnabled()) {
                    log.trace("IN interceptors: {}", cxfContext.getInInterceptors());
                    log.trace("OUT interceptors: {}", cxfContext.getOutInterceptors());
                    log.trace("FAULT interceptors: {}", cxfContext.getFaultInterceptors());
                }

                serverFactory.setInInterceptors(cxfContext.getInInterceptors());
                serverFactory.setOutInterceptors(cxfContext.getOutInterceptors());
                serverFactory.setOutFaultInterceptors(cxfContext.getFaultInterceptors());
            }

            final List<Object> _providers;
            if (providers == null) {
                _providers = applicationProviders.containsKey(applicationId) ? applicationProviders.get(applicationId) : providers;
            } else {
                _providers = providers;
            }
            serverFactory.setProviders(_providers);
            applicationProviders.put(applicationId, _providers);
            */

            //Bus bus = BusFactory.getThreadDefaultBus();
            //serverFactory.setBus(bus);
            final Server server = serverFactory.create();

            if (log.isDebugEnabled()) {
                log.info("Starting JAX-RS application, service class = " + application.getClass().getName() + " on path: " + applicationPath);
            }

            server.start();
            serverHolder.getServers().put(application, server);
        }
        return serverHolder;
    }

    private static String getApplicationPath(Application application) {
        final Map<String, Object> properties = application.getProperties();
        String applicationPath = properties != null ? (String) properties.get(APPLICATION_PATH) : null;

        if (application.getClass().isAnnotationPresent(ApplicationPath.class)) {
            ApplicationPath ap = application.getClass().getAnnotation(ApplicationPath.class);
            applicationPath = ap.value();
        }
        return applicationPath;
    }

    void setupCxf(Bus bus, ServletContextHandler applicationContext) {
//        System.setProperty(BusFactory.BUS_FACTORY_PROPERTY_NAME, CXFBusFactory.class.getName());
//        bus = BusFactory.getDefaultBus(true);
        final CXFServlet cxfServlet = new CXFServlet();
        cxfServlet.setBus(bus);
        final ServletHolder cxfServletHolder = new ServletHolder(cxfServlet);
        cxfServletHolder.setName(cxfJaxRsServerPath);
        cxfServletHolder.setForcedPath(cxfJaxRsServerPath);
        applicationContext.addServlet(cxfServletHolder, "/" + cxfJaxRsServerPath + "/*");
        log.info("Found request listners. Adding to the context.");
        BusFactory.setDefaultBus(bus);

    }

    void setupCxfBus(Bus bus) {

        if (skipDefaultJsonProviderRegistration != null) {
            bus.setProperty(SKIP_DEFAULT_JSON_PROVIDER_REGISTRATION_KEY, skipDefaultJsonProviderRegistration);
        }
        if (wadlServiceDescriptionAvailable != null) {
            bus.setProperty(WADL_SERVICE_DESCRIPTION_AVAILABLE_KEY, wadlServiceDescriptionAvailable);
        }
        if (metricsEnabled) {
            bus.getFeatures().add(new MetricsFeature());
        } else {
            bus.getFeatures().removeIf(f -> f instanceof MetricsFeature);
        }
        if (loggingEnabled) {
            bus.getFeatures().add(new LoggingFeature());
        } else {
            bus.getFeatures().removeIf(f -> f instanceof LoggingFeature);
        }

        /*
        log.debug("IN interceptors have been changed");
        inInterceptorsFilter = newInInterceptorsFilter;
        updated = true;
        if (inInterceptorTracker != null) {
            inInterceptorTracker.close();
            inInterceptorTracker = null;
        }
        if (inInterceptorsFilter != null && !inInterceptorsFilter.trim().isEmpty()) {
            try {
                inInterceptorTracker = new InterceptorTracker(context, inInterceptorsFilter, inInterceptors);
                inInterceptorTracker.open();
            } catch (InvalidSyntaxException ex) {
                log.error("Invalid IN interceptor filter, ignore it", ex);
            }
        }

        final String newOutInterceptorsFilter = config.interceptors_out_components();
        if (!Objects.equals(outInterceptorsFilter, newOutInterceptorsFilter)) {
            log.debug("OUT interceptors have been changed");
            outInterceptorsFilter = newOutInterceptorsFilter;
            updated = true;
            if (outInterceptorTracker != null) {
                outInterceptorTracker.close();
                outInterceptorTracker = null;
            }
            if (outInterceptorsFilter != null && !outInterceptorsFilter.trim().isEmpty()) {
                try {
                    outInterceptorTracker = new InterceptorTracker(context, outInterceptorsFilter, outInterceptors);
                    outInterceptorTracker.open();
                } catch (InvalidSyntaxException ex) {
                    log.error("Invalid OUT interceptor filter, ignore it", ex);
                }
            }
        }
        final String newFaultInterceptorsFilter = config.interceptors_fault_components();
        if (!Objects.equals(faultInterceptorsFilter, newFaultInterceptorsFilter)) {
            log.debug("FAULT interceptors have been changed");
            faultInterceptorsFilter = newFaultInterceptorsFilter;
            updated = true;
            if (faultInterceptorTracker != null) {
                faultInterceptorTracker.close();
                faultInterceptorTracker = null;
            }
            if (faultInterceptorsFilter != null && !faultInterceptorsFilter.trim().isEmpty()) {
                try {
                    faultInterceptorTracker = new InterceptorTracker(context, faultInterceptorsFilter, faultInterceptors);
                    faultInterceptorTracker.open();
                } catch (InvalidSyntaxException ex) {
                    log.error("Invalid FAULT interceptor filter, ignore it", ex);
                }
            }
        }

        if (updated) {
            log.debug("CXF bus registered: {} [{}={}; {}={}]", id, SKIP_DEFAULT_JSON_PROVIDER_REGISTRATION_KEY, skipDefaultJsonProviderRegistration, WADL_SERVICE_DESCRIPTION_AVAILABLE_KEY, wadlServiceDescriptionAvailable);
        } */
    }
}