package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.providers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.CxfConfigurations;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.CxfQualifiers;
import hu.blackbelt.judo.runtime.core.jetty.guice.JettyContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.cxf.Bus;
import org.apache.cxf.BusFactory;
import org.apache.cxf.endpoint.Server;
import org.apache.cxf.ext.logging.LoggingFeature;
import org.apache.cxf.interceptor.Interceptor;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.message.Message;
import org.apache.cxf.metrics.MetricsFeature;
import org.apache.cxf.transport.servlet.CXFServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.annotation.Nullable;
import javax.ws.rs.core.Application;
import javax.ws.rs.ext.RuntimeDelegate;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class CxfJaxrsServerProvider implements Provider<CxfJaxrsServerProvider.ServerHolder> {

    public static class ServerHolder {
        Map<Application, Server> servers = new HashMap<>();
        public Map<Application, Server> getServers() {
            return servers;
        }
    }

    private static final String SKIP_DEFAULT_JSON_PROVIDER_REGISTRATION_KEY = "skip.default.json.provider.registration";
    private static final String WADL_SERVICE_DESCRIPTION_AVAILABLE_KEY = "wadl.service.description.available";

    @Inject
    ObjectMapper objectMapper;

    @Inject
    JettyContainer jettyContainer;

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

    @Inject(optional = true)
    @Nullable
    private Set<Application> applications = new HashSet<>();

    @Inject(optional = true)
    @CxfQualifiers.InInterceptors
    @Nullable
    private Set<Interceptor> inInterceptors = new HashSet<>();

    @Inject(optional = true)
    @CxfQualifiers.OutInterceptors
    @Nullable
    private Set<Interceptor> outInterceptors = new HashSet<>();

    @Inject(optional = true)
    @CxfQualifiers.FaultInterceptors
    @Nullable
    private Set<Interceptor> faultInterceptors = new HashSet<>();

    @Inject(optional = true)
    @CxfQualifiers.Providers
    @Nullable
    private Set<Object> providers = new HashSet<>();

    @Override
    public ServerHolder get() {
        ServerHolder serverHolder = new ServerHolder();
//        JettyContainer jettyContainer = new JettyContainer(cxfJaxRsServerPort, "/");
        ServletContextHandler servletContextHandler = jettyContainer.getServletContextHandler();

        Bus bus = BusFactory.getThreadDefaultBus();
        setupCxfBus(bus);
        setupCxf(bus, servletContextHandler);


        RuntimeDelegate delegate = RuntimeDelegate.getInstance();
        for (Application application : applications) {
            final Set<Class<?>> classes = application.getClasses();
            final List<Object> singletons = new ArrayList<>(application.getSingletons());

            if ((classes == null || classes.isEmpty()) && (singletons == null || singletons.isEmpty())) {
                log.warn("No resource classes found, do not start JAX-RS application");
                return null;
            }

            final JAXRSServerFactoryBean serverFactory = delegate.createEndpoint(application, JAXRSServerFactoryBean.class);
            serverFactory.setBus(bus);

            setupCxfInterceptors(serverFactory);
            setupCxfProviders(serverFactory);
            setupCxFeatures(serverFactory);

            final Server server = serverFactory.create();
            log.info("Starting JAX-RS application, service class = " + application.getClass().getName() + " on path: " + serverFactory.getAddress());
            server.start();
            serverHolder.getServers().put(application, server);
        }
        return serverHolder;
    }

    void setupCxf(Bus bus, ServletContextHandler applicationContext) {
        final CXFServlet cxfServlet = new CXFServlet();
        cxfServlet.setBus(bus);
        final ServletHolder cxfServletHolder = new ServletHolder(cxfServlet);
        cxfServletHolder.setName(cxfJaxRsServerPath);
        cxfServletHolder.setForcedPath(cxfJaxRsServerPath);
        applicationContext.addServlet(cxfServletHolder, "/" + cxfJaxRsServerPath + "/*");
        log.info("Found request listners. Adding to the context.");
        BusFactory.setDefaultBus(bus);
    }

    void setupCxfInterceptors(JAXRSServerFactoryBean jaxrsServerFactoryBean) {
        log.info("Registering IN INTERCEPTORS: " + inInterceptors.stream().map(i -> i.getClass().getName()).collect(Collectors.joining(", ")));
        jaxrsServerFactoryBean.getInInterceptors().addAll(inInterceptors.stream().map(m -> (Interceptor<Message>) m).toList());
        log.info("Registering OUT INTERCEPTORS: " + outInterceptors.stream().map(i -> i.getClass().getName()).collect(Collectors.joining(", ")));
        jaxrsServerFactoryBean.getOutInterceptors().addAll(outInterceptors.stream().map(m -> (Interceptor<Message>) m).toList());
        log.info("Registering FAULT INTERCEPTORS: " + faultInterceptors.stream().map(i -> i.getClass().getName()).collect(Collectors.joining(", ")));
        jaxrsServerFactoryBean.getOutFaultInterceptors().addAll(faultInterceptors.stream().map(m -> (Interceptor<Message>) m).toList());
    }

    void setupCxfProviders(JAXRSServerFactoryBean jaxrsServerFactoryBean) {
        log.info("Registering PROVIDER: " + providers.stream().map(i -> i.getClass().getName()).collect(Collectors.joining(", ")));
        jaxrsServerFactoryBean.setProviders(providers.stream().toList());
    }

    void setupCxFeatures(JAXRSServerFactoryBean jaxrsServerFactoryBean) {
        if (metricsEnabled) {
            jaxrsServerFactoryBean.getFeatures().add(new MetricsFeature());
        } else {
            jaxrsServerFactoryBean.getFeatures().removeIf(f -> f instanceof MetricsFeature);
        }
        if (loggingEnabled) {
            jaxrsServerFactoryBean.getFeatures().add(new LoggingFeature());
        } else {
            jaxrsServerFactoryBean.getFeatures().removeIf(f -> f instanceof LoggingFeature);
        }
    }

    void setupCxfBus(Bus bus) {
        if (skipDefaultJsonProviderRegistration != null) {
            bus.setProperty(SKIP_DEFAULT_JSON_PROVIDER_REGISTRATION_KEY, skipDefaultJsonProviderRegistration);
        }
        if (wadlServiceDescriptionAvailable != null) {
            bus.setProperty(WADL_SERVICE_DESCRIPTION_AVAILABLE_KEY, wadlServiceDescriptionAvailable);
        }
    }
}