package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice;

import com.google.inject.Inject;
import com.google.inject.Provider;
import lombok.extern.slf4j.Slf4j;
import org.apache.cxf.endpoint.Server;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;

import javax.annotation.Nullable;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import javax.ws.rs.ext.RuntimeDelegate;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public class CxfJaxrsServerProvider implements Provider<CxfJaxrsServerProvider.ServerHolder> {

    public static class ServerHolder {
        Map<Application, Server> servers = new HashMap<>();
        public Map<Application, Server> getServers() {
            return servers;
        }
    }

    private static final String APPLICATION_PATH = "applicationPath";

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

    @Inject
    private Set<Application> applications;

    @Override
    public ServerHolder get() {
        ServerHolder serverHolder = new ServerHolder();

        /*
        JAXRSServerFactoryBean sf = new JAXRSServerFactoryBean();

        // you need to provide a default configuration
        JettyHTTPServerEngineFactory serverEngineFactory = sf.getBus().getExtension(JettyHTTPServerEngineFactory.class);

        JettyHTTPServerEngine eng = new JettyHTTPServerEngine();
        eng.setPort(0); // with the port zero
        ThreadingParameters defaultThreadingParams = new ThreadingParameters();
        defaultThreadingParams.setMinThreads(5);
        defaultThreadingParams.setMaxThreads(10);
        defaultThreadingParams.setThreadNamePrefix("myjetty");
        eng.setThreadingParameters(defaultThreadingParams);
        serverEngineFactory.setEnginesList(Arrays.asList(eng));

         */

        RuntimeDelegate delegate = RuntimeDelegate.getInstance();
        /*
        JAXRSServerFactoryBean bean = delegate.createEndpoint(application, JAXRSServerFactoryBean.class);
        bean.setAddress((cxfServerUrl == null ? "http://localhost" : cxfServerUrl)
                + ":"
                + (cxfServerPort == null ? "8080" : cxfServerPort.toString())
                + (cxfServerPath == null ? "/api" : cxfServerPath)
                + bean.getAddress());

        Server server = bean.create();
        return server;
        */
        /*
        applications.put(applicationId, application);
        if (applicationBundle != null) {
            applicationBundles.put(applicationId, applicationBundle);
        } else {
            applicationBundles.remove(applicationId);
        } */

        for (Application application : applications) {
            final Set<Class<?>> classes = application.getClasses();
            final Set<Object> singletons = application.getSingletons();

            if ((classes == null || classes.isEmpty()) && (singletons == null || singletons.isEmpty())) {
                log.warn("No resource classes found, do not start JAX-RS application");
                return null;
            }

            final JAXRSServerFactoryBean serverFactory = delegate.createEndpoint(application, JAXRSServerFactoryBean.class);

            String applicationPath = getApplicationPath(application);
            serverFactory.setAddress((cxfJaxRsServerUrl == null ? "http://localhost" : cxfJaxRsServerUrl)
                    + ":"
                    + (cxfJaxRsServerPort == null ? "8080" : cxfJaxRsServerPort.toString())
                    + (cxfJaxRsServerPath == null ? "/api" : cxfJaxRsServerPath)
                    + "/"
                    + applicationPath);


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

            final Server server = serverFactory.create();
            if (log.isDebugEnabled()) {
                log.debug("Starting JAX-RS application, service.id = " + application.getClass().getName());
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
            //log.warn("No @ApplicationPath found on component: " + application.getClass().getName());
        }
        return applicationPath;
    }
}