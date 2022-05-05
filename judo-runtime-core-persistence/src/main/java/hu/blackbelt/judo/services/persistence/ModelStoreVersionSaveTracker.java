package hu.blackbelt.judo.services.persistence;

import hu.blackbelt.judo.rdbms.schema.modelstore.ModelStoreRepository;
import hu.blackbelt.judo.rdbms.schema.modelstore.ModelVersion;
import hu.blackbelt.judo.rdbms.schema.modelstore.VersionFactory;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.xmi.XMLResource;
import org.eclipse.emf.ecore.xmi.impl.URIHandlerImpl;
import org.osgi.framework.*;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.*;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class ModelStoreVersionSaveTracker {

    @Reference
    DataSource dataSource;

    ModelServiceTracker modelServiceTracker;

    ModelStoreRepository modelStoreRepository;

    Version version;
    String table;
    String name;

    @Activate
    public void activate(ComponentContext componentContext) throws InvalidSyntaxException {
        String trackedModelTypesAsSting = (String) componentContext.getProperties().get("trackedModelTypes");
        String[] trackedModelTypes = new String[] {
                "hu.blackbelt.judo.meta.esm.runtime.EsmModel",
                "hu.blackbelt.judo.meta.psm.runtime.PsmModel",
                "hu.blackbelt.judo.meta.asm.runtime.AsmModel"
        };

        if (trackedModelTypesAsSting != null && !trackedModelTypesAsSting.trim().equals("")) {
            trackedModelTypes = trackedModelTypesAsSting.split(",");
        }
        String versionToTrack = (String) componentContext.getProperties().get("version");

        version = VersionFactory.createVersion(versionToTrack);
        table = (String) componentContext.getProperties().get("table");
        name = (String) componentContext.getProperties().get("name");

        Filter filter = componentContext.getBundleContext().createFilter("(&(|" +
                Arrays.stream(trackedModelTypes).map(s -> "(objectClass=" + s + ")").collect(Collectors.joining()) +
                ")(version=" + versionToTrack + ")(name=" + name + "))");

        modelStoreRepository = new ModelStoreRepository(dataSource, table);

        modelServiceTracker = new ModelServiceTracker(componentContext.getBundleContext(), filter);
        modelServiceTracker.open(true);


    }

    @Deactivate
    public void deactivate(ComponentContext componentContext) throws InvalidSyntaxException {
        if (modelServiceTracker != null) {
            modelServiceTracker.close();
        }
    }

    public class ModelServiceTracker extends ServiceTracker {

        ModelServiceTracker(BundleContext bundleContext, Filter trackedTypesFileter) throws InvalidSyntaxException {
            super(bundleContext, trackedTypesFileter, (ServiceTrackerCustomizer)null);
        }

        @Override
        public Object addingService(ServiceReference serviceReference) {
            Object instance = super.addingService(serviceReference);
            saveModel(serviceReference);
            return instance;
        }

        @Override
        public void removedService(ServiceReference serviceReference, Object service) {
            super.removedService(serviceReference, service);
        }

        @Override
        public void modifiedService(ServiceReference serviceReference, Object service) {
            super.modifiedService(serviceReference, service);
        }
    }


    public void saveModel(ServiceReference serviceReference) {
        Object resourceSetObject = serviceReference.getProperty("resourceset");
        Object uriObject = serviceReference.getProperty("uri");

        if (resourceSetObject != null && uriObject != null && resourceSetObject instanceof ResourceSet) {
            ResourceSet resourceSet = (ResourceSet) resourceSetObject;
            URI uri = (URI) uriObject;
            Resource resource = resourceSet.getResource(uri, false);
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try {
                resource.save(byteArrayOutputStream, getSaveOptions());
            } catch (Exception e) {
                log.warn("Could not store model: " + uri.toString());
            }

            // Calculate type
            String[] parts = uri.toString().substring(0, uri.toString().lastIndexOf('.')).split("-");
            String type = parts[parts.length - 1];

            modelStoreRepository.addEntryToVersion(
                    name,
                    version,
                    ModelVersion.ModelEntry.buildEntry()
                            .modelType(type)
                            .data(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()))
                            .build());
        }
    }

    public static Map<Object, Object> getSaveOptions() {
        Map<Object, Object> saveOptions = new HashMap<>();
        saveOptions.put(XMLResource.OPTION_DECLARE_XML, Boolean.TRUE);
        saveOptions.put(XMLResource.OPTION_PROCESS_DANGLING_HREF, XMLResource.OPTION_PROCESS_DANGLING_HREF_DISCARD);
        saveOptions.put(XMLResource.OPTION_URI_HANDLER, new URIHandlerImpl() {
            public URI deresolve(URI uri) {
                return uri.hasFragment()
                        && uri.hasOpaquePart()
                        && this.baseURI.hasOpaquePart()
                        && uri.opaquePart().equals(this.baseURI.opaquePart())
                        ? URI.createURI("#" + uri.fragment())
                        : super.deresolve(uri);
            }
        });
        saveOptions.put(XMLResource.OPTION_SCHEMA_LOCATION, Boolean.TRUE);
        saveOptions.put(XMLResource.OPTION_DEFER_IDREF_RESOLUTION, Boolean.TRUE);
        saveOptions.put(XMLResource.OPTION_SKIP_ESCAPE_URI, Boolean.FALSE);
        saveOptions.put(XMLResource.OPTION_ENCODING, "UTF-8");
        return saveOptions;
    }

}
