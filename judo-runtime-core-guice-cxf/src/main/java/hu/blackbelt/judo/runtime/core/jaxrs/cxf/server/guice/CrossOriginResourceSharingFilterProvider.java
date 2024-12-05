package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice;

import com.google.inject.Provider;
import org.apache.cxf.rs.security.cors.CrossOriginResourceSharingFilter;

public class CrossOriginResourceSharingFilterProvider implements Provider<CrossOriginResourceSharingFilter> {
    @Override
    public CrossOriginResourceSharingFilter get() {
        return new CrossOriginResourceSharingFilter();
    }
}
