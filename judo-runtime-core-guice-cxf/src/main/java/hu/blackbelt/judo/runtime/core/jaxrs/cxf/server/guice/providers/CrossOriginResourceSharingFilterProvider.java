package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.providers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.CxfConfigurations;
import org.apache.cxf.rs.security.cors.CrossOriginResourceSharingFilter;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.AttributeType;

import javax.annotation.Nullable;
import java.util.Arrays;

public class CrossOriginResourceSharingFilterProvider implements Provider<CrossOriginResourceSharingFilter> {

    // @AttributeDefinition(name = "CORS allow origin", description = "Comma-separated list of Access-Control-Allow-Origin")
    @Inject(optional = true)
    @CxfConfigurations.CxfCorsAllowOrigin
    @Nullable
    String corsAllowOrigin = "*";

    // @AttributeDefinition(name = "CORS allow headers", description = "Access-Control-Allow-Credentials", type = AttributeType.BOOLEAN)
    @Inject(optional = true)
    @CxfConfigurations.CxfCorsAllowCredentials
    @Nullable
    Boolean corsAllowCredentials = true;

    // @AttributeDefinition(name = "CORS allow headers", description = "Comma-separated list of Access-Control-Allow-Headers")
    @Inject(optional = true)
    @CxfConfigurations.CxfCorsAllowHeaders
    @Nullable
    String corsAllowHeaders = "Content-Type,Origin,Accept,Authorization,X-Judo-SignedIdentifier,X-Judo-CountRecords";

    // @AttributeDefinition(name = "CORS expose headers", description = "Comma-separated list of Access-Control-Expose-Headers")
    @Inject(optional = true)
    @CxfConfigurations.CxfCorsExposeHeaders
    @Nullable
    String corsExposeHeaders ="X-Exchange-Id,X-Fault,X-Judo-Count";

    // @AttributeDefinition(name = "CORS max age", description = "Access-Control-Max-Age")
    @Inject(optional = true)
    @CxfConfigurations.CxfCorsMaxAge
    @Nullable
    Integer corsMaxAge = -1;

    // @AttributeDefinition(name = "CORS preflight error code", description = "HTTP status code returned by failed prefligth requests", type = AttributeType.INTEGER)
    @Inject(optional = true)
    @CxfConfigurations.CxfCorsPrefligthErrorStatus
    @Nullable
    Integer corsPrefligthErrorStatus = 400;

    // @AttributeDefinition(name = "Block CORS if unauthorized", type = AttributeType.BOOLEAN)
    @Inject(optional = true)
    @CxfConfigurations.CxfCorsBlockIfUnauthorized
    @Nullable
    Boolean corsBlockIfUnauthorized = false;

    // @AttributeDefinition(name = "Default OPTIONS preflight", type = AttributeType.BOOLEAN)
    @Inject(optional = true)
    @CxfConfigurations.CxfCorsDefaultOptionsMethodsHandlePreflight
    @Nullable
    Boolean corsDefaultOptionsMethodsHandlePreflight = false;

    @Override
    public CrossOriginResourceSharingFilter get() {
        CrossOriginResourceSharingFilter crossOriginResourceSharingFilter = new CrossOriginResourceSharingFilter();
        crossOriginResourceSharingFilter.setAllowOrigins(Arrays.stream(corsAllowOrigin.split(",")).map(s -> s.trim()).toList());
        crossOriginResourceSharingFilter.setAllowCredentials(corsAllowCredentials);
        crossOriginResourceSharingFilter.setAllowHeaders(Arrays.stream(corsAllowHeaders.split(",")).map(s -> s.trim()).toList());
        crossOriginResourceSharingFilter.setExposeHeaders(Arrays.stream(corsExposeHeaders.split(",")).map(s -> s.trim()).toList());
        crossOriginResourceSharingFilter.setMaxAge(corsMaxAge);
        crossOriginResourceSharingFilter.setBlockCorsIfUnauthorized(corsBlockIfUnauthorized);
        crossOriginResourceSharingFilter.setDefaultOptionsMethodsHandlePreflight(corsDefaultOptionsMethodsHandlePreflight);
        crossOriginResourceSharingFilter.setPreflightErrorStatus(corsPrefligthErrorStatus);
        return new CrossOriginResourceSharingFilter();
    }
}
