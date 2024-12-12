package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.providers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.CxfConfigurations;
import hu.blackbelt.judo.runtime.core.jaxrs.providers.SetDefaultContentTypePreMatchContainerRequestFilter;

import javax.annotation.Nullable;

public class SetDefaultContentTypePreMatchContainerRequestFilterProvider implements Provider<SetDefaultContentTypePreMatchContainerRequestFilter> {
    @Inject(optional = true)
    @CxfConfigurations.CxfDefaultRequestContentType
    @Nullable
    private String defaultRequestContentType;

    @Override
    public SetDefaultContentTypePreMatchContainerRequestFilter get() {
        return SetDefaultContentTypePreMatchContainerRequestFilter.builder()
                .defaultRequestContentType(defaultRequestContentType)
                .build();
    }
}
