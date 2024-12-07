package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.providers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.CxfConfigurations;
import hu.blackbelt.judo.runtime.core.jaxrs.providers.ClientExceptionMapper;
import hu.blackbelt.judo.runtime.core.jaxrs.providers.RuntimeExceptionMapper;

import javax.annotation.Nullable;

public class RuntimeExceptionMapperProvider implements Provider<RuntimeExceptionMapper> {

    @Inject(optional = true)
    @CxfConfigurations.CxfReturnRuntimeExceptions
    @Nullable
    private Boolean returnRuntimeExceptions;

    @Inject(optional = true)
    @CxfConfigurations.CxfIncludeBusinessCause
    @Nullable
    private Boolean includeBusinessCause;

    @Override
    public RuntimeExceptionMapper get() {
        RuntimeExceptionMapper mapper = RuntimeExceptionMapper.builder()
                .returnRuntimeExceptions(returnRuntimeExceptions)
                .includeBusinessCause(includeBusinessCause)
                .build();
        return mapper;
    }
}
