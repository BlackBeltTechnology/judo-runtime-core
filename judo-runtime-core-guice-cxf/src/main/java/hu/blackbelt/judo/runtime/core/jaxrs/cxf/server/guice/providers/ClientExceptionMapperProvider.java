package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.providers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.CxfConfigurations;
import hu.blackbelt.judo.runtime.core.jaxrs.providers.ClientExceptionMapper;

import javax.annotation.Nullable;

public class ClientExceptionMapperProvider implements Provider<ClientExceptionMapper> {

    @Inject(optional = true)
    @CxfConfigurations.CxfLogException
    @Nullable
    private Boolean cxfLogException;

    @Override
    public ClientExceptionMapper get() {
        ClientExceptionMapper mapper = new ClientExceptionMapper();
        mapper.setLogException(cxfLogException);
        return mapper;
    }
}
