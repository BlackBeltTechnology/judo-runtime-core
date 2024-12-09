package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.providers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.CxfConfigurations;
import hu.blackbelt.judo.runtime.core.jaxrs.providers.PayloadMessageBodyWriter;

import javax.annotation.Nullable;

public class PayloadMessageBodyWriterProvider implements Provider<PayloadMessageBodyWriter> {
    @Inject
    ObjectMapper objectMapper;

    @Inject(optional = true)
    @CxfConfigurations.CxfLogException
    @Nullable
    private Boolean cxfLogException;

    @Override
    public PayloadMessageBodyWriter get() {
        PayloadMessageBodyWriter writer = PayloadMessageBodyWriter.builder()
                .objectMapper(objectMapper)
                .build();
        return writer;
    }
}
