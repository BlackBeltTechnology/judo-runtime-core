package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.providers;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class JacksonJaxbJsonProviderProvider implements Provider<JacksonJaxbJsonProvider> {
    @Inject
    ObjectMapper objectMapper;

    @Override
    public JacksonJaxbJsonProvider get() {
        JacksonJaxbJsonProvider jacksonJaxbJsonProvider = new JacksonJaxbJsonProvider(objectMapper, JacksonJaxbJsonProvider.DEFAULT_ANNOTATIONS);
        jacksonJaxbJsonProvider.configure(SerializationFeature.INDENT_OUTPUT, false);
        jacksonJaxbJsonProvider.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        jacksonJaxbJsonProvider.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return jacksonJaxbJsonProvider;
    }
}
