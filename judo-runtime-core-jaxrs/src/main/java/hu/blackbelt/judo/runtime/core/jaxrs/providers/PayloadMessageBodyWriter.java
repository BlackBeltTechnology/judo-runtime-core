package hu.blackbelt.judo.runtime.core.jaxrs.providers;

import com.fasterxml.jackson.databind.ObjectMapper;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dao.api.PayloadImpl;
import lombok.Builder;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

@Provider
@Builder
@Slf4j
public class PayloadMessageBodyWriter implements MessageBodyWriter<PayloadImpl> {

    @Builder.Default
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return type == Payload.class || type == PayloadImpl.class;
    }

    @Override
    public void writeTo(PayloadImpl payload, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(entityStream);
        objectMapper.writeValue(outputStreamWriter, payload);
    }
}
