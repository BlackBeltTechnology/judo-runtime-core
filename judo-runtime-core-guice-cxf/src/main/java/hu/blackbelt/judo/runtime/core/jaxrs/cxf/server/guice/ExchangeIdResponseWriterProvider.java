package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice;

import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.interceptors.ExchangeIdResponseWriter;


public class ExchangeIdResponseWriterProvider implements Provider<ExchangeIdResponseWriter> {

    public ExchangeIdResponseWriter get() {
        return new ExchangeIdResponseWriter();
    }
}
