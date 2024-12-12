package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.providers;

import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.interceptors.ExchangeIdResponseWriter;


public class ExchangeIdResponseWriterProviderOut implements Provider<ExchangeIdResponseWriter> {

    public ExchangeIdResponseWriter get() {
        return new ExchangeIdResponseWriter();
    }
}
