package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.providers;

import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.interceptors.FaultInterceptor;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.interceptors.ExchangeIdDecorator;


public class FaultInterceptorProvider implements Provider<FaultInterceptor> {

    public FaultInterceptor get() {
        return new FaultInterceptor();
    }
}
