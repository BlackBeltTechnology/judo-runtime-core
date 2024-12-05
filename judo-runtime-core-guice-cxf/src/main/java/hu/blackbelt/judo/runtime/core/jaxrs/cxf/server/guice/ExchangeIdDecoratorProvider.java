package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice;

import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.interceptors.ExchangeIdDecorator;


public class ExchangeIdDecoratorProvider implements Provider<ExchangeIdDecorator> {

    public ExchangeIdDecorator get() {
        return new ExchangeIdDecorator();
    }
}
