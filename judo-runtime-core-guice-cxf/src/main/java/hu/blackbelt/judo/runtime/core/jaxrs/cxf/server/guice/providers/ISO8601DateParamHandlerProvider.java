package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.providers;

import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.jaxrs.providers.ISO8601DateParamHandler;

public class ISO8601DateParamHandlerProvider implements Provider<ISO8601DateParamHandler> {
    @Override
    public ISO8601DateParamHandler get() {
        return new ISO8601DateParamHandler();
    }
}
