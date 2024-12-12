package hu.blackbelt.judo.runtime.core.jetty.guice;

import com.google.inject.Inject;
import com.google.inject.Provider;

import javax.annotation.Nullable;

public class JettyContainerProvider implements Provider<JettyContainer> {

    @Inject(optional = true)
    @JettyConfigurations.JettyServerPort
    @Nullable
    private Integer port;

    @Inject(optional = true)
    @JettyConfigurations.JettyServerContextPath
    @Nullable
    private String contextPath;

    @Override
    public JettyContainer get() {
        return JettyContainer.builder()
                .port(port)
                .contextPath(contextPath)
                .build();
    }
}
