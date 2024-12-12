package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.providers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.interceptors.JudoAuthorizingInterceptor;

public class JudoAuthorizingInterceptorProvider implements Provider<JudoAuthorizingInterceptor> {
    @Inject
    JudoModelLoader judoModelLoader;

    @Override
    public JudoAuthorizingInterceptor get() {
        return JudoAuthorizingInterceptor.builder()
                .asmModel(judoModelLoader.getAsmModel())
                .build();
    }
}
