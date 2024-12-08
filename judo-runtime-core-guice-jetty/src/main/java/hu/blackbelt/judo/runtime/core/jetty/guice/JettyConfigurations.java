package hu.blackbelt.judo.runtime.core.jetty.guice;

import com.google.inject.Inject;
import lombok.Getter;

import javax.annotation.Nullable;
import javax.inject.Qualifier;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

public class JettyConfigurations {

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface JettyServerPort {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface JettyServerContextPath {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface JettyServerMaxThreads {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface JettyServerMinThreads {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface JettyServerIdleTimeout {}

}
