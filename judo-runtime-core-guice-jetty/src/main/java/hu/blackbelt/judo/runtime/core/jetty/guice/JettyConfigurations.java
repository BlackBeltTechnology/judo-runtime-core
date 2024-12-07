package hu.blackbelt.judo.runtime.core.jetty.guice;

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

}
