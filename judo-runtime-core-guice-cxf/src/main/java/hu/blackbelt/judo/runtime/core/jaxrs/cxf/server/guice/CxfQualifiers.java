package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice;

import javax.inject.Qualifier;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

public class CxfQualifiers {

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface InInterceptors {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface OutInterceptors {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface FaultInterceptors {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Providers {}


}
