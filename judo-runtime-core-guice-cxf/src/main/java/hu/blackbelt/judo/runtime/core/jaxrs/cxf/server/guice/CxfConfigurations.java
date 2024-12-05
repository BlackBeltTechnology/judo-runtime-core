package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice;

import javax.inject.Qualifier;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

public class CxfConfigurations {

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    @interface CxfJaxRsServerPort {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    @interface CxfJaxRsServerUrl {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    @interface CxfJaxRsServerPath {}

}
