package hu.blackbelt.judo.runtime.core.guice.dao.rdbms.hsqldb;

import javax.inject.Qualifier;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public class HsqlDbConfiguration {

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface HsqldbServerDatabaseName {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface HsqldbServerDatabasePath {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface HsqldbServerPort {}

}
