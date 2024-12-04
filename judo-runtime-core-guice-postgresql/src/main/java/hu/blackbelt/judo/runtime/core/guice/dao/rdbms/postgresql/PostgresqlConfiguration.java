package hu.blackbelt.judo.runtime.core.guice.dao.rdbms.postgresql;

import com.google.inject.name.Names;

import javax.inject.Qualifier;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public class PostgresqlConfiguration {

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface PostgresqlPort {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface PostgresqlHost {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface PostgresqlUser {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface PostgresqlPassword {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface PostgresqlDatabaseName {}

}
