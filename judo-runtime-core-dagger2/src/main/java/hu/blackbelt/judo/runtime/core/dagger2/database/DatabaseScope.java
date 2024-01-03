package hu.blackbelt.judo.runtime.core.dagger2.database;

import javax.inject.Scope;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Scope
@Retention(RetentionPolicy.RUNTIME)
public @interface DatabaseScope {
}