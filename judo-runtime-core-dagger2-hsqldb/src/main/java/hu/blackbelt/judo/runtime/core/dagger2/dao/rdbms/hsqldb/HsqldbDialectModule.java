package hu.blackbelt.judo.runtime.core.dagger2.dao.rdbms.hsqldb;

import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.dagger2.database.DatabaseScope;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbDialect;

@Module
public class HsqldbDialectModule {

    @JudoApplicationScope
    @Provides
    public Dialect providesDialect() {
        return new HsqldbDialect();
    }
}
