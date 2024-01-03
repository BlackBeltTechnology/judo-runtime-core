package hu.blackbelt.judo.runtime.core.dagger2.dao.rdbms.hsqldb;

import dagger.Component;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.dagger2.Utils;
import hu.blackbelt.judo.runtime.core.dagger2.database.Database;
import hu.blackbelt.judo.runtime.core.dagger2.ModelHolder;

@JudoApplicationScope
@Component(modules = {
                HsqldbConfigurationModule.class,
                HsqldbDialectModule.class,
                HsqldbDataSourceModule.class,
                HsqldbMapperFactoryModule.class,
                HsqldbRdbmsInitModule.class,
                HsqldbRdbmsParameterMapperModule.class,
                HsqldbRdbmsSequenceModule.class,
                HsqldbServerModule.class
        },
        dependencies = {
                ModelHolder.class,
                Utils.class
        }
)
interface HsqldbDatabaseComponent extends Database { };