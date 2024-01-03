package hu.blackbelt.judo.runtime.core.dagger2.database;

import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;

import javax.inject.Inject;
import javax.sql.DataSource;

@Module
public class DataSourceModule {

    @Inject
    Database database;


    @JudoApplicationScope
    @Provides
    DataSource provideDataSource() {
        return database.getDataSource();
    };

}
