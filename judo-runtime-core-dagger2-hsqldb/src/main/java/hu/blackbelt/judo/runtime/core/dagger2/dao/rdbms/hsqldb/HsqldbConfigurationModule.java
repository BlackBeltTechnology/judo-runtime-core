package hu.blackbelt.judo.runtime.core.dagger2.dao.rdbms.hsqldb;

import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;

import javax.annotation.Nullable;
import javax.inject.Named;

import java.io.File;

import static hu.blackbelt.judo.runtime.core.dagger2.dao.rdbms.hsqldb.HsqldbServerModule.*;
import static hu.blackbelt.judo.runtime.core.dagger2.database.Database.*;

@Module
public class HsqldbConfigurationModule {

    @JudoApplicationScope
    @Provides
    @Named(RDBMS_SEQUENCE_START)
    @Nullable
    Long providesRdbmsSequenceStart() {
        return 1L;
    };

    @JudoApplicationScope
    @Provides
    @Named(RDBMS_SEQUENCE_INCREMENT)
    @Nullable
    Long providesRdbmsSequenceIncrement() {
        return 1L;
    }

    @JudoApplicationScope
    @Provides
    @Named(RDBMS_SEQUENCE_CREATE_IF_NOT_EXISTS)
    @Nullable
    Boolean providesRdbmsSequenceCreateIfNotExists() {
        return true;
    }

    @JudoApplicationScope
    @Provides
    @Named(HSQLDB_SERVER_DATABASE_NAME)
    @Nullable
    String providesHsqldbServerDatabaseName() {
        return null;
    }

    @JudoApplicationScope
    @Provides
    @Named(HSQLDB_SERVER_DATABASE_PATH)
    @Nullable
    File providesHsqldbServerDatabasePath() {
        return null;
    }

    @JudoApplicationScope
    @Provides
    @Named(HSQLDB_SERVER_PORT)
    @Nullable
    Integer providesHsqldbServerPort() {
        return null;
    }
}
