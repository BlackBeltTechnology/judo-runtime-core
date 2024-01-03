package hu.blackbelt.judo.runtime.core.dagger2.database;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;

import javax.sql.DataSource;

public interface Database {
    String RDBMS_SEQUENCE_START = "rdbmsSequenceStart";
    String RDBMS_SEQUENCE_INCREMENT = "rdbmsSequenceIncrement";
    String RDBMS_SEQUENCE_CREATE_IF_NOT_EXISTS = "rdbmsSequenceCreateIfNotExists";

    DataSource getDataSource();
    MapperFactory getMapperFactory();
    RdbmsInit getRdbmsInit();

    RdbmsParameterMapper getRdbmsParameterMapper();

    Sequence getSequence();

    Dialect getDialect();

}
