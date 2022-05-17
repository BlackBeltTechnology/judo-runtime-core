package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.postgresql;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbRdbmsSequence;
import hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.sequence.PostgresqlRdbmsSequence;

import javax.annotation.Nullable;
import javax.sql.DataSource;

@SuppressWarnings("rawtypes")
public class PostgresqlRdbmsSequenceProvider implements Provider<Sequence> {

    public static final String RDBMS_SEQUENCE_START = "rdbmsSequenceStart";
    public static final String RDBMS_SEQUENCE_INCREMENT = "rdbmsSequenceIncrement";
    public static final String RDBMS_SEQUENCE_CREATE_IF_NOT_EXISTS = "rdbmsSequenceCreateIfNotExists";

    @Inject
    private DataSource dataSource;

    @Inject(optional = true)
    @Named(RDBMS_SEQUENCE_START)
    @Nullable
    Long start = 1L;

    @Inject(optional = true)
    @Named(RDBMS_SEQUENCE_INCREMENT)
    @Nullable
    Long increment = 1L;

    @Inject(optional = true)
    @Named(RDBMS_SEQUENCE_CREATE_IF_NOT_EXISTS)
    @Nullable
    Boolean createIfNotExists = true;

    @Override
    public Sequence get() {
        PostgresqlRdbmsSequence.PostgresqlRdbmsSequenceBuilder builder = PostgresqlRdbmsSequence.builder();
        builder
                .dataSource(dataSource)
                .start(start)
                .increment(increment)
                .createIfNotExists(createIfNotExists);

        return builder.build();
    }
}
