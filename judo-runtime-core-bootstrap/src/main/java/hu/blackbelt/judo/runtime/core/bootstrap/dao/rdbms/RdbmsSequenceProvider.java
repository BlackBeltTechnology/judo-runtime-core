package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.sequence.RdbmsSequence;

import javax.sql.DataSource;

public class RdbmsSequenceProvider implements Provider<Sequence> {

    public static final String RDBMS_SEQUENCE_START = "rdbmsSequenceStart";
    public static final String RDBMS_SEQUENCE_INCREMENT = "rdbmsSequenceIncrement";
    public static final String RDBMS_SEQUENCE_CREATE_IF_NOT_EXISTS = "rdbmsSequenceCreateIfNotExists";

    @Inject
    private DataSource dataSource;

    @Inject
    private Dialect dialect;

    @Inject(optional = true)
    @Named(RDBMS_SEQUENCE_START)
    Long start = 1L;

    @Inject(optional = true)
    @Named(RDBMS_SEQUENCE_INCREMENT)
    Long increment = 1L;

    @Inject(optional = true)
    @Named(RDBMS_SEQUENCE_CREATE_IF_NOT_EXISTS)
    Boolean createIfNotExists = true;


    @Override
    public Sequence get() {
        RdbmsSequence.RdbmsSequenceBuilder builder = RdbmsSequence.builder();
        builder
                .dataSource(dataSource)
                .dialect(dialect)
                .start(start)
                .increment(increment)
                .createIfNotExists(createIfNotExists);

        return builder.build();
    }
}
