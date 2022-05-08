package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModuleConfig;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.sequence.RdbmsSequence;

import javax.sql.DataSource;

public class RdbmsSequenceProvider implements Provider<Sequence> {
    private final DataSource dataSource;

    private final Dialect dialect;

    private final RdbmsSequenceConfig config;

    @Inject
    public RdbmsSequenceProvider(JudoModuleConfig config,
                                 DataSource dataSource,
                                 Dialect dialect) {
        this.dataSource = dataSource;
        this.config = config.getRdbmsSequenceConfig();
        this.dialect = dialect;
    }

    @Override
    public Sequence get() {
        RdbmsSequence.RdbmsSequenceBuilder builder = RdbmsSequence.builder();
        builder
                .dataSource(dataSource)
                .dialect(dialect);

        if (config != null) {
            builder
                    .start(config.getStart().longValue())
                    .increment(config.getIncrement().longValue())
                    .createIfNotExists(config.getCreateIfNotExists());
        }
        return builder.build();
    }
}
