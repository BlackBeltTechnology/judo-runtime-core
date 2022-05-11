package hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.sequence;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.util.Collections;

import static java.util.Objects.requireNonNullElse;

@Builder
public class PostgresqlRdbmsSequence implements Sequence<Long> {

    private static final Long DEFAULT_START = Sequence.DEFAULT_START;
    private static final long DEFAULT_INCREMENT = Sequence.DEFAULT_INCREMENT;

    private final DataSource dataSource;
    private final Long start;
    private final Long increment;
    private final Boolean createIfNotExists;


    @Builder
    public PostgresqlRdbmsSequence(@NonNull DataSource dataSource,
                                   Long start, Long increment,
                                   Boolean createIfNotExists) {
        this.dataSource = dataSource;
        this.start = requireNonNullElse(start, DEFAULT_START);
        this.increment = requireNonNullElse(increment, DEFAULT_INCREMENT);
        this.createIfNotExists = requireNonNullElse(createIfNotExists, true);
    }

    @AllArgsConstructor
    private enum Operation {
        CURRENT_VALUE("CURRVAL"), NEXT_VALUE("NEXTVAL");
        String functionName;
    }

    private Long execute(String sequenceName, final Operation operation) {
        sequenceName = sequenceName.replaceAll("[^a-zA-Z0-9_]", "_");
        final NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        if (createIfNotExists) {
            jdbcTemplate.execute("CREATE SEQUENCE IF NOT EXISTS " + sequenceName +
                            (start != null ? " START WITH " + start : "") +
                            (increment != null ? " INCREMENT BY " + increment : ""),
                    Collections.emptyMap(),
                    (stmt) -> stmt.execute());
        }
        return jdbcTemplate.queryForObject("SELECT " + (operation.functionName) + "('" + sequenceName + "')", Collections.emptyMap(), Long.class);
    }

    @Override
    public Long getNextValue(final String sequenceName) {
        return execute(sequenceName, Operation.NEXT_VALUE);
    }

    @Override
    public Long getCurrentValue(String sequenceName) {
        return execute(sequenceName, Operation.CURRENT_VALUE);
    }
}
