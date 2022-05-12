package hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql;

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.DefaultRdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.NonNull;

import java.sql.Time;
import java.sql.Timestamp;

public class PostgresqlRdbmsParameterMapper<ID> extends DefaultRdbmsParameterMapper<ID> implements RdbmsParameterMapper<ID> {
    @Builder
    private PostgresqlRdbmsParameterMapper(@NonNull Coercer coercer,
                                          @NonNull RdbmsModel rdbmsModel,
                                          @NonNull IdentifierProvider<ID> identifierProvider) {
        super(coercer, rdbmsModel, identifierProvider);

        getSqlTypes().put(Timestamp.class, vd -> "TIMESTAMPTZ");
        getSqlTypes().put(Time.class, vd -> "TIMETZ");
        getSqlTypes().put(String.class, vd -> "TEXT");
        getSqlTypes().put(Double.class, vd -> "DOUBLE PRECISION");

    }
}
