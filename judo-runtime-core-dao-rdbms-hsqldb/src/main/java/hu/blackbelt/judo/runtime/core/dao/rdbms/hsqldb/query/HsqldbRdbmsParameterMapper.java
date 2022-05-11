package hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.query;

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.DefaultRdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.NonNull;

import java.sql.Time;
import java.sql.Timestamp;

public class HsqldbRdbmsParameterMapper extends DefaultRdbmsParameterMapper implements RdbmsParameterMapper {
    @Builder
    public HsqldbRdbmsParameterMapper(@NonNull Coercer coercer,
                                      @NonNull RdbmsModel rdbmsModel,
                                      @NonNull IdentifierProvider identifierProvider) {
        super(coercer, rdbmsModel, identifierProvider);

        getSqlTypes().put(Timestamp.class, vd -> "TIMESTAMP WITH TIME ZONE");
        getSqlTypes().put(Time.class, vd -> "TIMESTAMP WITH TIME ZONE");
        getSqlTypes().put(String.class, vd -> "LONGVARCHAR");
    }
}
