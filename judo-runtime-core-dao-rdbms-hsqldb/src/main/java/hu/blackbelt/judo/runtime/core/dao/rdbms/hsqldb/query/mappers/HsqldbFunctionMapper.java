package hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.query.mappers;

import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.FunctionMapper;
import lombok.Builder;
import lombok.NonNull;

public class HsqldbFunctionMapper<ID> extends FunctionMapper<ID> {

    @Builder
    public HsqldbFunctionMapper(@NonNull RdbmsBuilder<ID> rdbmsBuilder) {
        super(rdbmsBuilder);
    }
}

