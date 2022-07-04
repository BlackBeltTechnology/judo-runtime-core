package hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.query.mappers;

import hu.blackbelt.judo.meta.query.FunctionSignature;
import hu.blackbelt.judo.meta.query.ParameterName;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.FunctionMapper;
import lombok.Builder;
import lombok.NonNull;

import java.util.List;

public class HsqldbFunctionMapper<ID> extends FunctionMapper<ID> {

    @Builder
    public HsqldbFunctionMapper(@NonNull RdbmsBuilder<ID> rdbmsBuilder) {
        super(rdbmsBuilder);

        getFunctionBuilderMap().put(FunctionSignature.ILIKE, c ->
                c.builder.pattern("(LOWER({0}) LIKE LOWER({1}))")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN))));

    }
}

