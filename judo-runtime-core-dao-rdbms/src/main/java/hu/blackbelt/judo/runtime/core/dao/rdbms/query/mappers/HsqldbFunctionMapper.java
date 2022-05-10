package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsFunction;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class HsqldbFunctionMapper extends FunctionMapper {

    private final Map<FunctionSignature, java.util.function.Function<FunctionContext, RdbmsFunction>> functionMap = new LinkedHashMap<>();

    @Builder
    public HsqldbFunctionMapper(@NonNull RdbmsBuilder rdbmsBuilder) {
        super(rdbmsBuilder);
    }
}
