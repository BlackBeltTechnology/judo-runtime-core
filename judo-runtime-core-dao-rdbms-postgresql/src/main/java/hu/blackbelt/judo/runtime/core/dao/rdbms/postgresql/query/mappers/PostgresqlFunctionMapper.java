package hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.query.mappers;

import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.FunctionMapper;
import lombok.Builder;
import lombok.NonNull;

import java.util.List;

public class PostgresqlFunctionMapper<ID> extends FunctionMapper<ID> {

    @SuppressWarnings("unchecked")
	@Builder
    public PostgresqlFunctionMapper(@NonNull RdbmsBuilder<ID> rdbmsBuilder) {
        super(rdbmsBuilder);

        getFunctionBuilderMap().put(FunctionSignature.MODULO_INTEGER, c ->
                c.builder.pattern("({0} % {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        getFunctionBuilderMap().put(FunctionSignature.INTEGER_TO_STRING, c ->
                c.builder.pattern("CAST({0} AS TEXT)")
                        .parameters(List.of(c.parameters.get(ParameterName.PRIMITIVE))));
        getFunctionBuilderMap().put(FunctionSignature.DECIMAL_TO_STRING, getFunctionBuilderMap().get(FunctionSignature.INTEGER_TO_STRING));
        getFunctionBuilderMap().put(FunctionSignature.DATE_TO_STRING, getFunctionBuilderMap().get(FunctionSignature.INTEGER_TO_STRING));
        getFunctionBuilderMap().put(FunctionSignature.TIME_TO_STRING, getFunctionBuilderMap().get(FunctionSignature.INTEGER_TO_STRING));
        getFunctionBuilderMap().put(FunctionSignature.LOGICAL_TO_STRING, getFunctionBuilderMap().get(FunctionSignature.INTEGER_TO_STRING));
        getFunctionBuilderMap().put(FunctionSignature.ENUM_TO_STRING, getFunctionBuilderMap().get(FunctionSignature.INTEGER_TO_STRING));
        getFunctionBuilderMap().put(FunctionSignature.CUSTOM_TO_STRING, getFunctionBuilderMap().get(FunctionSignature.INTEGER_TO_STRING));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_TO_STRING, c ->
                c.builder.pattern("REPLACE(CAST({0} AS TEXT), '' '', ''T'')")
                        .parameters(List.of(c.parameters.get(ParameterName.PRIMITIVE))));

        getFunctionBuilderMap().put(FunctionSignature.MATCHES_STRING, c ->
                c.builder.pattern("({0} ~ {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN))));

        getFunctionBuilderMap().put(FunctionSignature.ADD_DATE, c ->
                c.builder.pattern("({0} + CAST({1} || '' days'' AS INTERVAL))")
                        .parameters(List.of(c.parameters.get(ParameterName.DATE), c.parameters.get(ParameterName.ADDITION))));

        getFunctionBuilderMap().put(FunctionSignature.DIFFERENCE_DATE, c ->
                c.builder.pattern("(CAST({0} AS DATE) - CAST({1} AS DATE))")
                        .parameters(List.of(c.parameters.get(ParameterName.END), c.parameters.get(ParameterName.START))));

        getFunctionBuilderMap().put(FunctionSignature.ADD_TIMESTAMP, c ->
                c.builder.pattern("({0} + CAST({1} / 1000 || '' seconds'' AS INTERVAL))")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP), c.parameters.get(ParameterName.ADDITION))));

        getFunctionBuilderMap().put(FunctionSignature.ADD_TIME, c ->
                c.builder.pattern("({0} + CAST({1} / 1000 || '' seconds'' AS INTERVAL))")
                        .parameters(List.of(c.parameters.get(ParameterName.TIME), c.parameters.get(ParameterName.ADDITION))));

        getFunctionBuilderMap().put(FunctionSignature.DIFFERENCE_TIMESTAMP, c ->
                c.builder.pattern("(EXTRACT(EPOCH FROM {0} - {1}) * 1000)")
                        .parameters(List.of(c.parameters.get(ParameterName.END), c.parameters.get(ParameterName.START))));

        getFunctionBuilderMap().put(FunctionSignature.DIFFERENCE_TIME, c ->
                c.builder.pattern("(EXTRACT(EPOCH FROM {0} - {1}) * 1000)")
                        .parameters(List.of(c.parameters.get(ParameterName.END), c.parameters.get(ParameterName.START))));

        getFunctionBuilderMap().put(FunctionSignature.MILLISECONDS_OF_TIMESTAMP, c ->
                c.builder.pattern("(CAST(EXTRACT(SECOND from CAST({0} AS TIMESTAMP)) * 1000 AS INTEGER) % 1000)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        getFunctionBuilderMap().put(FunctionSignature.MILLISECONDS_OF_TIME, c ->
                c.builder.pattern("(CAST(EXTRACT(SECOND from CAST({0} AS TIME)) * 1000 AS INTEGER) % 1000)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIME))));

    }
}
