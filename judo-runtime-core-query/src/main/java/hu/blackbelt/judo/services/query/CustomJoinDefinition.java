package hu.blackbelt.judo.services.query;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class CustomJoinDefinition {

    private final String sourceIdParameterName;

    private final String sourceIdSetParameterName;

    private final String navigationSql;
}
