package hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.util.Map;

@Builder
@Getter
public class JoinProcessParameters {
    @NonNull
    RdbmsBuilderContext builderContext;

    @NonNull
    Node join;

    boolean withoutFeatures;
    Map<String, Object> mask;
}
