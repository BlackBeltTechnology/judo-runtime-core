package hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor;

import hu.blackbelt.judo.meta.query.Join;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.ecore.EReference;

@SuppressWarnings("unchecked")
@Builder
@Getter
public class SimpleJoinProcessorParameters {
    @Builder.Default
    private String postfix = "";
    @NonNull
    private Join join;

    private EReference reference;
    private EReference opposite;

    @NonNull
    private RdbmsBuilderContext builderContext;
}
