package hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor;

import hu.blackbelt.judo.meta.query.Join;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import lombok.Builder;
import lombok.NonNull;
import org.eclipse.emf.ecore.EReference;

@SuppressWarnings("unchecked")
@Builder
public class SimpleJoinProcessorParameters {
    @Builder.Default
    String postfix = "";
    @NonNull
    Join join;

    EReference reference;
    EReference opposite;

    @NonNull
    RdbmsBuilderContext builderContext;
}
