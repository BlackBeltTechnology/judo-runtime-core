package hu.blackbelt.judo.runtime.core.dao.core.collectors;

import lombok.NonNull;
import lombok.ToString;
import org.eclipse.emf.ecore.EReference;

@lombok.Getter
@lombok.Builder
@ToString
public class InstanceReference<ID> {

    @NonNull
    private EReference reference;

    @NonNull
    private final InstanceGraph<ID> referencedElement;

    @Override
    public String toString() {
        return reference.getName() + ":" + referencedElement;
    }
}
