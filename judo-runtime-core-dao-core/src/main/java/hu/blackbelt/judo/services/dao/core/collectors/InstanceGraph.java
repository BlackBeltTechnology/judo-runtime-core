package hu.blackbelt.judo.services.dao.core.collectors;

import lombok.NonNull;

import java.util.ArrayList;
import java.util.Collection;

@lombok.Getter
@lombok.Builder
public class InstanceGraph<ID> {

    @NonNull
    private ID id;

    private final Collection<InstanceReference<ID>> containments = new ArrayList<>();

    private final Collection<InstanceReference<ID>> references = new ArrayList<>();

    private final Collection<InstanceReference<ID>> backReferences = new ArrayList<>();

    @Override
    public String toString() {
        return "[id=" + id +
                (containments.isEmpty() ? "" : ", containments=" + containments) +
                (references.isEmpty() ? "" : ", references=" + references) +
                (backReferences.isEmpty() ? "" : ", backReferences=" + backReferences) +
                "]";
    }
}
