package hu.blackbelt.judo.runtime.core.query;

import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.meta.query.support.QueryModelResourceSupport;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

@Getter
@Builder
@Slf4j
public class Context implements Cloneable {

    @Setter
    private Node node;

    private final QueryModelResourceSupport queryModelResourceSupport;

    @NonNull
    private final Map<String, Node> variables;

    @NonNull
    private final AtomicInteger sourceCounter;

    @NonNull
    private final AtomicInteger targetCounter;

    public Context clone() {
        return Context.builder()
                .node(node)
                .queryModelResourceSupport(queryModelResourceSupport)
                .variables(new TreeMap<>(variables))
                .sourceCounter(sourceCounter)
                .targetCounter(targetCounter)
                .build();
    }

    public Context clone(final String variableName, final Node node) {
        final Map<String, Node> newVariables = new TreeMap<>(variables);
        if (variableName != null) {
            newVariables.put(variableName, node);
        }

        return Context.builder()
                .node(node)
                .queryModelResourceSupport(queryModelResourceSupport)
                .variables(newVariables)
                .sourceCounter(sourceCounter)
                .targetCounter(targetCounter)
                .build();
    }

    public void addFeature(Feature feature) {
        checkArgument(node != null, "No Node found in context");
        if (node instanceof Select) {
            node.getFeatures().add(feature);
        } else if (node instanceof Filter) {
            node.getFeatures().add(feature);
        } else if (node instanceof OrderBy) {
            ((Node) node.eContainer()).getFeatures().add(feature);
        } else if (node instanceof Join) {
            ((Join) node).getBase().getFeatures().add(feature);
        } else {
            throw new IllegalStateException("Unsupported node");
        }
    }

    @Override
    public String toString() {
        return variables.entrySet().stream().map(e -> e.getKey() + ": " + (e.getValue().getType() != null ? e.getValue().getType().getName() : "-") + " [" + e.getValue().getAlias() + "]").collect(Collectors.joining(","));
    }
}
