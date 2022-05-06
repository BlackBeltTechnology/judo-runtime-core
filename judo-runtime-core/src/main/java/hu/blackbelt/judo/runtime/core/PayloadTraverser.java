package hu.blackbelt.judo.runtime.core;

import com.google.common.collect.ImmutableList;
import hu.blackbelt.judo.dao.api.Payload;
import lombok.*;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Getter
@Builder
public class PayloadTraverser {

    @NonNull
    private BiConsumer<Payload, PayloadTraverserContext> processor;

    @NonNull
    private Predicate<EReference> predicate;

    private static final Predicate<EStructuralFeature> IS_COLLECTION = (r) -> r.isMany();

    public Payload traverse(final Payload payload, final EClass transferObjectType) {
        return traverse(payload, PayloadTraverserContext
                .builder()
                .type(transferObjectType)
                .path(Collections.emptyList())
                .build());
    }

    private Payload traverse(final Payload payload, final PayloadTraverserContext ctx) {
        if (payload == null) {
            return null;
        }

        processor.accept(payload, ctx);

        ctx.getType().getEAllReferences().stream()
                .filter(predicate)
                .filter(r -> payload.containsKey(r.getName()) && payload.get(r.getName()) != null)
                .collect(toReferencePayloadMapOfPayloadCollection(payload))
                .entrySet().stream()
                .forEach(e -> {
                    int idx = 0;
                    for (Iterator<Payload> it = e.getValue().iterator(); it.hasNext(); idx++) {
                        final Payload p = it.next();
                        traverse(p, PayloadTraverserContext.builder()
                                .type(e.getKey().getEReferenceType())
                                .path(ImmutableList.<PathEntry>builder()
                                        .addAll(ctx.getPath())
                                        .add(PathEntry.builder()
                                                .reference(e.getKey())
                                                .index(idx)
                                                .build())
                                        .build())
                                .build());
                    }
                });

        return payload;
    }

    private static Collector<EReference, ?, Map<EReference, Collection<Payload>>> toReferencePayloadMapOfPayloadCollection(Payload payload) {
        return Collectors.toMap(Function.identity(), (r) -> {
            if (IS_COLLECTION.test(r)) {
                return payload.getAsCollectionPayload(r.getName());
            } else {
                return ImmutableList.of(payload.getAsPayload(r.getName()));
            }
        });
    }

    @Getter
    @Builder
    public static class PayloadTraverserContext {

        @NonNull
        private EClass type;

        @NonNull
        private List<PathEntry> path;

        public String getPathAsString() {
            return path.stream().map(e -> e.getReference().getName() + (e.getReference().isMany() ? "[" + e.getIndex() + "]" : "")).collect(Collectors.joining("."));
        }
    }

    @Getter
    @Builder
    public static class PathEntry {

        @NonNull
        private final EReference reference;

        private final Integer index;
    }
}
