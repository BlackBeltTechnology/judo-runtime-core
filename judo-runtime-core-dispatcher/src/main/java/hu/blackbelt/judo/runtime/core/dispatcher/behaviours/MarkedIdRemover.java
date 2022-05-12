package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultDispatcher;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.Map;

@RequiredArgsConstructor
public class MarkedIdRemover<ID> {

    @NonNull
    private final String key;

    public void process(final Payload payload) {
        processAndCollect(payload, null, false);
    }

    public void unmark(final Payload payload) {
        processAndCollect(payload, null, true);
    }

    public void processAndCollect(final Payload payload, Collection<ID> collected) {
        processAndCollect(payload, collected, false);
    }

    private void processAndCollect(final Payload payload, Collection<ID> collected, boolean markerOnly) {
        if (payload.containsKey("__$created")) {
            payload.remove("__$created");

            if (!markerOnly) {
                @SuppressWarnings("unchecked")
				ID removed = (ID) payload.remove(key);
                payload.remove(DefaultDispatcher.UPDATEABLE_KEY);
                payload.remove(DefaultDispatcher.DELETEABLE_KEY);
                if (collected != null) {
                    collected.add(removed);
                }
            }
        }

        payload.values().forEach(v -> processRemoval(v, collected));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
	private void processRemoval(final Object o, Collection<ID> collected) {
        if (o instanceof Payload) {
            processAndCollect((Payload) o, collected);
        } else if (o instanceof Map) {
            processAndCollect(Payload.asPayload((Map<String, Object>) o), collected);
        } else if (o instanceof Collection) {
            ((Collection) o).forEach(i -> processRemoval(i, collected));
        }
    }
}
