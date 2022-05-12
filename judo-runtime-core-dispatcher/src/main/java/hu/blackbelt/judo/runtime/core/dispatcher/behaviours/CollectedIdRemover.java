package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultDispatcher;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.Map;

@RequiredArgsConstructor
public class CollectedIdRemover<ID> {

    @NonNull
    private final String key;

    public void removeIdentifiers(final Payload payload, final Collection<ID> ids) {
        final Object id = payload.get(key);
        if (id != null && ids.contains(id)) {
            payload.remove("__$created");
            payload.remove(key);
            payload.remove(DefaultDispatcher.UPDATEABLE_KEY);
            payload.remove(DefaultDispatcher.DELETEABLE_KEY);
        }

        payload.values().forEach(v -> processRemoval(v, ids));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	private void processRemoval(final Object o, final Collection<ID> ids) {
        if (o instanceof Payload) {
            removeIdentifiers((Payload) o, ids);
        } else if (o instanceof Map) {
            removeIdentifiers(Payload.asPayload((Map<String, Object>) o), ids);
        } else if (o instanceof Collection) {
            ((Collection) o).forEach(i -> processRemoval(i, ids));
        }
    }
}
