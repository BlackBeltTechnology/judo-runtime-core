package hu.blackbelt.judo.runtime.core.dispatcher.environment;

import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import lombok.*;

import java.util.Map;
import java.util.function.Function;

@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ActorVariableProvider implements Function<String, Object> {

    @NonNull
    @Setter
    Context context;

    @Override
    public Object apply(final String key) {
        final Map<String, Object> actor = context.getAs(Map.class, Dispatcher.ACTOR_KEY);
        return actor != null ? actor.get(key) : null;
    }
}
