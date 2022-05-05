package hu.blackbelt.judo.services.dispatcher.environment;

import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicyOption;

import java.util.Map;
import java.util.function.Function;

@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE, property = {
        "judo.environment-variable-provider=true",
        "category=ACTOR",
        "cacheable=false"
})
public class ActorVariableProvider implements Function<String, Object> {

    @Reference(policyOption = ReferencePolicyOption.GREEDY)
    Context context;

    @Override
    public Object apply(final String key) {
        final Map<String, Object> actor = context.getAs(Map.class, Dispatcher.ACTOR_KEY);
        return actor != null ? actor.get(key) : null;
    }
}
