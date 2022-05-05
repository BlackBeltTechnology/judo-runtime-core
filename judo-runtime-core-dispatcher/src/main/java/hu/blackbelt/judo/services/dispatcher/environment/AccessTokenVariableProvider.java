package hu.blackbelt.judo.services.dispatcher.environment;

import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import lombok.extern.slf4j.Slf4j;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicyOption;

import java.security.Principal;
import java.util.function.Function;

@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE, property = {
        "judo.environment-variable-provider=true",
        "category=USER",
        "cacheable=false"
})
@Slf4j
public class AccessTokenVariableProvider implements Function<String, Object> {

    private static final String DEFAULT_PRINCIPAL_NAME = "name";

    @Reference(policyOption = ReferencePolicyOption.GREEDY)
    Context context;

    @Override
    public Object apply(final String key) {
        final Principal principal = context.getAs(Principal.class, Dispatcher.PRINCIPAL_KEY);
        if (principal instanceof JudoPrincipal) {
            return ((JudoPrincipal) principal).getAttributes().get(key);
        } else if (principal instanceof Principal && DEFAULT_PRINCIPAL_NAME.equalsIgnoreCase(key)) {
            return principal.getName();
        } else if (principal instanceof Principal) {
            log.warn("Java principal supports 'name' key only");
            return null;
        } else {
            return null;
        }
    }
}
