package hu.blackbelt.judo.services.dispatcher.environment;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicyOption;

import java.util.function.Function;

@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE, property = {
        "judo.environment-variable-provider=true",
        "category=SEQUENCE",
        "cacheable=false"
})
@NoArgsConstructor
@RequiredArgsConstructor
@Slf4j
public class SequenceProvider<T> implements Function<String, T> {

    @Reference(policyOption = ReferencePolicyOption.GREEDY)
    @NonNull
    Sequence<T> sequence;

    @Override
    public T apply(final String sequenceName) {
        return sequence.getNextValue(sequenceName);
    }
}
