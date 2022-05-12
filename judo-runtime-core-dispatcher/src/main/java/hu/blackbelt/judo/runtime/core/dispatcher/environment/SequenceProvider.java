package hu.blackbelt.judo.runtime.core.dispatcher.environment;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.function.Function;

@NoArgsConstructor
@RequiredArgsConstructor
public class SequenceProvider<T> implements Function<String, T> {

    @NonNull
    @Setter
    Sequence<T> sequence;

    @Override
    public T apply(final String sequenceName) {
        return sequence.getNextValue(sequenceName);
    }
}
