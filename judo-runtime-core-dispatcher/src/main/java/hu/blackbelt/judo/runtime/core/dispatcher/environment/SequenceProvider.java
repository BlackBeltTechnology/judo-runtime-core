package hu.blackbelt.judo.runtime.core.dispatcher.environment;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;

@NoArgsConstructor
@RequiredArgsConstructor
@Slf4j
public class SequenceProvider<T> implements Function<String, T> {

    @NonNull
    Sequence<T> sequence;

    @Override
    public T apply(final String sequenceName) {
        return sequence.getNextValue(sequenceName);
    }
}
