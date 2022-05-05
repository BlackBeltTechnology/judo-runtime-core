package hu.blackbelt.judo.services.core;

import com.google.common.collect.ImmutableSet;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.mapper.api.Converter;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import hu.blackbelt.mapper.api.Formatter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EDataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * It is aggregate {@link Coercer} and converters for custom types.
 */
@RequiredArgsConstructor
public class DataTypeManager {

    private final EMap<EDataType, String> customTypes = ECollections.asEMap(new ConcurrentHashMap<>());

    private static Function<Formatter, Collection<Converter>> CONVERTER_FACTORY = f -> ImmutableSet.of(
            new Converter() {
                @Override
                public Class getSourceType() {
                    return f.getType();
                }

                @Override
                public Class getTargetType() {
                    return String.class;
                }

                @Override
                public Object apply(Object o) {
                    return f.convertValueToString(o);
                }
            },
            new Converter() {
                @Override
                public Class getSourceType() {
                    return String.class;
                }

                @Override
                public Class getTargetType() {
                    return f.getType();
                }

                @Override
                public Object apply(Object o) {
                    return f.parseString((String) o);
                }
            }
    );

    @NonNull
    private final ExtendableCoercer coercer;

    private final EMap<EDataType, Collection<Converter>> converterMap = ECollections.asEMap(new ConcurrentHashMap<>());

    public void registerCustomType(final EDataType customDataType, final String customClassName, final Collection<Converter> converters) {
        registerCustomType(customDataType, customClassName, converters, null);
    }

    public synchronized void registerCustomType(final EDataType customDataType, final String customClassName, final Collection<Converter> converters, final Formatter formatter) {
        if (!customTypes.containsKey(customDataType)) {
            customTypes.put(customDataType, customClassName);

            final Collection<Converter> _converters = converters != null ? new ArrayList<>(converters) : new ArrayList<>();
            if (formatter != null) {
                _converters.addAll(CONVERTER_FACTORY.apply(formatter));
            }

            if (_converters != null) {
                _converters.forEach(c -> coercer.getConverterFactory().registerConverter(c));
                converterMap.put(customDataType, _converters);
            } else {
                converterMap.put(customDataType, Collections.emptyList());
            }
        } else {
            throw new IllegalStateException("Custom type already registered");
        }
    }

    public void unregisterCustomType(final EDataType customDataType) {
        if (customTypes.containsKey(customDataType)) {
            converterMap.get(customDataType).forEach(c -> coercer.getConverterFactory().unregisterConverter(c));
        }
        customTypes.removeKey(customDataType);
    }

    public Optional<String> getCustomTypeName(final EDataType customDataType) {
        return Optional.ofNullable(customTypes.get(customDataType));
    }

    public Coercer getCoercer() {
        return coercer;
    }
}
