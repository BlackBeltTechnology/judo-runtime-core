package hu.blackbelt.judo.runtime.core;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import com.google.common.collect.ImmutableSet;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.mapper.api.Converter;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import hu.blackbelt.mapper.api.Formatter;
import lombok.*;
import org.eclipse.emf.ecore.EDataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * It is aggregate {@link Coercer} and converters for custom types.
 */
@RequiredArgsConstructor
@NoArgsConstructor
public class DataTypeManager {

    private final Map<EDataType, String> customTypes = new ConcurrentHashMap<>();

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final Function<Formatter<Object>, Collection<Converter<Object, Object>>> CONVERTER_FACTORY = f -> ImmutableSet.of(
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
    @Getter
    @Setter
    private ExtendableCoercer coercer;

    @SuppressWarnings({"rawtypes"})
    private final Map<EDataType, Collection<Converter>> converterMap = new ConcurrentHashMap<>();

    @SuppressWarnings({"rawtypes"})
    public void registerCustomType(final EDataType customDataType, final String customClassName, final Collection<Converter> converters) {
        registerCustomType(customDataType, customClassName, converters, null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public synchronized void registerCustomType(final EDataType customDataType, final String customClassName, final Collection<Converter> converters, final Formatter formatter) {
        if (!customTypes.containsKey(customDataType)) {
            customTypes.put(customDataType, customClassName);

            final Collection<Converter> _converters = converters != null ? new ArrayList<>(converters) : new ArrayList<>();
            if (formatter != null) {
                _converters.addAll(CONVERTER_FACTORY.apply(formatter));
            }
            _converters.forEach(c -> coercer.getConverterFactory().registerConverter(c));
            converterMap.put(customDataType, _converters);
        } else {
            throw new IllegalStateException("Custom type already registered");
        }
    }

    @SuppressWarnings({"unchecked"})
    public void unregisterCustomType(final EDataType customDataType) {
        if (customTypes.containsKey(customDataType)) {
            converterMap.get(customDataType).forEach(c -> coercer.getConverterFactory().unregisterConverter(c));
        }
        customTypes.remove(customDataType);
    }

    public Optional<String> getCustomTypeName(final EDataType customDataType) {
        return Optional.ofNullable(customTypes.get(customDataType));
    }

}
