package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

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

import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import lombok.Builder;
import lombok.Getter;
import org.eclipse.emf.ecore.EAttribute;

import java.util.Collections;
import java.util.stream.Stream;

public abstract class RdbmsMapper<T extends ParameterType> {

    private static final int MAX_ALIAS_LENGTH_WITHOUT_INDEX = 25;

    public abstract Stream<? extends RdbmsField> map(T parameter, RdbmsBuilderContext builderContext);

    public static Stream<RdbmsTarget> getTargets(final Feature feature) {
        if (feature.getTargetMappings().isEmpty()) {
            return Collections.singleton(RdbmsTarget.builder()
                    .alias(getAttributeOrFeatureName(null, feature))
                    .build()).stream();
        } else {
            return feature.getTargetMappings().stream()
                    .map(tm -> RdbmsTarget.builder()
                            .target(tm.getTarget())
                            .targetAttribute(tm.getTargetAttribute())
                            .alias(getAttributeOrFeatureName(tm.getTargetAttribute(), feature))
                            .build());
        }
    }

    public static String getAttributeOrFeatureName(final EAttribute attribute, final Feature feature) {
        if (attribute != null) {
            return attribute.getName().length() > MAX_ALIAS_LENGTH_WITHOUT_INDEX ? attribute.getName().substring(0, MAX_ALIAS_LENGTH_WITHOUT_INDEX - 10) + attribute.hashCode() : attribute.getName();
        } else if (feature != null) {
            return "__f" + ((Node) feature.eContainer()).getFeatures().indexOf(feature);
        } else {
            throw new IllegalArgumentException("Attribute or feature is mandatory");
        }
    }

    @Getter
    @Builder
    public static class RdbmsTarget {
        private final Target target;
        private final String alias;
        private final EAttribute targetAttribute;
    }
}
