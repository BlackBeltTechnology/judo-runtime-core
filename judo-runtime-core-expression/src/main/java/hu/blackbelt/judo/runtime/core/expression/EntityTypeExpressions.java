package hu.blackbelt.judo.runtime.core.expression;

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

import hu.blackbelt.judo.meta.expression.DataExpression;
import hu.blackbelt.judo.meta.expression.ReferenceExpression;
import lombok.NonNull;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@lombok.Getter
@lombok.Builder
public class EntityTypeExpressions {

    @NonNull
    private final EClass entityType;

    private final Map<EAttribute, DataExpression> getterAttributeExpressions = new ConcurrentHashMap<>();

    private final Map<EReference, ReferenceExpression> getterReferenceExpressions = new ConcurrentHashMap<>();
}
