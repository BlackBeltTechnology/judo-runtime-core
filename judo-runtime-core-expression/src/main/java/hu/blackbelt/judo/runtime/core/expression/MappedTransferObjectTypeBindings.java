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

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.DataExpression;
import hu.blackbelt.judo.meta.expression.LogicalExpression;
import hu.blackbelt.judo.meta.expression.ReferenceExpression;
import lombok.NonNull;
import lombok.Setter;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Expression tree node of a given (mapped) transfer object type. It is the base of logical queries.
 */
@lombok.Getter
@lombok.Builder
public class MappedTransferObjectTypeBindings {

    /**
     * Mapped entity type of {@link #entityType}.
     */
    @NonNull
    private final EClass entityType;

    /**
     * Transfer object type that the node belongs to.
     */
    @NonNull
    private final EClass transferObjectType;

    /**
     * Filter expression of mapped transfer object type.
     */
    @Setter
    private LogicalExpression filter;

    /**
     * Getter attributes of transfer attributes.
     */
    private final Map<EAttribute, DataExpression> getterAttributeExpressions = new ConcurrentHashMap<>();

    /**
     * Setter expressions of transfer attributes.
     */
    private final Map<EAttribute, DataExpression> setterAttributeExpressions = new ConcurrentHashMap<>();

    /**
     * Getter expressions of the transfer object relations.
     */
    private final Map<EReference, ReferenceExpression> getterReferenceExpressions = new ConcurrentHashMap<>();

    /**
     * Setter expressions of the transfer object relations.
     */
    private final Map<EReference, ReferenceExpression> setterReferenceExpressions = new ConcurrentHashMap<>();

    /**
     * References (both single and multiple) of the transfer object type.
     */
    private final Map<EReference, MappedTransferObjectTypeBindings> references = new ConcurrentHashMap<>();

    @Override
    public String toString() {
        return "FROM: " + AsmUtils.getClassifierFQName(entityType) + "\n"
                + "TO: " + AsmUtils.getClassifierFQName(transferObjectType)
                + (getterAttributeExpressions.isEmpty() ? "" : getterAttributeExpressions.entrySet().stream().map(e -> "\n  - getter attribute " + e.getKey().getName() + ": " + e.getValue()).collect(Collectors.joining()))
                + (setterAttributeExpressions.isEmpty() ? "" : setterAttributeExpressions.entrySet().stream().map(e -> "\n  - setter attribute " + e.getKey().getName() + ": " + e.getValue()).collect(Collectors.joining()))
                + (getterReferenceExpressions.isEmpty() ? "" : getterReferenceExpressions.entrySet().stream().map(e -> "\n  - getter relation " + e.getKey().getName() + ": " + e.getValue()).collect(Collectors.joining()))
                + (setterReferenceExpressions.isEmpty() ? "" : setterReferenceExpressions.entrySet().stream().map(e -> "\n  - setter relation " + e.getKey().getName() + ": " + e.getValue()).collect(Collectors.joining()));
    }
}
