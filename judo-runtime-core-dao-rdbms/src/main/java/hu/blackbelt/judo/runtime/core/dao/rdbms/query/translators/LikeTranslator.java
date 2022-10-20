package hu.blackbelt.judo.runtime.core.dao.rdbms.query.translators;

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

import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.StringExpression;
import hu.blackbelt.judo.meta.expression.constant.StringConstant;
import hu.blackbelt.judo.meta.expression.logical.Like;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

import static hu.blackbelt.judo.meta.expression.logical.util.builder.LogicalBuilders.newLikeBuilder;

@Builder
public class LikeTranslator implements Function<Like, Like> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public Like apply(final Like likeExpression) {
        return newLikeBuilder()
                .withExpression((StringExpression) translator.apply(likeExpression.getExpression()))
                .withPattern((StringConstant) translator.apply(likeExpression.getPattern()))
                .withCaseInsensitive(likeExpression.isCaseInsensitive())
                .build();
    }

}
