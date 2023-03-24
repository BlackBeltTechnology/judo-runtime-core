package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join;

/*-
 * #%L
 * JUDO Runtime Core :: RDBMS Data Access Objects
 * %%
 * Copyright (C) 2018 - 2023 BlackBelt Technology
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

import java.util.*;

public class RdbmsJoinComparator implements Comparator<RdbmsJoin> {

    final List<RdbmsJoin> originalOrder;

    public RdbmsJoinComparator(Collection<RdbmsJoin> originalOrder) {
        this.originalOrder = new ArrayList<>(originalOrder);
    }

    @Override
    public int compare(RdbmsJoin left, RdbmsJoin right) {
        checkArguments(left, right);

        String leftAliasToCompare = left.alias;
        if (left.aliasToCompareWith != null && !left.aliasToCompareWith.isBlank()) {
            leftAliasToCompare = left.aliasToCompareWith;
        }

        String rightAliasToCompare = left.alias;
        if (right.aliasToCompareWith != null && !right.aliasToCompareWith.isBlank()) {
            rightAliasToCompare = right.aliasToCompareWith;
        }

        if (left.equals(right)) {
            return 0;
        }

        // right needs left
        if (right.joinConditionTableAliases.contains(leftAliasToCompare)) {
            return -1;
        }

        // left needs right
        if (left.joinConditionTableAliases.contains(rightAliasToCompare)) {
            return 1;
        }

        // there is no explicit dependency but original order must be kept
        return originalOrder.indexOf(left) - originalOrder.indexOf(right);
    }

    private static void checkArguments(RdbmsJoin left, RdbmsJoin right) {
        if (left == null || right == null) {
            throw new IllegalArgumentException("Compared arguments cannot be null");
        }
        if (left.alias == null || left.alias.isBlank() || right.alias == null || right.alias.isBlank()) {
            throw new IllegalArgumentException("Compared arguments' aliases cannot be null/empty");
        }
    }

}
