package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join;

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
