package hu.blackbelt.judo.services.dao.rdbms.query.utils;

import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.services.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.services.dao.rdbms.query.mappers.RdbmsMapper;
import hu.blackbelt.judo.services.query.QueryFactory;

import java.util.stream.Stream;

public class RdbmsAliasUtil {

    private static final String INSTANCE_IDS = "_ids";
    private static final String PARENT_IDS = "_parents";

    public static final String AGGREGATE_PREFIX = "aggr_";
    private static final String JOIN_PREFIX = "j_";

    private static final String FILTER_PREFIX = "filter_";

    /**
     * Get target column alias. Targets are indexed because they can be added multiple times in different roles.
     *
     * @param target target object
     * @param alias  alias
     * @return column (indexed) alias
     */
    public static String getTargetColumnAlias(final Target target, final String alias) {
        return alias + "_" + target.getIndex();
    }

    /**
     * Get column alias of a given source.
     *
     * @param node source (SELECT or JOIN)
     * @return alias of source
     */
    public static String getIdColumnAlias(final Node node) {
        return node != null ? node.getAlias() + "_" + StatementExecutor.ID_COLUMN_NAME : null;
    }

    public static String getTypeColumnAlias(final Node node) {
        return node != null ? node.getAlias() + "_" + StatementExecutor.ENTITY_TYPE_COLUMN_NAME : null;
    }

    public static String getVersionColumnAlias(final Node node) {
        return node != null ? node.getAlias() + "_" + StatementExecutor.ENTITY_VERSION_COLUMN_NAME : null;
    }

    public static String getCreateUsernameColumnAlias(final Node node) {
        return node != null ? node.getAlias() + "_" + StatementExecutor.ENTITY_CREATE_USERNAME_COLUMN_NAME : null;
    }

    public static String getCreateUserIdColumnAlias(final Node node) {
        return node != null ? node.getAlias() + "_" + StatementExecutor.ENTITY_CREATE_USER_ID_COLUMN_NAME : null;
    }

    public static String getCreateTimestampColumnAlias(final Node node) {
        return node != null ? node.getAlias() + "_" + StatementExecutor.ENTITY_CREATE_TIMESTAMP_COLUMN_NAME : null;
    }

    public static String getUpdateUsernameColumnAlias(final Node node) {
        return node != null ? node.getAlias() + "_" + StatementExecutor.ENTITY_UPDATE_USERNAME_COLUMN_NAME : null;
    }

    public static String getUpdateUserIdColumnAlias(final Node node) {
        return node != null ? node.getAlias() + "_" + StatementExecutor.ENTITY_UPDATE_USER_ID_COLUMN_NAME : null;
    }

    public static String getUpdateTimestampColumnAlias(final Node node) {
        return node != null ? node.getAlias() + "_" + StatementExecutor.ENTITY_UPDATE_TIMESTAMP_COLUMN_NAME : null;
    }

    public static String getParentIdColumnAlias(final Node node) {
        return node != null ? "__parent_" + getIdColumnAlias(node) : null;
    }

    public static String getOptionalParentIdColumnAlias(final Node node) {
        if (node == null) {
            return null;
        } else if (node instanceof SubSelectJoin && ((SubSelectJoin) node).getSubSelect().getPartner() == null) {
            return ((SubSelectJoin) node).getSubSelect().getSelect().getFeatures().stream()
                    .filter(f -> !f.getAggregations().isEmpty())
                    .map(f -> RdbmsMapper.getTargets(f).map(tt -> tt.getAlias() + (tt.getTarget() != null ? "_" + tt.getTarget().getIndex() : "")).findAny().get())
                    .findAny()
                    .orElse(null);
        } else {
            return "__parent_" + getIdColumnAlias(node);
        }
    }

    public static String getInstanceIdsKey(final Select select) {
        return select.getAlias() + INSTANCE_IDS;
    }

    public static String getParentIdsKey(final Select select) {
        return select.getAlias() + PARENT_IDS;
    }

    public static String getNavigationSubSelectAlias(final SubSelect query) {
        return JOIN_PREFIX + query.getAlias();
    }

    public static String getFilterPrefix(final String prefix) {
        return prefix + FILTER_PREFIX;
    }
}
