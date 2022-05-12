package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.mapper.api.Coercer;
import lombok.experimental.SuperBuilder;
import org.eclipse.emf.common.util.EMap;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.text.MessageFormat;
import java.util.regex.Pattern;

/**
 * RDBMS column definition.
 */
@SuperBuilder
public class RdbmsColumn extends RdbmsField {

    private String pattern;
    private String partnerTablePrefix;
    private Node partnerTable;
    private String partnerTablePostfix;
    private String columnName;

    private boolean skipLastPrefix;

    @SuppressWarnings("unused")
	private DomainConstraints sourceDomainConstraints;

    private static final String DEFAULT_PATTERN = "{0}.{1}";

    @Override
    public String toSql(final String prefix, final boolean includeAlias, final Coercer coercer, final MapSqlParameterSource sqlParameters, final EMap<Node, String> prefixes) {
        final String partnerTableName;
        if (partnerTable != null) {
            final String partnerTableNameWithPrefix;
            if (prefixes.containsKey(partnerTable)) {
                partnerTableNameWithPrefix = prefixes.get(partnerTable) + (partnerTablePrefix != null ? partnerTablePrefix : "") + partnerTable.getAlias() + (partnerTablePostfix != null ? partnerTablePostfix : "");
            } else {
                partnerTableNameWithPrefix = prefix + (partnerTablePrefix != null ? partnerTablePrefix : "") + partnerTable.getAlias() + (partnerTablePostfix != null ? partnerTablePostfix : "");
            }
            if (skipLastPrefix) {
                partnerTableName = partnerTableNameWithPrefix.replaceFirst("^" + Pattern.quote(prefix), "");
            } else {
                partnerTableName = partnerTableNameWithPrefix;
            }
        } else {
            partnerTableName = null;
        }

        final String sql = cast(MessageFormat.format(pattern != null ? pattern : DEFAULT_PATTERN, new Object[]{partnerTableName, columnName}), null, targetAttribute);
        return getWithAlias(sql, includeAlias);
    }
}