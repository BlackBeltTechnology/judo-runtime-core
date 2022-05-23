package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.Target;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EAnnotation;
import org.eclipse.emf.ecore.EAttribute;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.Optional;

@SuperBuilder
@NoArgsConstructor
public abstract class RdbmsField {

    protected String alias;

    protected Target target;

    @Getter
    protected EAttribute targetAttribute;

    /**
     * Convert field definition to SQL format.
     *
     * @return SQL string
     */
    abstract String toSql(String prefix, boolean includeAlias, Coercer coercer, MapSqlParameterSource sqlParameters, EMap<Node, String> prefixes);

    protected String getWithAlias(final String sql, final boolean includeAlias) {
        return sql + (includeAlias && alias != null ? " AS " + getRdbmsAlias() : "");
    }

    public String getRdbmsAlias() {
        return (target != null ? RdbmsAliasUtil.getTargetColumnAlias(target, alias) : alias);
    }

    public static DomainConstraints getDomainConstraints(final EAttribute attribute) {
        final Optional<EAnnotation> constraints = AsmUtils.getExtensionAnnotationByName(attribute, "constraints", false);

        if (constraints.isPresent()) {
            final EMap<String, String> details = constraints.get().getDetails();
            return DomainConstraints.builder()
                    .precision(details.containsKey("precision") ? Integer.parseInt(details.get("precision")) : null)
                    .scale(details.containsKey("scale") ? Integer.parseInt(details.get("scale")) : null)
                    .maxLength(details.containsKey("maxLength") ? Integer.parseInt(details.get("maxLength")) : null)
                    .build();
        } else {
            return null;
        }
    }

    private String convertDomainConstraintsToCast(final DomainConstraints domainConstraints) {
        final String typeCasting;
        if (domainConstraints == null) {
            typeCasting = "";
        } else if (domainConstraints.getPrecision() != null) {
            typeCasting = "DECIMAL(" + domainConstraints.getPrecision() + (domainConstraints.getScale() != null ? "," + domainConstraints.getScale() : "") + ")";
        } else if (domainConstraints.getMaxLength() != null) {
            typeCasting = "VARCHAR(" + domainConstraints.getMaxLength() + ")";
        } else {
            typeCasting = "";
        }
        return typeCasting;
    }

    protected String cast(final String sql, final String typeName, final EAttribute targetAttribute) {
        final String typeCasting;
        if (targetAttribute != null) {
            typeCasting = convertDomainConstraintsToCast(getDomainConstraints(targetAttribute));
        } else {
            typeCasting = "";
        }
        if (typeName != null && typeName.length() > 0) {
            return "CAST(" + sql + " AS " + typeName + ")";
        } else if (typeCasting != null && typeCasting.length() > 0) {
            return "CAST(" + sql + " AS " + typeCasting + ")";
        } else {
            return sql;
        }
    }

    @Getter
    @Builder
    public static class DomainConstraints {
        private final Integer precision;
        private final Integer scale;
        private final Integer maxLength;
    }
}
