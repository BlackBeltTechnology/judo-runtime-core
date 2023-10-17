package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.eclipse.emf.common.util.EMap;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

@Builder(toBuilder = true)
@Getter
@ToString
public class SqlConverterContext {
    @Builder.Default
    private String prefix = "";
    @Builder.Default
    private boolean includeAlias = false;
    private Coercer coercer;
    private MapSqlParameterSource sqlParameters;
    private EMap<Node, String> prefixes;
}
