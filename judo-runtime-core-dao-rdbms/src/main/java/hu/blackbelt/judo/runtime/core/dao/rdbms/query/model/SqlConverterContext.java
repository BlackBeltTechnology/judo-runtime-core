package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import org.eclipse.emf.common.util.EMap;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

@Builder(toBuilder = true)
public class SqlConverterContext {
    @Builder.Default
    public String prefix = "";
    @Builder.Default
    boolean includeAlias = false;
    public Coercer coercer;
    public MapSqlParameterSource sqlParameters;
    public EMap<Node, String> prefixes;
}
