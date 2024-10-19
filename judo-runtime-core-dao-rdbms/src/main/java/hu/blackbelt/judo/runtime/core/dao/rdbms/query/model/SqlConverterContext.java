package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.Map;

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
    private Map<Node, String> prefixes;
}
