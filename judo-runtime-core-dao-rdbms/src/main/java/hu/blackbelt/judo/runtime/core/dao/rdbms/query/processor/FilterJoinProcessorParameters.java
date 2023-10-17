package hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor;

import hu.blackbelt.judo.meta.query.Filter;
import hu.blackbelt.judo.meta.query.Join;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsJoin;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.eclipse.emf.common.util.EList;

import java.util.List;

@Builder
@Getter
@ToString
public class FilterJoinProcessorParameters {
    @NonNull
    private List<RdbmsJoin> joins;
    @NonNull
    private SubSelect query;
    @NonNull
    private EList<Join> processedNodesForJoins;
    @NonNull
    private List<RdbmsField> conditions;
    @Builder.Default
    private String partnerTablePrefix = "";
    @NonNull
    private Filter filter;
    @NonNull
    private Node partnerTable;
    private boolean addJoinsOfFilterFeature;
}
