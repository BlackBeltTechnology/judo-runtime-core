package hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor;

import hu.blackbelt.judo.meta.query.Filter;
import hu.blackbelt.judo.meta.query.Join;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsJoin;
import lombok.Builder;
import lombok.NonNull;
import org.eclipse.emf.common.util.EList;

import java.util.List;

@Builder
public class FilterJoinProcessorParameters {
    @NonNull
    List<RdbmsJoin> joins;
    @NonNull
    SubSelect query;
    @NonNull
    EList<Join> processedNodesForJoins;
    @NonNull
    List<RdbmsField> conditions;
    @Builder.Default
    String partnerTablePrefix = "";
    @NonNull
    Filter filter;
    @NonNull
    Node partnerTable;
    boolean addJoinsOfFilterFeature;
}
