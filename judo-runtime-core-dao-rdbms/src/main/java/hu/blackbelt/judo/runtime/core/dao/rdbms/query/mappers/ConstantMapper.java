package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

import hu.blackbelt.judo.meta.query.Constant;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsConstant;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.util.EcoreUtil;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class ConstantMapper extends RdbmsMapper<Constant> {

    @NonNull
    private final RdbmsBuilder rdbmsBuilder;

    @Override
    public Stream<? extends RdbmsField> map(final Constant constant, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        final String id = EcoreUtil.getIdentification(constant);
        if (id != null) {
            synchronized (this) {
                if (rdbmsBuilder.getConstantFields().get().containsKey(id)) {
                    return rdbmsBuilder.getConstantFields().get().get(id).stream();
                } else {
                    final List<? extends RdbmsField> fields = getFields(constant).collect(Collectors.toList());
                    rdbmsBuilder.getConstantFields().get().put(id, fields);
                    return fields.stream();
                }
            }
        } else {
            return getFields(constant);
        }
    }

    private Stream<? extends RdbmsField> getFields(final Constant constant) {
        return getTargets(constant).map(t -> RdbmsConstant.builder()
                .parameter(rdbmsBuilder.getParameterMapper().createParameter(constant.getValue(), null))
                .index(rdbmsBuilder.getConstantCounter().getAndIncrement())
                .target(t.getTarget())
                .targetAttribute(t.getTargetAttribute())
                .alias(t.getAlias())
                .build());
    }
}
