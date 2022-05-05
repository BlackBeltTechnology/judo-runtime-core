package hu.blackbelt.judo.services.dao.rdbms.query.mappers;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.meta.query.Variable;
import hu.blackbelt.judo.services.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.services.dao.rdbms.query.model.RdbmsField;
import hu.blackbelt.judo.services.dao.rdbms.query.model.RdbmsNamedParameter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.util.EcoreUtil;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class VariableMapper extends RdbmsMapper<Variable> {

    public static final String PARAMETER_VARIABLE_KEY = "PARAMETER";

    @NonNull
    private final RdbmsBuilder rdbmsBuilder;

    @Override
    public Stream<? extends RdbmsField> map(final Variable variable, final EMap<Node, EList<EClass>> ancestors, SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        final String id = EcoreUtil.getIdentification(variable);
        if (id != null) {
            synchronized (this) {
                if (rdbmsBuilder.getConstantFields().get().containsKey(id)) {
                    return rdbmsBuilder.getConstantFields().get().get(id).stream();
                } else {
                    final List<? extends RdbmsField> fields = getFields(variable, queryParameters).collect(Collectors.toList());
                    rdbmsBuilder.getConstantFields().get().put(id, fields);
                    return fields.stream();
                }
            }
        } else {
            return getFields(variable, queryParameters);
        }
    }

    private Stream<? extends RdbmsField> getFields(final Variable variable, final Map<String, Object> queryParameters) {
        final Object resolvedValue;
        if (PARAMETER_VARIABLE_KEY.equals(variable.getCategory())) {
            resolvedValue = queryParameters != null ? queryParameters.get(variable.getName()): null;
        } else {
            resolvedValue = rdbmsBuilder.getVariableResolver().resolve(Object.class, variable.getCategory(), variable.getName());
        }
        final Object parameterValue;
        if (AsmUtils.isEnumeration(variable.getType())) {
            EEnum eEnum = (EEnum) variable.getType();
            parameterValue = resolvedValue != null ? eEnum.getEEnumLiteral(String.valueOf(resolvedValue)).getValue() : null;
        } else {
            parameterValue = rdbmsBuilder.getCoercer().coerce(resolvedValue, variable.getType().getInstanceClassName());
        }
        return getTargets(variable).map(t -> RdbmsNamedParameter.builder()
                .parameter(rdbmsBuilder.getParameterMapper().createParameter(parameterValue, variable.getType(), null))
                .index(rdbmsBuilder.getConstantCounter().getAndIncrement())
                .target(t.getTarget())
                .targetAttribute(t.getTargetAttribute())
                .alias(t.getAlias())
                .build());
    }
}
