package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsNamedParameter;
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
public class VariableMapper<ID> extends RdbmsMapper<Variable> {

    public static final String PARAMETER_VARIABLE_KEY = "PARAMETER";

    @NonNull
    private final RdbmsBuilder<ID> rdbmsBuilder;

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
        boolean isParameter = PARAMETER_VARIABLE_KEY.equals(variable.getCategory());
        final Object resolvedValue;
        if (isParameter) {
            resolvedValue = queryParameters != null ? queryParameters.get(variable.getName()) : null;
        } else {
            resolvedValue = rdbmsBuilder.getVariableResolver().resolve(Object.class, variable.getCategory(), variable.getName());
        }
        final Object parameterValue;
        if (AsmUtils.isEnumeration(variable.getType())) {
            if (resolvedValue != null) {
                EEnum eEnum = (EEnum) variable.getType();
                if (isParameter) {
                    parameterValue = eEnum.getEEnumLiteral((Integer) resolvedValue).getValue();
                } else {
                    parameterValue = eEnum.getEEnumLiteral(String.valueOf(resolvedValue)).getValue();
                }
            } else {
                parameterValue = null;
            }
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
