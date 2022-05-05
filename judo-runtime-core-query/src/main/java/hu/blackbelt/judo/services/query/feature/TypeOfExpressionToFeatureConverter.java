package hu.blackbelt.judo.services.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.logical.TypeOfExpression;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.services.query.Context;
import hu.blackbelt.judo.services.query.FeatureFactory;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;

import java.util.Optional;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.*;

public class TypeOfExpressionToFeatureConverter extends ExpressionToFeatureConverter<TypeOfExpression> {

    public TypeOfExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final TypeOfExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        final Optional<? extends EClassifier> typeName = modelAdapter.get(expression.getElementName());
        if (!typeName.isPresent()) {
            throw new IllegalStateException("Unknown type name");
        } else if (!(typeName.get() instanceof EClass)) {
            throw new IllegalStateException("Invalid type name");
        }

        final EntityTypeName entityTypeName = newEntityTypeNameBuilder()
                .withType((EClass) typeName.get())
                .build();
        context.addFeature(entityTypeName);

        final Feature feature = newFunctionBuilder()
                .withSignature(FunctionSignature.TYPE_OF)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.INSTANCE)
                        .withParameterValue(factory.convert(expression.getObjectExpression(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.TYPE)
                        .withParameterValue(entityTypeName)
                        .build())
                .build();

        context.addFeature(feature);
        return feature;
    }
}
