package hu.blackbelt.judo.runtime.core.query.feature;

import hu.blackbelt.judo.meta.expression.AttributeSelector;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.numeric.DecimalAttribute;
import hu.blackbelt.judo.meta.expression.numeric.IntegerAttribute;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;
import hu.blackbelt.judo.runtime.core.query.JoinFactory;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;

import java.util.Objects;
import java.util.Optional;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newAttributeBuilder;

public class AttributeToFeatureConverter extends ExpressionToFeatureConverter<AttributeSelector> {

    private final JoinFactory joinFactory;
    private final AsmModelAdapter modelAdapter;

    public AttributeToFeatureConverter(final FeatureFactory factory, final JoinFactory joinFactory, final AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
        this.joinFactory = joinFactory;
        this.modelAdapter = modelAdapter;
    }

    @Override
    public Feature convert(final AttributeSelector attributeSelector, final Context context, final FeatureTargetMapping targetMapping) {
        final String attributeName = attributeSelector.getAttributeName();
        final EClass sourceType = (EClass) attributeSelector.getObjectExpression().getObjectType(modelAdapter);
        final Optional<EAttribute> entityAttribute = sourceType.getEAllAttributes().stream().filter(a -> Objects.equals(a.getName(), attributeName)).findAny();

        // create additional JOINs for navigation, ie. derived attribute via multiple navigation steps
        final JoinFactory.PathEnds pathEnds = joinFactory.convertNavigationToJoins(context, context.getNode(), attributeSelector.getObjectExpression(), false);

        final boolean measured;
        if (attributeSelector instanceof IntegerAttribute) {
            measured = modelAdapter.isMeasured((IntegerAttribute) attributeSelector);
        } else if (attributeSelector instanceof DecimalAttribute) {
            measured = modelAdapter.isMeasured((DecimalAttribute) attributeSelector);
        } else {
            measured = false;
        }

        // create new attribute
        final Attribute attribute = newAttributeBuilder()
                .withNode(pathEnds.getPartner())
                .withSourceAttribute((EAttribute) sourceType.getEStructuralFeature(attributeName))
                .build();
        context.addFeature(attribute);

        if (attribute.getSourceAttribute().isDerived()) {
            throw new IllegalStateException("Derived attributes must be resolved by expression builder");
        }

        final Feature scaled;
        if (measured) {
            scaled = applyMeasure(context, attribute, entityAttribute.orElse(null), targetMapping);
        } else {
            scaled = attribute;
        }

        return scaled;
    }
}
