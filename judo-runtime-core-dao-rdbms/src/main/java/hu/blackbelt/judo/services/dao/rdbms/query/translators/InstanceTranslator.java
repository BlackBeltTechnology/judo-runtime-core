package hu.blackbelt.judo.services.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.constant.Instance;
import lombok.Builder;
import lombok.NonNull;
import org.eclipse.emf.ecore.EClass;

import java.util.function.Function;

import static hu.blackbelt.judo.meta.expression.constant.util.builder.ConstantBuilders.newInstanceBuilder;
import static hu.blackbelt.judo.meta.expression.util.builder.ExpressionBuilders.newTypeNameBuilder;

@Builder
public class InstanceTranslator implements Function<Instance, Instance> {

    @NonNull
    private final AsmUtils asmUtils;

    @Override
    public Instance apply(final Instance instance) {
        final String name = instance.getElementName().getName();
        final String namespace = instance.getElementName().getNamespace();

        final EClass transferObjectType = asmUtils.getClassByFQName(namespace.replace("::", ".") + "." + name)
                .orElseThrow(() -> new IllegalStateException("Transfer object type not found: " + instance));
        final EClass entityType = asmUtils.getMappedEntityType(transferObjectType)
                .orElseThrow(() -> new IllegalStateException("Entity type not found: " + instance));

        final String entityName = entityType.getName();
        final String entityNamespace = AsmUtils.getPackageFQName(entityType.getEPackage());

        return newInstanceBuilder()
                .withName("self")
                .withElementName(newTypeNameBuilder()
                        .withName(entityName)
                        .withNamespace(entityNamespace)
                        .build())
                .build();
    }
}
