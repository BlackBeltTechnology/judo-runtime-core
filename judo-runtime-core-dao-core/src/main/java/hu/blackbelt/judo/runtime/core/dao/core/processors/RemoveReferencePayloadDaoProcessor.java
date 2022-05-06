package hu.blackbelt.judo.runtime.core.dao.core.processors;

import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.core.statements.InstanceExistsValidationStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.Statement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.RemoveReferenceStatement;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.resource.ResourceSet;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newHashSet;


/**
 * Creating statement for unsetting a references. It will remove and detach all the required data.
 */
@Slf4j(topic = "dao-core")
public class RemoveReferencePayloadDaoProcessor<ID> extends PayloadDaoProcessor<ID> {

    public RemoveReferencePayloadDaoProcessor(ResourceSet resourceSet, IdentifierProvider<ID> idIdentifierProvider,
                                              QueryFactory queryFactory, InstanceCollector instanceCollector) {
        super(resourceSet, idIdentifierProvider, queryFactory, instanceCollector);
    }

    public Collection<Statement<ID>> removeReference(EReference reference, Collection<ID> identifiers, ID parentIdentifier, boolean existenceCheck) {
        checkArgument(reference != null, "Type is mandatory");
        checkArgument(parentIdentifier != null, "Parent identifier is mandatory");

        Collection<Statement<ID>> statements = newHashSet();

        // If reference madatory it is not allowed
        if (reference.isRequired()) {
            // statements.add(new UnsetRelationStatement(reference.getEReferenceType(), payload, reference, parentIdentifier));
        }

        identifiers.stream().forEach(identifier -> {
            if (existenceCheck) {
                statements.add(InstanceExistsValidationStatement.<ID>buildInstanceExistsValidationStatement()
                        .type(reference.getEReferenceType())
                        .identifier(identifier)
                        .build());
            }

            statements.add(RemoveReferenceStatement.<ID>buildRemoveReferenceStatement()
                    .type(reference.getEReferenceType())
                    .reference(reference)
                    .identifier(parentIdentifier)
                    .referenceIdentifier(identifier)
                    .build());
        });


        return ImmutableSet.copyOf(statements);
    }

}
