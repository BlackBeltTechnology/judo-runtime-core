package hu.blackbelt.judo.runtime.core.dao.core.processors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.core.statements.AddReferenceStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.InstanceExistsValidationStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.Statement;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.resource.ResourceSet;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;


/**
 * Creating statement for updating a single reference.
 */
@Slf4j(topic = "dao-core")
public class AddReferencePayloadDaoProcessor<ID> extends PayloadDaoProcessor<ID> {

    public AddReferencePayloadDaoProcessor(
            ResourceSet resourceSet,
            IdentifierProvider<ID> idIdentifierProvider, QueryFactory queryFactory, InstanceCollector instanceCollector) {

        super(resourceSet, idIdentifierProvider, queryFactory, instanceCollector);
    }

    public Collection<Statement<ID>> addReference(
            EReference reference,
            Collection<ID> identifiers,
            ID parentIdentifier,
            boolean existenceCheck) {

        checkArgument(reference != null, "Type is mandatory");
        checkArgument(identifiers != null, "Identifiers is mandatory");

        Collection<Statement<ID>> statements = Sets.newHashSet();

        /*
        TODO: Add relation will unset existing relations
        statements.add(UnsetRelationStatement.<ID>buildUnsetRelationStatement()
                .type(reference.getEReferenceType())
                .reference(reference)
                .identifier(parentIdentifier)
                .build());
        */

        // collect already referencing instances of bidirectional relation
        Map<ID, Collection<ID>> alreadyReferencingInstances;
        if (reference.getEOpposite() != null) {
            alreadyReferencingInstances = getInstanceCollector().collectGraph(reference.getEReferenceType(), identifiers).entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().getBackReferences().stream()
                            .filter(br -> AsmUtils.equals(br.getReference(), reference) && !Objects.equals(parentIdentifier, br.getReferencedElement().getId()))
                            .map(br -> br.getReferencedElement().getId())
                            .collect(Collectors.toList())));
        } else {
            alreadyReferencingInstances = Collections.emptyMap();
        }

        identifiers.stream().forEach(identifier -> {
            if (existenceCheck) {
                statements.add(InstanceExistsValidationStatement.<ID>buildInstanceExistsValidationStatement()
                        .type(reference.getEReferenceType())
                        .identifier(identifier)
                        .build());
            }

            statements.add(AddReferenceStatement.<ID>buildAddReferenceStatement()
                    .type(reference.getEReferenceType())
                    .reference(reference)
                    .referenceIdentifier(identifier)
                    .alreadyReferencingInstances(alreadyReferencingInstances.get(identifier))
                    .identifier(parentIdentifier)
                    .build());
        });

        // TODO: Creating implicit detaches, when the opposite can refer only one and that reference is nullable.

        return ImmutableSet.copyOf(statements);
    }

}
