package hu.blackbelt.judo.runtime.core.dao.rdbms;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 * 
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 * 
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.rdbmsRules.Rule;
import hu.blackbelt.judo.meta.rdbmsRules.Rules;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import lombok.Builder;
import lombok.NonNull;
import org.eclipse.emf.ecore.EReference;

import java.util.Optional;

import static hu.blackbelt.judo.runtime.core.dao.core.processors.PayloadDaoProcessor.hasOpposite;
import static hu.blackbelt.judo.runtime.core.dao.core.processors.PayloadDaoProcessor.isMandatory;

public class RdbmsReferenceUtil<ID> {

    @NonNull
    private final AsmModel asmModel;

    @NonNull
    private final TransformationTraceService transformationTraceService;

    private final Rules rules;

    private final AsmUtils asmUtils;

    @Builder
    public RdbmsReferenceUtil(@NonNull AsmModel asmModel,
                              @NonNull RdbmsModel rdbmsModel,
                              @NonNull TransformationTraceService transformationTraceService) {
        this.asmModel = asmModel;
        this.transformationTraceService = transformationTraceService;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());

        rules = rdbmsModel.getResource().getContents().stream()
                .filter(Rules.class :: isInstance)
                .map(Rules.class :: cast)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Rules not found in RDBMS model"));
    }

    public RdbmsReference<ID> buildRdbmsReferenceForStatement(
                                    RdbmsReference.RdbmsReferenceBuilder<ID> rdbmsReferenceBuilder) {

        RdbmsReference<ID> rdbmsReference = rdbmsReferenceBuilder.build();
        rdbmsReferenceBuilder.rule(rules.getRuleFromReference(rdbmsReference.getReference()));


        if (rdbmsReference.getReference().getEOpposite() != null) {
            rdbmsReferenceBuilder.oppositeReference(rdbmsReference.getOppositeReference());
            rdbmsReferenceBuilder.oppositeRule(rules.getRuleFromReference(rdbmsReference.getReference().getEOpposite()));
        } else {
            Optional<EReference> oppositeReference = asmUtils.all(EReference.class)
                    .filter(hasOpposite.and(e -> e.getEOpposite().equals(rdbmsReference.getReference()))).findFirst();

            if (oppositeReference.isPresent()) {
                rdbmsReferenceBuilder.oppositeReference(oppositeReference.get());
                rdbmsReferenceBuilder.oppositeRule(rules.getRuleFromReference(oppositeReference.get()));
            }
        }
        return rdbmsReferenceBuilder.build();
    }

    public String toString(RdbmsReference<ID> rdbmsReference) {
        // InverseForeign:   table(reference.getEReferenceType()) + column(reference)
        // ForeignKey:       table(reference.getEReferenceType()) + column(reference)

        RdbmsResolver rdbmsUtils = new RdbmsResolver(asmModel, transformationTraceService);
        return String.format("Entity: %s UUID: %s Swapped: %s" +
                        "\n     Reference: %s (%s) Type: %s UUID: %s OUUID: %s  Mandatory: %s Rule: %s Field mandatory: %s === ",
                        //+ "\n     Opposite reference: %s (%s) Type: %s UUID: %s Mandatory: %s Rule: %s Field mandatory: %s",
                AsmUtils.getClassifierFQName(rdbmsReference.getStatement().getInstance().getType()),
                rdbmsReference.getStatement().getInstance().getIdentifier(),
                rdbmsReference.isSwapped() ? "true" : "false",

                AsmUtils.getReferenceFQName(rdbmsReference.getReference()),

                rdbmsReference.getRule().isInverseForeignKey()
                        ? rdbmsUtils.rdbmsTable(rdbmsReference.getReference().getEReferenceType()).getSqlName() + "." + rdbmsUtils.rdbmsField(rdbmsReference.getReference()).getSqlName()
                        : rdbmsUtils.rdbmsTable(rdbmsReference.getStatement().getInstance().getType()).getSqlName() + "." + rdbmsUtils.rdbmsField(rdbmsReference.getReference()).getSqlName(),
                AsmUtils.getClassifierFQName(rdbmsReference.getReference().getEReferenceType()),

                rdbmsReference.getIdentifier(),
                rdbmsReference.getOppositeIdentifier(),

                isMandatory.test(rdbmsReference.getReference()) ? "true" : "false",
                ruleToString(rdbmsReference.getRule()),
                rdbmsReference.getRule().isForeignKey()
                        || rdbmsReference.getRule().isInverseForeignKey() ?
                        rdbmsUtils.rdbmsField(rdbmsReference.getReference()).isMandatory() ? "true" : "false" : "-"

                /*
                hasOpposite.test(reference) ? asmUtils.getReferenceFQName(oppositeReference) : "-",
                hasOpposite.test(reference) ? rdbmsUtils.sqlTableName(oppositeReference.getEReferenceType()) + "." + rdbmsUtils.sqlReferenceName(oppositeReference) : "-",
                hasOpposite.test(reference) ? asmUtils.getClassFQName(oppositeReference.getEReferenceType()) : "-",

                oppositeUuid,
                hasOpposite.test(reference) ? isMandatory.test(oppositeReference) ? "true" : "false" : "-",
                ruleToString(getOppositeRule()),
                hasOpposite.test(reference) && (oppositeRule.isForeignKey() || oppositeRule.isInverseForeignKey()) ? rdbmsUtils.rdbmsField(oppositeReference).isMandatory() ? "true" : "false" : "-"
                 */
        );

    }

    private String ruleToString(Rule rule) {
        if (rule != null) {
            return rule.getSymbol() + " Foreign: " + rule.isForeignKey() + " InverseForeign: " + rule.isInverseForeignKey() + " ";
        } else {
            return "";
        }

    }
}
