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

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dao.core.processors.PayloadDaoProcessor;
import lombok.Builder;
import lombok.Getter;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.util.List;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toList;

@Getter
@Builder
class PayloadTraverser<E> {
    EClass transferObjectType;
    Payload payload;
    EReference container;
    AsmUtils asmUtils;

    public static void traversePayload(@SuppressWarnings("rawtypes") Consumer<PayloadTraverser> processor, 
    		@SuppressWarnings("rawtypes") PayloadTraverser context) {
        List<EReference> references;

        processor.accept(context);
        references = context.getTransferObjectType().getEAllReferences().stream()
                .filter(
                        PayloadDaoProcessor.notParent(context.getContainer())
                                .and(r -> context.getAsmUtils().getMappedReference(r).isPresent()))
                .collect(toList());
        references.stream()
                .filter(r -> context.getPayload().containsKey(r.getName()) && context.getPayload().get(r.getName()) != null)
                .collect(PayloadDaoProcessor.toReferencePayloadMapOfPayloadCollection(context.getPayload()))
                .entrySet().stream()
                .forEach(e -> {
                    e.getValue().stream().forEach(p -> {
                        traversePayload(processor,
                                PayloadTraverser.builder()
                                        .transferObjectType(e.getKey().getEReferenceType())
                                        .payload(p)
                                        .container(e.getKey())
                                        .asmUtils(context.getAsmUtils())
                                        .build());

                    });
                });
    }

}
