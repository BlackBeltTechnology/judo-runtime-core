package hu.blackbelt.judo.services.dao.rdbms;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.services.dao.core.processors.PayloadDaoProcessor;
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

    public static void traversePayload(Consumer<PayloadTraverser> processor, PayloadTraverser context) {
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
