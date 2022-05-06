package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.rdbms.RdbmsField;
import hu.blackbelt.judo.meta.rdbms.RdbmsForeignKey;
import hu.blackbelt.judo.meta.rdbms.RdbmsJunctionTable;
import hu.blackbelt.judo.meta.rdbms.RdbmsTable;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getClassifierFQName;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getReferenceFQName;

@Slf4j(topic = "dao-rdbms")
@RequiredArgsConstructor
public class RdbmsResolver {

    @NonNull
    AsmModel asmModel;

    @NonNull
    TransformationTraceService transformationTraceService;

    public RdbmsTable rdbmsTable(EClass entityType) {
        List<RdbmsTable> rdbmsTableList =
                transformationTraceService.getDescendantOfInstanceByModelType(asmModel.getName(),
                        hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel.class, entityType)
                        .stream()
                        .filter(RdbmsTable.class::isInstance)
                        .map(RdbmsTable.class::cast)
                        .collect(Collectors.toList());

        checkState(rdbmsTableList.size() == 1,
                "One and only one table have to be mapped for class: " + getClassifierFQName(entityType));
        return rdbmsTableList.get(0);
    }


    public RdbmsTable rdbmsJunctionTable(EReference reference) {
        List<RdbmsTable> rdbmsTableList =
                transformationTraceService.getDescendantOfInstanceByModelType(asmModel.getName(),
                        hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel.class, reference)
                        .stream()
                        .filter(RdbmsJunctionTable.class::isInstance)
                        .map(RdbmsJunctionTable.class::cast)
                        .collect(Collectors.toList());
        if (rdbmsTableList.size() == 0 && reference.getEOpposite() != null) {
            rdbmsTableList = transformationTraceService.getDescendantOfInstanceByModelType(asmModel.getName(),
                    hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel.class, reference.getEOpposite())
                    .stream()
                    .filter(RdbmsJunctionTable.class::isInstance)
                    .map(RdbmsJunctionTable.class::cast)
                    .collect(Collectors.toList());
        }
        checkState(rdbmsTableList.size() == 1,
                "One and only one table have to be mapped for class: " + getReferenceFQName(reference));
        return rdbmsTableList.get(0);
    }


    public RdbmsField rdbmsField(EAttribute attribute) {
        List<RdbmsField> rdbmsFieldList =
                transformationTraceService.getDescendantOfInstanceByModelType(asmModel.getName(),
                        hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel.class, attribute)
                .stream()
                .filter(RdbmsField.class::isInstance)
                .map(RdbmsField.class::cast)
                .collect(Collectors.toList());


        checkState(rdbmsFieldList.size() == 1, "One and only one field have to be mapped for attributes: " + AsmUtils.getAttributeFQName(attribute));
        return rdbmsFieldList.get(0);
    }

    public RdbmsField rdbmsField(EReference reference, boolean junction) {
        List<RdbmsField> rdbmsFieldList =
                transformationTraceService.getDescendantOfInstanceByModelType(asmModel.getName(),
                        hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel.class, reference)
                .stream()
                .filter(RdbmsForeignKey.class::isInstance)
                .map(RdbmsForeignKey.class::cast)
                .filter(r -> !junction || junction && r.getName().equals(reference.getName()))
                .collect(Collectors.toList());

        checkState(rdbmsFieldList.size() == 1, "One and only one field have to be mapped for reference: " + getReferenceFQName(reference));
        return rdbmsFieldList.get(0);
    }

    public RdbmsField rdbmsField(EReference reference) {
        return rdbmsField(reference, false);
    }

    public RdbmsField rdbmsJunctionField(EReference reference) {
        return rdbmsField(reference, true);
    }

    public RdbmsField rdbmsJunctionOppositeField(EReference reference) {

        if (reference.getEOpposite() != null) {
            return rdbmsField(reference.getEOpposite(), true);
        }

        List<RdbmsField> rdbmsFieldList =
                transformationTraceService.getDescendantOfInstanceByModelType(asmModel.getName(),
                        hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel.class, reference)
                        .stream()
                        .filter(RdbmsForeignKey.class::isInstance)
                        .map(RdbmsForeignKey.class::cast)
                        .filter(r -> !r.getName().equals(reference.getName()))
                        .collect(Collectors.toList());

        checkState(rdbmsFieldList.size() == 1, "One and only one field have to be mapped for reference: " + getReferenceFQName(reference));
        return rdbmsFieldList.get(0);
    }


    public void logAttributeParameters(Map<EAttribute, Object> attributeMap) {
        if (log.isDebugEnabled()) {
            AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());
            attributeMap.entrySet().stream().forEach(e -> {
                log.debug("    Attr: " +
                        asmUtils.getAttributeFQName(e.getKey())
                        + " (" + rdbmsField(e.getKey()).getSqlName() + ")"
                        + " = " + e.getValue() + "(" + (e.getValue() != null ? e.getValue().getClass().getName() : "N/A") + ")"
                        + " ===== EAttribute: " + e.getKey());
            });
        }
    }


    public <ID> void logReferenceParameters(Map<EReference, ID> referenceMap) {
        if (log.isDebugEnabled()) {
            referenceMap.entrySet().stream().forEach(e -> {
                log.debug("    Ref: " +
                        getReferenceFQName(e.getKey())
                        + " (" + rdbmsField(e.getKey()).getSqlName() + ")"
                        + " = " + e.getValue()
                        + " ===== EReference: " + e.getKey());
            });
        }
    }
}
