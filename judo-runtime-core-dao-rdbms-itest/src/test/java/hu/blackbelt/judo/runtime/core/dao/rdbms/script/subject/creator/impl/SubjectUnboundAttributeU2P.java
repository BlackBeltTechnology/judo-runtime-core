package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator;

public class SubjectUnboundAttributeU2P implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectUnboundAttributeU2P() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject")
                .withAttribute("Double", "DoubleAttribute");
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferObject uto = new demo::services::TransferObject(DoubleAttribute = 3.14)\n";
    }

    @Override
    public String getUse() {
        return "uto.DoubleAttribute";
    }

    @Override
    public String getName() {
        return "SubjectUnboundAttributeU2P";
    }

    @Override
    public String getReturnType() {
        return "demo::types::Double";
    }
}
