package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator;

public class SubjectUnboundAttributeM2P implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectUnboundAttributeM2P() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity");
        modelBuilder.addMappedTransferObject("TransferEntity", "Entity")
                .withAttribute("Double", "DoubleAttribute");
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferEntity mto = new demo::services::TransferEntity(DoubleAttribute = 3.14)\n";
    }

    @Override
    public String getUse() {
        return "mto.DoubleAttribute";
    }

    @Override
    public String getName() {
        return "SubjectUnboundAttributeM2P";
    }

    @Override
    public String getReturnType() {
        return "demo::types::Double";
    }
}
