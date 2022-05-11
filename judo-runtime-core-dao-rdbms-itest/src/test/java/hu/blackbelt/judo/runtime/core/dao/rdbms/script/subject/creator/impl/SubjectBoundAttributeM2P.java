package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator;

public class SubjectBoundAttributeM2P implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectBoundAttributeM2P() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity")
                .withAttribute("Double", "DoubleAttribute");
        modelBuilder.addMappedTransferObject("TransferEntity", "Entity")
                .withAttribute("Double", "DoubleAttribute");
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferEntity mto = new demo::services::TransferEntity(DoubleAttribute = 3.14)";
    }

    @Override
    public String getUse() {
        return "mto.DoubleAttribute";
    }

    @Override
    public String getName() {
        return "SubjectBoundAttributeM2P";
    }

    @Override
    public String getReturnType() {
        return "demo::types::Double";
    }
}
