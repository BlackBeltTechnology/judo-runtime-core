package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator;

public class SubjectNewM implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectNewM() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity");
        modelBuilder.addMappedTransferObject("TransferEntity", "Entity");
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "";
    }

    @Override
    public String getUse() {
        return "new demo::services::TransferEntity()";
    }

    @Override
    public String getName() {
        return "SubjectNewM";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferEntity";
    }
}
