package hu.blackbelt.judo.services.dao.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator;

public class SubjectVariableM implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectVariableM() {
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
        return "var demo::services::TransferEntity mto = new demo::services::TransferEntity()\n";
    }

    @Override
    public String getUse() {
        return "mto";
    }

    @Override
    public String getName() {
        return "SubjectVariableM";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferEntity";
    }
}
