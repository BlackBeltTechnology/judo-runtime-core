package hu.blackbelt.judo.services.dao.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator.SubjectCollectionCreator;

public class SubjectVariableMC implements SubjectCollectionCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectVariableMC() {
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
        return "var demo::services::TransferEntity[] te = new demo::services::TransferEntity[] " +
                "{ new demo::services::TransferEntity(), new demo::services::TransferEntity() }\n";
    }

    @Override
    public String getUse() {
        return "te";
    }

    @Override
    public String getName() {
        return "SubjectVariableMC";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferEntity[]";
    }

    @Override
    public int getCollectionSize() {
        return 2;
    }
}
