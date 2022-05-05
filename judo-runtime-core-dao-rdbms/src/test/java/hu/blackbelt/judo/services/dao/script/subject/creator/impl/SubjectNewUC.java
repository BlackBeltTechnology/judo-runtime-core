package hu.blackbelt.judo.services.dao.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator.SubjectCollectionCreator;

public class SubjectNewUC implements SubjectCollectionCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectNewUC() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject");
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
        return "new demo::services::TransferObject[] " +
                "{ new demo::services::TransferObject(), new demo::services::TransferObject() }\n";
    }

    @Override
    public String getName() {
        return "SubjectNewUC";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferObject[]";
    }

    @Override
    public int getCollectionSize() {
        return 2;
    }
}
