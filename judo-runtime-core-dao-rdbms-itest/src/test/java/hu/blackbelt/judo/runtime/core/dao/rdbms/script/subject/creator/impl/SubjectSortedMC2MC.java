package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator.SubjectCollectionCreator;

public class SubjectSortedMC2MC implements SubjectCollectionCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectSortedMC2MC() {
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
        return "var demo::services::TransferEntity[] tes = new demo::services::TransferEntity[] {\n" +
                "\tnew demo::services::TransferEntity(),\n" +
                "\tnew demo::services::TransferEntity()\n" +
                "}\n";
    }

    @Override
    public String getUse() {
        return "tes!sort()";
    }

    @Override
    public String getName() {
        return "SubjectSortedMC2MC";
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
