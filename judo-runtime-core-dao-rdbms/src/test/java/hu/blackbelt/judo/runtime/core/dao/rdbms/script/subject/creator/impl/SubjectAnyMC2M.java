package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator;

public class SubjectAnyMC2M implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectAnyMC2M() {
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
        return "var demo::services::TransferEntity[] mtos = new demo::services::TransferEntity[] {\n" +
                "\tnew demo::services::TransferEntity(),\n" +
                "\tnew demo::services::TransferEntity()\n" +
                "}\n";
    }

    @Override
    public String getUse() {
        return "mtos!any()";
    }

    @Override
    public String getName() {
        return "SubjectAnyMC2M";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferEntity";
    }
}
