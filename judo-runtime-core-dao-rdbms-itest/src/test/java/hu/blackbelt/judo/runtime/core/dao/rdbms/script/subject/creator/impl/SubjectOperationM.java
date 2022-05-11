package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;

public class SubjectOperationM implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectOperationM() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity");
        modelBuilder.addMappedTransferObject("TransferEntity", "Entity");
        modelBuilder.addUnboundOperation("action")
                .withBody("return new demo::services::TransferEntity()\n")
                .withOutput("TransferEntity", cardinality(0, 1));
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
        return "demo::services::UnboundServices.action()";
    }

    @Override
    public String getName() {
        return "SubjectOperationM";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferEntity";
    }
}
