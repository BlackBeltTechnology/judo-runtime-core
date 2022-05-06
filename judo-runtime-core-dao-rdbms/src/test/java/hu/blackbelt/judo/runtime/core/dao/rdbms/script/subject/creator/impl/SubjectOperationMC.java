package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator.SubjectCollectionCreator;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;

public class SubjectOperationMC implements SubjectCollectionCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectOperationMC() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity");
        modelBuilder.addMappedTransferObject("TransferEntity", "Entity");
        modelBuilder.addUnboundOperation("action")
                .withBody("\n" +
                        "var demo::services::TransferEntity[] es = new demo::services::TransferEntity[] " +
                        "{ new demo::services::TransferEntity(), new demo::services::TransferEntity() }\n" +
                        "\n" +
                        "return es\n"
                )
                .withOutput("TransferEntity", cardinality(0, -1));
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
        return "SubjectOperationMC";
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
