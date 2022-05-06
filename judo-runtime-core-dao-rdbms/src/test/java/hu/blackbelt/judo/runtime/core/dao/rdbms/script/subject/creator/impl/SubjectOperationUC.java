package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator.SubjectCollectionCreator;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;

public class SubjectOperationUC implements SubjectCollectionCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectOperationUC() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject");
        modelBuilder.addUnboundOperation("action")
                .withBody("\n" +
                        "var demo::services::TransferObject[] es = new demo::services::TransferObject[] " +
                        "{ new demo::services::TransferObject(), new demo::services::TransferObject() }\n" +
                        "\n" +
                        "return es\n"
                )
                .withOutput("TransferObject", cardinality(0, -1));
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
        return "SubjectOperationUC";
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
