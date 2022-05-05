package hu.blackbelt.judo.services.dao.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator.SubjectCollectionCreator;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;

public class SubjectUnboundRelationU2UC implements SubjectCollectionCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectUnboundRelationU2UC() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject");

        modelBuilder.addUnmappedTransferObject("TransferObject1")
                .withRelation("TransferObject", "te", cardinality(0, -1));
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferObject1 mto = new demo::services::TransferObject1()\n" +
                "mto.te += new demo::services::TransferObject()\n" +
                "mto.te += new demo::services::TransferObject()\n";
    }

    @Override
    public String getUse() {
        return "mto.te";
    }

    @Override
    public String getName() {
        return "SubjectUnboundRelationU2UC";
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
