package hu.blackbelt.judo.services.dao.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator.SubjectCollectionCreator;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;

public class SubjectUnboundRelationUC2MC implements SubjectCollectionCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectUnboundRelationUC2MC() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity");
        modelBuilder.addMappedTransferObject("TransferEntity", "Entity");
        modelBuilder.addUnmappedTransferObject("TransferObject1")
                .withRelation("TransferEntity", "te", cardinality(0, -1));
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferObject1 te1 = new demo::services::TransferObject1()\n" +
                "te1.te += new demo::services::TransferEntity()\n" +
                "te1.te += new demo::services::TransferEntity()\n" +
                "\n" +
                "var demo::services::TransferObject1 te2 = new demo::services::TransferObject1()\n" +
                "te2.te += new demo::services::TransferEntity()\n" +
                "te2.te += new demo::services::TransferEntity()\n" +
                "\n" +
                "var demo::services::TransferObject1[] tes = new demo::services::TransferObject1[] { te1, te2 }\n";
    }

    @Override
    public String getUse() {
        return "tes.te";
    }

    @Override
    public String getName() {
        return "SubjectUnboundRelationUC2MC";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferEntity[]";
    }

    @Override
    public int getCollectionSize() {
        return 4;
    }
}
