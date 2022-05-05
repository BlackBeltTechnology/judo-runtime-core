package hu.blackbelt.judo.services.dao.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator.SubjectCollectionCreator;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;

public class SubjectUnboundRelationMC2MC implements SubjectCollectionCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectUnboundRelationMC2MC() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity2");
        modelBuilder.addEntity("Entity");
        modelBuilder.addMappedTransferObject("TransferEntity1", "Entity2");
        modelBuilder.addMappedTransferObject("TransferEntity", "Entity")
                .withRelation("TransferEntity1", "te", cardinality(0, -1));
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferEntity te1 = new demo::services::TransferEntity()\n" +
                "te1.te += new demo::services::TransferEntity1()\n" +
                "te1.te += new demo::services::TransferEntity1()\n" +
                "\n" +
                "var demo::services::TransferEntity te2 = new demo::services::TransferEntity()\n" +
                "te2.te += new demo::services::TransferEntity1()\n" +
                "te2.te += new demo::services::TransferEntity1()\n" +
                "\n" +
                "var demo::services::TransferEntity[] tes = new demo::services::TransferEntity[] { te1, te2 }\n";
    }

    @Override
    public String getUse() {
        return "tes.te";
    }

    @Override
    public String getName() {
        return "SubjectUnboundRelationMC2MC";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferEntity1[]";
    }

    @Override
    public int getCollectionSize() {
        return 4;
    }
}
