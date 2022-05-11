package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator.SubjectCollectionCreator;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;

public class SubjectUnboundRelationMC2UC implements SubjectCollectionCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectUnboundRelationMC2UC() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity");
        modelBuilder.addUnmappedTransferObject("TransferObject1");
        modelBuilder.addMappedTransferObject("TransferEntityType", "Entity")
                .withRelation("TransferObject1", "u", cardinality(0, -1));
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferEntityType mto1 = new demo::services::TransferEntityType()\n" +
                "mto1.u += new demo::services::TransferObject1()\n" +
                "mto1.u += new demo::services::TransferObject1()\n" +
                "\n" +
                "var demo::services::TransferEntityType mto2 = new demo::services::TransferEntityType()\n" +
                "mto2.u += new demo::services::TransferObject1()\n" +
                "mto2.u += new demo::services::TransferObject1()\n" +
                "\n" +
                "var demo::services::TransferEntityType[] mtos = new demo::services::TransferEntityType[] { mto1, mto2 }\n";
    }

    @Override
    public String getUse() {
        return "mtos.u";
    }

    @Override
    public String getName() {
        return "SubjectUnboundRelationMC2UC";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferObject1[]";
    }

    @Override
    public int getCollectionSize() {
        return 4;
    }
}
