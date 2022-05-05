package hu.blackbelt.judo.services.dao.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator.SubjectCollectionCreator;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;

public class SubjectUnboundRelationUC2UC implements SubjectCollectionCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectUnboundRelationUC2UC() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject1");
        modelBuilder.addUnmappedTransferObject("TransferObject")
                .withRelation("TransferObject1", "u", cardinality(0, -1));
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferObject uto1 = new demo::services::TransferObject()\n" +
                "uto1.u += new demo::services::TransferObject1()\n" +
                "uto1.u += new demo::services::TransferObject1()\n" +
                "var demo::services::TransferObject uto2 = new demo::services::TransferObject()\n" +
                "uto2.u += new demo::services::TransferObject1()\n" +
                "uto2.u += new demo::services::TransferObject1()\n" +
                "\n" +
                "var demo::services::TransferObject[] utos = new demo::services::TransferObject[] { uto1, uto2 }\n";
    }

    @Override
    public String getUse() {
        return "utos.u";
    }

    @Override
    public String getName() {
        return "SubjectUnboundRelationUC2UC";
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
