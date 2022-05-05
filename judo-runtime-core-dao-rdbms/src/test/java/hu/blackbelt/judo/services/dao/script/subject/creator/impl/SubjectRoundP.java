package hu.blackbelt.judo.services.dao.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator;

public class SubjectRoundP implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectRoundP() {
        modelBuilder = new PsmTestModelBuilder();
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::types::Double d = 3.14\n";
    }

    @Override
    public String getUse() {
        return "d!round()";
    }

    @Override
    public String getName() {
        return "SubjectRoundP";
    }

    @Override
    public String getReturnType() {
        return "demo::types::Integer";
    }
}
