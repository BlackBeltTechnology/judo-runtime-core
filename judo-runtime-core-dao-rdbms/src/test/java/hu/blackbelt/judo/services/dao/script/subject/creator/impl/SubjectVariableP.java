package hu.blackbelt.judo.services.dao.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator;

public class SubjectVariableP implements SubjectCreator {

    private final PsmTestModelBuilder modelBuilder;

    public SubjectVariableP() {
        modelBuilder = new PsmTestModelBuilder();
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::types::Double i = 3.14\n";
    }

    @Override
    public String getUse() {
        return "i";
    }

    @Override
    public String getName() {
        return "SubjectVariableP";
    }

    @Override
    public String getReturnType() {
        return "demo::types::Double";
    }
}
