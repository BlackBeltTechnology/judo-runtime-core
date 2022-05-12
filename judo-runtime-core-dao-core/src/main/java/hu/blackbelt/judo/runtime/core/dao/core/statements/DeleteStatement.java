package hu.blackbelt.judo.runtime.core.dao.core.statements;

import hu.blackbelt.judo.runtime.core.dao.core.values.InstanceValue;

public class DeleteStatement<ID> extends Statement<ID> {

    public DeleteStatement(InstanceValue<ID> instance) {
        super(instance);
    }


    public String toString() {
        return "DeleteStatement(" + this.instance.toString() +  ")";
    }
}
