package hu.blackbelt.judo.services.dao.core.statements;

import hu.blackbelt.judo.services.dao.core.values.InstanceValue;

public class DeleteStatement<ID> extends Statement<ID> {

    public DeleteStatement(InstanceValue instance) {
        super(instance);
    }


    public String toString() {
        return "DeleteStatement(" + this.instance.toString() +  ")";
    }
}
