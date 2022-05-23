package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.meta.rdbmsRules.Rule;
import hu.blackbelt.judo.runtime.core.dao.core.statements.Statement;
import lombok.Getter;
import lombok.ToString;
import org.eclipse.emf.ecore.EReference;

@ToString
@Getter
public class RdbmsReference<ID> {

    private Statement<ID> statement;

    private ID identifier;

    private ID oppositeIdentifier;

    private EReference reference;

    private Rule rule;

    private EReference oppositeReference;

    private Rule oppositeRule;

    RdbmsReference(Statement<ID> statement, ID identifier, ID oppositeIdentifier, EReference reference, Rule rule, EReference oppositeReference, Rule oppositeRule) {
        this.statement = statement;
        this.identifier = identifier;
        this.oppositeIdentifier = oppositeIdentifier;
        this.reference = reference;
        this.rule = rule;
        this.oppositeReference = oppositeReference;
        this.oppositeRule = oppositeRule;
    }

    public static <ID> RdbmsReferenceBuilder<ID> rdbmsReferenceBuilder() {
        return new RdbmsReferenceBuilder<ID>();
    }

    public boolean isSwapped() {
        if (!getRule().isForeignKey() && !getRule().isInverseForeignKey()) {
            return true;
        }
        return false;
    }

    public static class RdbmsReferenceBuilder<ID> {
        private Statement<ID> statement;
        private ID identifier;
        private ID oppositeIdentifier;
        private EReference reference;
        private Rule rule;
        private EReference oppositeReference;
        private Rule oppositeRule;

        RdbmsReferenceBuilder() {
        }

        public RdbmsReferenceBuilder<ID> statement(Statement<ID> statement) {
            this.statement = statement;
            return this;
        }

        public RdbmsReferenceBuilder<ID> identifier(ID identifier) {
            this.identifier = identifier;
            return this;
        }

        public RdbmsReferenceBuilder<ID> oppositeIdentifier(ID oppositeIdentifier) {
            this.oppositeIdentifier = oppositeIdentifier;
            return this;
        }

        public RdbmsReferenceBuilder<ID> reference(EReference reference) {
            this.reference = reference;
            return this;
        }

        public RdbmsReferenceBuilder<ID> rule(Rule rule) {
            this.rule = rule;
            return this;
        }

        public RdbmsReferenceBuilder<ID> oppositeReference(EReference oppositeReference) {
            this.oppositeReference = oppositeReference;
            return this;
        }

        public RdbmsReferenceBuilder<ID> oppositeRule(Rule oppositeRule) {
            this.oppositeRule = oppositeRule;
            return this;
        }

        public RdbmsReference<ID> build() {
            return new RdbmsReference<ID>(statement, identifier, oppositeIdentifier, reference, rule, oppositeReference, oppositeRule);
        }

        public String toString() {
            return "RdbmsReference.RdbmsReferenceBuilder(statement=" + this.statement + ", identifier=" + this.identifier + ", oppositeIdentifier=" + this.oppositeIdentifier + ", reference=" + this.reference + ", rule=" + this.rule + ", oppositeReference=" + this.oppositeReference + ", oppositeRule=" + this.oppositeRule + ")";
        }
    }
}
