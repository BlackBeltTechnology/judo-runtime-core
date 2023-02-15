package hu.blackbelt.judo.runtime.core.dao.rdbms.query.types;

import hu.blackbelt.judo.meta.query.Function;
import hu.blackbelt.judo.meta.query.ResultConstraint;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsColumn;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField.DomainConstraints;

import java.util.Objects;

public class RdbmsDecimalType {

    public static final int DEFAULT_PRECISION = 35;
    public static final int DEFAULT_SCALE = 20;
    public static final String DECIMAL_PATTERN = "DECIMAL(%d,%d)";

    private final Integer precision;
    private final Integer scale;

    public RdbmsDecimalType() {
        this(DEFAULT_PRECISION, DEFAULT_SCALE);
    }

    public RdbmsDecimalType(Integer precision, Integer scale) {
        if (precision != null && scale != null && precision < scale) {
            throw new IllegalArgumentException("Precision (" + precision + ") cannot be smaller than scale (" + scale + ")");
        }
        if (precision == null && scale != null && DEFAULT_PRECISION < scale) {
            throw new IllegalArgumentException("Default precision (" + DEFAULT_PRECISION + ") cannot be smaller than scale (" + scale + ")");
        }

        this.precision = precision;
        this.scale = scale;
    }

    public static RdbmsDecimalType of(RdbmsField rdbmsField) {
        RdbmsDecimalType rdbmsDecimalType = new RdbmsDecimalType();

        if (rdbmsField instanceof RdbmsColumn) { // included null check
            DomainConstraints constraints = ((RdbmsColumn) rdbmsField).getSourceDomainConstraints();
            if (constraints != null) {
                rdbmsDecimalType = new RdbmsDecimalType(constraints.getPrecision(), constraints.getScale());
            }
        }

        return rdbmsDecimalType;
    }

    public static RdbmsDecimalType of(Function function) {
        RdbmsDecimalType rdbmsDecimalType = new RdbmsDecimalType();

        if (function != null) {
            final Integer precision = function.getConstraints().stream()
                                              .filter(c -> ResultConstraint.PRECISION.equals(c.getResultConstraint()))
                                              .map(c -> Integer.parseInt(c.getValue()))
                                              .findAny()
                                              .orElse(null);
            final Integer scale = function.getConstraints().stream()
                                          .filter(c -> ResultConstraint.SCALE.equals(c.getResultConstraint()))
                                          .map(c -> Integer.parseInt(c.getValue()))
                                          .findAny()
                                          .orElse(null);
            rdbmsDecimalType = new RdbmsDecimalType(precision, scale);
        }

        return rdbmsDecimalType;
    }

    public RdbmsDecimalType expandWith(RdbmsDecimalType expansionTarget) {
        RdbmsDecimalType rdbmsDecimalType = new RdbmsDecimalType();

        if (expansionTarget != null) {
            if (expansionTarget.getPrecisionOrDefault() < this.getPrecisionOrDefault()) {
                throw new IllegalArgumentException("Expanded type's precision (" + expansionTarget.getPrecisionOrDefault() + ") " +
                                                   "cannot be smaller than current precision (" + this.getPrecisionOrDefault() + ")");
            }

            rdbmsDecimalType = new RdbmsDecimalType(Math.max(this.getPrecisionOrDefault(), expansionTarget.getPrecisionOrDefault()),
                                                    Math.max(this.getScaleOrDefault(), expansionTarget.getScaleOrDefault()));
        }

        return rdbmsDecimalType;

    }

    public Integer getPrecision() {
        return precision;
    }

    public Integer getPrecisionOrDefault() {
        return Objects.requireNonNullElse(precision, DEFAULT_PRECISION);
    }

    public Integer getScale() {
        return scale;
    }

    public Integer getScaleOrDefault() {
        return Objects.requireNonNullElse(scale, DEFAULT_SCALE);
    }

    public String toSql() {
        return String.format(DECIMAL_PATTERN, getPrecisionOrDefault(), getScaleOrDefault());
    }

    @Override
    public String toString() {
        return String.format(DECIMAL_PATTERN, precision, scale);
    }

}
