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

    private static final String DECIMAL_PATTERN = "DECIMAL(%d,%d)";

    private final Integer precision;
    private final Integer scale;

    /**
     * Create an instance of {@link RdbmsDecimalType} with default precision ({@link RdbmsDecimalType#DEFAULT_PRECISION})
     * and scale {@link RdbmsDecimalType#DEFAULT_SCALE}.
     */
    public RdbmsDecimalType() {
        this(DEFAULT_PRECISION, DEFAULT_SCALE);
    }

    /**
     * Create an instance of {@link RdbmsDecimalType}.
     *
     * @param precision {@link Integer}
     * @param scale     {@link Integer}
     *
     * @throws IllegalArgumentException If neither precision nor scale is null and precision < scale, or precision is null and DEFAULT_PRECISION < scale.
     */
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

    /**
     * Construct an {@link RdbmsDecimalType} of {@link RdbmsField} if it's an instance of an {@link RdbmsColumn} and has {@link DomainConstraints}.
     *
     * @param rdbmsField {@link RdbmsField}
     *
     * @return If <i>rdbmsField</i> is an {@link RdbmsColumn} and has {@link DomainConstraints} returns an {@link RdbmsDecimalType} with precision and scale based on {@link DomainConstraints}.
     * Otherwise, returns a default {@link RdbmsDecimalType}.
     */
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

    /**
     * Construct an {@link RdbmsDecimalType} of {@link Function}.
     *
     * @param function {@link Function}
     *
     * @return An {@link RdbmsDecimalType} based on <i>function</i>'s precision and scale properties.
     */
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

    /**
     * Expand current instance of {@link RdbmsDecimalType} with another instance by selecting the max of their precision and scale
     *
     * @param expansionTarget {@link RdbmsDecimalType}
     *
     * @return Expanded {@link RdbmsDecimalType} with current's- and <i>expansionTarget</i>'s max of precision and scale
     */
    public RdbmsDecimalType expandWith(RdbmsDecimalType expansionTarget) {
        RdbmsDecimalType rdbmsDecimalType = new RdbmsDecimalType();

        if (expansionTarget != null) {
            rdbmsDecimalType = new RdbmsDecimalType(Math.max(this.getPrecisionOrDefault(), expansionTarget.getPrecisionOrDefault()),
                                                    Math.max(this.getScaleOrDefault(), expansionTarget.getScaleOrDefault()));
        }

        return rdbmsDecimalType;

    }

    public Integer getPrecision() {
        return precision;
    }

    /**
     * Get precision or it's null get {@link RdbmsDecimalType#DEFAULT_PRECISION}
     *
     * @return precision it no null, {@link RdbmsDecimalType#DEFAULT_PRECISION} otherwise
     */
    public Integer getPrecisionOrDefault() {
        return Objects.requireNonNullElse(precision, DEFAULT_PRECISION);
    }

    public Integer getScale() {
        return scale;
    }

    /**
     * Get scale or it's null get {@link RdbmsDecimalType#DEFAULT_SCALE}
     *
     * @return scale it no null, {@link RdbmsDecimalType#DEFAULT_SCALE} otherwise
     */
    public Integer getScaleOrDefault() {
        return Objects.requireNonNullElse(scale, DEFAULT_SCALE);
    }

    /**
     * <p>Generate SQL type from {@link RdbmsDecimalType} instance. To comply mathematical rules, for result precision current instance's precision and scale is added.</p>
     * <p>E.g.: precision: 10, scale: 2 => DECIMAL(12, 2)</p>
     *
     * @return {@link String} containing SQL type from {@link RdbmsDecimalType} instance.
     */
    public String toSql() {
        Integer scale = getScaleOrDefault();
        return String.format(DECIMAL_PATTERN, getPrecisionOrDefault() + scale, scale);
    }

    /**
     * Get current instance of {@link RdbmsDecimalType} in {@link String} form containing actual precision and scale and additionally future sql result.
     *
     * @return {@link String} containing actual precision and scale and additionally future sql result. E.g.: "DECIMAL(10, 2) (=> DECIMAL(12, 2))"
     */
    @Override
    public String toString() {
        return String.format("%s (=> %s)", String.format(DECIMAL_PATTERN, precision, scale), toSql());
    }

}
