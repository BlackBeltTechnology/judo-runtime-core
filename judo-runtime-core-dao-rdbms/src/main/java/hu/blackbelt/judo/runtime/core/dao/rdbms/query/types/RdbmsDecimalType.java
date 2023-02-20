package hu.blackbelt.judo.runtime.core.dao.rdbms.query.types;

import hu.blackbelt.judo.meta.query.Function;
import hu.blackbelt.judo.meta.query.ResultConstraint;

import java.util.Objects;

public class RdbmsDecimalType {

    // TODO: JNG-4468 - Default (and max) precision and scale should be configured with environment variables
    public static final int MAX_PRECISION = 100; // default
    public static final int MAX_SCALE = 30; // default

    private static final String DECIMAL_PATTERN = "DECIMAL(%d,%d)";

    private final Integer precision;
    private final Integer scale;

    /**
     * Create an instance of {@link RdbmsDecimalType} with default precision ({@link RdbmsDecimalType#MAX_PRECISION})
     * and scale {@link RdbmsDecimalType#MAX_SCALE}.
     */
    public RdbmsDecimalType() {
        this(MAX_PRECISION, MAX_SCALE);
    }

    /**
     * Create an instance of {@link RdbmsDecimalType}.
     *
     * @param precision {@link Integer}
     * @param scale     {@link Integer}
     *
     * @throws IllegalArgumentException
     * <p>If <i>precision</i> is not null and <i>precision</i> > {@link RdbmsDecimalType#MAX_PRECISION}.</p>
     * <p>If <i>scale</i> is not null and <i>scale</i> > {@link RdbmsDecimalType#MAX_SCALE}.</p>
     * <p>If <i>precision</i> is not null, <i>scale</i> is not null and <i>precision</i> < <i>scale</i>.</p>
     * <p>If <i>precision</i> is null, <i>scale</i> is not null and {@link RdbmsDecimalType#MAX_PRECISION} < <i>scale</i>.</p>
     */
    public RdbmsDecimalType(Integer precision, Integer scale) {
        if (precision != null && precision > MAX_PRECISION) {
            throw new IllegalArgumentException("Precision (" + precision + ") cannot be higher than allowed maximum (" + MAX_PRECISION + ")");
        }
        if (scale != null && scale > MAX_SCALE) {
            throw new IllegalArgumentException("Scale (" + scale + ") cannot be higher than allowed maximum (" + MAX_SCALE + ")");
        }
        if (precision != null && scale != null && precision < scale) {
            throw new IllegalArgumentException("Precision (" + precision + ") cannot be less than scale (" + scale + ")");
        }
        if (precision == null && scale != null && MAX_PRECISION < scale) {
            throw new IllegalArgumentException("Scale (" + scale + ") cannot be higher than maximum precision (" + MAX_PRECISION + ")");
        }

        this.precision = precision;
        this.scale = scale;
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

    public Integer getPrecision() {
        return precision;
    }

    /**
     * Get precision or if it's null get {@link RdbmsDecimalType#MAX_PRECISION}
     *
     * @return precision if not null, {@link RdbmsDecimalType#MAX_PRECISION} otherwise
     */
    public Integer getPrecisionOrDefault() {
        return Objects.requireNonNullElse(precision, MAX_PRECISION);
    }

    public Integer getScale() {
        return scale;
    }

    /**
     * Get scale or if it's null get {@link RdbmsDecimalType#MAX_SCALE}
     *
     * @return scale if not null, {@link RdbmsDecimalType#MAX_SCALE} otherwise
     */
    public Integer getScaleOrDefault() {
        return Objects.requireNonNullElse(scale, MAX_SCALE);
    }

    /**
     * <p>Generate SQL type from {@link RdbmsDecimalType} instance.</p>
     *
     * @return {@link String} containing SQL type from {@link RdbmsDecimalType} instance.
     */
    public String toSql() {
        return String.format(DECIMAL_PATTERN, getPrecisionOrDefault(), getScaleOrDefault());
    }

    /**
     * Get current instance of {@link RdbmsDecimalType} in {@link String} form containing actual precision and scale and additionally future sql result.
     *
     * @return {@link String} containing actual precision and scale and additionally future sql result. E.g.: "DECIMAL(10, 2) (=> DECIMAL(10, 2))"
     */
    @Override
    public String toString() {
        return String.format("%s (=> %s)", String.format(DECIMAL_PATTERN, precision, scale), toSql());
    }

}
