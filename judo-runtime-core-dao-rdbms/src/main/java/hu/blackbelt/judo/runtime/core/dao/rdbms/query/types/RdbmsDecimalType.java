package hu.blackbelt.judo.runtime.core.dao.rdbms.query.types;

import java.util.Objects;

public class RdbmsDecimalType {

    // TODO: env
    public static final int MAX_PRECISION = 60; // default
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
     * <p>Create an instance of {@link RdbmsDecimalType}.</p>
     * <p>{@link RdbmsDecimalType} handles precision and scale as if scale is part of precision. E.g.: in case of precision: 5 and scale 2
     * the maximum values are <i>99999.0</i>, <i>9999.9</i>, <i>999.99</i></p>
     *
     * @param precision {@link Integer}
     * @param scale     {@link Integer}
     *
     * @throws IllegalArgumentException If <i>precision</i> is not null and <i>precision</i> > {@link RdbmsDecimalType#MAX_PRECISION}.
     * @throws IllegalArgumentException If <i>scale</i> is not null and <i>scale</i> > {@link RdbmsDecimalType#MAX_SCALE}.
     * @throws IllegalArgumentException If <i>precision</i> is not null, <i>scale</i> is not null and <i>precision</i> < <i>scale</i>.
     * @throws IllegalArgumentException If <i>precision</i> is null, <i>scale</i> is not null and {@link RdbmsDecimalType#MAX_PRECISION} < <i>scale</i>.
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

    public Integer getPrecision() {
        return precision;
    }

    /**
     * Get precision or it's null get {@link RdbmsDecimalType#MAX_PRECISION}
     *
     * @return precision it no null, {@link RdbmsDecimalType#MAX_PRECISION} otherwise
     */
    public Integer getPrecisionOrDefault() {
        return Objects.requireNonNullElse(precision, MAX_PRECISION);
    }

    public Integer getScale() {
        return scale;
    }

    /**
     * Get scale or it's null get {@link RdbmsDecimalType#MAX_SCALE}
     *
     * @return scale it no null, {@link RdbmsDecimalType#MAX_SCALE} otherwise
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
