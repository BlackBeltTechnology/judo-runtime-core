package hu.blackbelt.judo.runtime.core.dao.rdbms.query.types;

import hu.blackbelt.judo.meta.query.Function;
import hu.blackbelt.judo.meta.query.ResultConstraint;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField.DomainConstraints;

import java.util.Objects;

public class RdbmsDecimalType {

    // TODO: JNG-4468 - Max precision and scale should be configured with environment/application variables
    private static final int MAX_PRECISION = 130;
    private static final int MAX_SCALE = 30;
    public static final int DEFAULT_PRECISION = MAX_PRECISION - MAX_SCALE; // considered as maximum value for caller
    public static final int DEFAULT_SCALE = MAX_PRECISION - DEFAULT_PRECISION; // considered as maximum value for caller

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
     * @throws IllegalArgumentException
     * <p>precision is not null and {@link RdbmsDecimalType#DEFAULT_PRECISION} < precision</p>
     * <p>OR scale is not null and {@link RdbmsDecimalType#DEFAULT_SCALE} < scale</p>
     * <p>OR precision is not null, scale is not null and precision < scale</p>
     * <p>OR precision is not null, scale is null and precision < {@link RdbmsDecimalType#DEFAULT_SCALE}</p>
     * <p>OR precision is null, scale is not null and {@link RdbmsDecimalType#DEFAULT_PRECISION} < scale</p>
     * <p>OR precision is null, scale is null and {@link RdbmsDecimalType#DEFAULT_PRECISION} < {@link RdbmsDecimalType#DEFAULT_SCALE}</p>
     */
    public RdbmsDecimalType(Integer precision, Integer scale) {
        // precision check
        if (precision != null && DEFAULT_PRECISION < precision) {
            throw new IllegalArgumentException("Precision (" + precision + ") cannot be higher than allowed maximum (" + DEFAULT_PRECISION + ")");
        }

        // scale check
        if (scale != null && DEFAULT_SCALE < scale) {
            throw new IllegalArgumentException("Scale (" + scale + ") cannot be higher than allowed maximum (" + DEFAULT_SCALE + ")");
        }

        // precision vs scale
        if (precision != null && scale != null && precision < scale) {
            throw new IllegalArgumentException("Precision (" + precision + ") cannot be less than scale (" + scale + ")");
        }

        // precision vs DEFAULT_SCALE
        if (precision != null && scale == null && precision < DEFAULT_SCALE) {
            throw new IllegalArgumentException("Precision (" + precision + ") cannot be less than scale (" + DEFAULT_SCALE + ")");
        }

        // DEFAULT_PRECISION vs scale
        if (precision == null && scale != null && DEFAULT_PRECISION < scale) {
            throw new IllegalArgumentException("Precision (" + DEFAULT_PRECISION + ") cannot be less than scale (" + scale + ")");
        }

        // DEFAULT_PRECISION vs DEFAULT_SCALE
        if (precision == null && scale == null && DEFAULT_PRECISION < DEFAULT_SCALE) {
            throw new IllegalArgumentException("Precision (" + DEFAULT_PRECISION + ") cannot be less than scale (" + DEFAULT_SCALE + ")");
        }

        this.precision = precision;
        this.scale = scale;
    }

    public Integer getPrecision() {
        return precision;
    }

    /**
     * Get precision or if it's null get {@link RdbmsDecimalType#DEFAULT_PRECISION}
     *
     * @return precision if not null, {@link RdbmsDecimalType#DEFAULT_PRECISION} otherwise
     */
    public Integer getPrecisionOrDefault() {
        return Objects.requireNonNullElse(precision, DEFAULT_PRECISION);
    }

    public Integer getScale() {
        return scale;
    }

    /**
     * Get scale or if it's null get {@link RdbmsDecimalType#DEFAULT_SCALE}
     *
     * @return scale if not null, {@link RdbmsDecimalType#DEFAULT_SCALE} otherwise
     */
    public Integer getScaleOrDefault() {
        return Objects.requireNonNullElse(scale, DEFAULT_SCALE);
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
     * Cut result according to target scale
     */
    public static String cutResult(String result, String targetSqlType, Integer scale) {
        String defaultType = new RdbmsDecimalType().toSql();
        if (scale != null && scale == 0) {
            return String.format("CAST(FLOOR(CAST(%s AS %s)) AS %s)", result, defaultType, targetSqlType);
        } else {
            String zeros = "0".repeat(Objects.requireNonNullElse(scale, DEFAULT_SCALE));
            return String.format("CAST(CAST(FLOOR(CAST(%s AS %s) * 1%s) AS %s) / 1%s AS %s)",
                                 result, defaultType, zeros, defaultType, zeros, targetSqlType);
        }
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
