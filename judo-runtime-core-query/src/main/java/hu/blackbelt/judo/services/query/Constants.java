package hu.blackbelt.judo.services.query;

public class Constants {

    /**
     * Scale that is used by query builder to calculate rate of measured value. This value must be greater then
     * MEASURE_CONVERTING_SCALE and recommended to equal to MEASURE_CONVERTING_SCALE time 2.
     */
    public static final int MEASURE_RATE_CALCULATION_SCALE = 30;

    /**
     * Precision that is used by RDBMS to convert measured values.
     */
    public static final int MEASURE_CONVERTING_PRECISION = 100;

    /**
     * Scale that is used by RDBMS to convert measured values.
     */
    public static final int MEASURE_CONVERTING_SCALE = 15;
}
