package hu.blackbelt.judo.runtime.core.query;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 * 
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 * 
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

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
