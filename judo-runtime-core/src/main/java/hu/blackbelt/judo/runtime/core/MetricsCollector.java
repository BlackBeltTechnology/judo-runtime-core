package hu.blackbelt.judo.runtime.core;

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

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics collector.
 */
public interface MetricsCollector {

    String FRAMEWORK_METRICS = "FRAMEWORK_METRICS";

    /**
     * Starts a new (nested) measurement with the given key.
     * @param key the key of the measurement to be started
     * @return MetricsCancelToken that can be used to auto stop measurement in a try block.
     */
    MetricsCancelToken start(String key);

    /**
     * Stops the measurement with the given key.
     * @param key the key of the measurement to be stopped
     * @throws IllegalStateException if the key of the measurement does not match that of the last started one
     */
    void stop(String key) throws IllegalStateException;

    /**
     * Returns the so far collected metrics.
     */
    Map<String, AtomicLong> getMetrics();
}
