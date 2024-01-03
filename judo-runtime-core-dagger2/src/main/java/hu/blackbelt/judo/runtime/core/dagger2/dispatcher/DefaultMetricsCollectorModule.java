package hu.blackbelt.judo.runtime.core.dagger2.dispatcher;

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

import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultMetricsCollector;

import javax.annotation.Nullable;
import java.util.function.Consumer;

import javax.inject.Inject;
import javax.inject.Named;

import static java.util.Objects.requireNonNullElse;

@Module
public class DefaultMetricsCollectorModule {

    public static final String METRICS_COLLECTOR_CONSUMER = "metricsCollectorConsumer";
    public static final String METRICS_COLLECTOR_ENABLED = "metricsCollectorEnabled";
    public static final String METRICS_COLLECTOR_VERBOSE = "metricsCollectorVerbose";

    @SuppressWarnings("unchecked")
    @JudoApplicationScope
    @Provides
    public MetricsCollector providesMetricsCollector(
            Context context,
            @Named(METRICS_COLLECTOR_CONSUMER) @Nullable Consumer metricsConsumer,
            @Named(METRICS_COLLECTOR_ENABLED) @Nullable Boolean enabled,
            @Named(METRICS_COLLECTOR_VERBOSE) @Nullable Boolean verbose
    ) {
        return DefaultMetricsCollector.builder()
                .context(context)
                .metricsConsumer(requireNonNullElse(metricsConsumer, (m) -> {}))
                .enabled(requireNonNullElse(enabled, false))
                .verbose(requireNonNullElse(verbose, false))
                .build();
    }
}
