package hu.blackbelt.judo.runtime.core.security.keycloak;

/*-
 * #%L
 * JUDO Services Keycloak Security
 * %%
 * Copyright (C) 2018 - 2023 BlackBelt Technology
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

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.ProcessingException;
import java.net.ConnectException;
import java.time.Duration;

@Slf4j
public class RetryUtil {

    public static RetryRegistry createRetryRegistry(int maxAttempts, long waitDuration, boolean exponentialBackoff) {
        RetryConfig.Builder retryConfigBuilder = RetryConfig.<Void>custom()
                .maxAttempts(maxAttempts);

        if (exponentialBackoff) {
            retryConfigBuilder
                    .intervalFunction(IntervalFunction.ofExponentialBackoff(waitDuration));
        } else {
            retryConfigBuilder
                    .waitDuration(Duration.ofMillis(waitDuration));
        }
        retryConfigBuilder
                .failAfterMaxAttempts(true);


        retryConfigBuilder.retryExceptions(
                NotFoundException.class,
                ConnectException.class,
                ProcessingException.class,
                IllegalStateException.class);

        RetryConfig retryConfig = retryConfigBuilder.build();
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        return retryRegistry;
    }

    public static void registerLogEventHandlers(Retry retry) {
        Retry.EventPublisher eventPublisher = retry.getEventPublisher();
        eventPublisher.onError(e -> log.error("Error {} - {}", e.getNumberOfRetryAttempts(), e.getName()));
        eventPublisher.onRetry(e -> log.warn("Retry {} - {}", e.getNumberOfRetryAttempts(), e.getName()));
        eventPublisher.onSuccess(e -> log.info("Success {} - {}", e.getNumberOfRetryAttempts(), e.getName()));
        eventPublisher.onIgnoredError(e -> log.info("Ignored error {} - {}", e.getNumberOfRetryAttempts(), e.getName()));
    }
}
