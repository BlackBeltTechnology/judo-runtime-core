package hu.blackbelt.judo.runtime.core.query;

/*-
 * #%L
 * JUDO Runtime Core :: Query
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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Captures the events (INFO and above) of one class' logger for the duration of a test.
 */
final class LogCapture implements AutoCloseable {

    private final Logger logger;
    private final Level originalLevel;
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    LogCapture(final Class<?> loggerClass) {
        logger = (Logger) LoggerFactory.getLogger(loggerClass);
        originalLevel = logger.getLevel();
        logger.setLevel(Level.INFO);
        appender.start();
        logger.addAppender(appender);
    }

    /**
     * @return the formatted messages of the captured events of the level, or of a higher one
     */
    List<String> atLeast(final Level level) {
        return appender.list.stream()
                .filter(e -> e.getLevel().isGreaterOrEqual(level))
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.toList());
    }

    /**
     * @return the formatted messages of the captured events of exactly the level
     */
    List<String> only(final Level level) {
        return appender.list.stream()
                .filter(e -> e.getLevel().equals(level))
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.toList());
    }

    /**
     * @return the WARN and ERROR messages
     */
    List<String> warnings() {
        return atLeast(Level.WARN);
    }

    @Override
    public void close() {
        logger.detachAppender(appender);
        logger.setLevel(originalLevel);
        appender.stop();
    }
}
