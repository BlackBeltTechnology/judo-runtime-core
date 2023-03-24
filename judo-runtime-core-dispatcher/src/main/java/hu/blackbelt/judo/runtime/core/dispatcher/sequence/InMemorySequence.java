package hu.blackbelt.judo.runtime.core.dispatcher.sequence;

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

import hu.blackbelt.judo.dispatcher.api.Sequence;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@NoArgsConstructor
@AllArgsConstructor
public class InMemorySequence implements Sequence<Long> {

    private static final Map<String, AtomicLong> sequences = new ConcurrentHashMap<>();

    private Long start = DEFAULT_START;

    private Long increment = DEFAULT_INCREMENT;

    @Override
    public Long getNextValue(final String sequenceName) {
        return increment != null
                ? getSequence(sequenceName).addAndGet(increment)
                : getSequence(sequenceName).incrementAndGet();
    }

    @Override
    public Long getCurrentValue(String sequenceName) {
        return getSequence(sequenceName).get();
    }

    private synchronized AtomicLong getSequence(final String sequenceName) {
        if (sequences.containsKey(sequenceName)) {
            return sequences.get(sequenceName);
        } else {
            final AtomicLong sequence = start != null && increment != null ? new AtomicLong(start - increment) : new AtomicLong();
            sequences.put(sequenceName, sequence);
            return sequence;
        }
    }
}
