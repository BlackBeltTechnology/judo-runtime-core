package hu.blackbelt.judo.runtime.core.dao.rdbms.query;

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

import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;

import java.text.MessageFormat;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AncestorNameFactory {

    private final EMap<EClass, Integer> ancestorIndexes = ECollections.asEMap(new ConcurrentHashMap<>());
    private final AtomicInteger nextAncestorIndex = new AtomicInteger(0);
    private static final String ANCESTOR_ALIAS_FORMAT = "_a{0,number,00}";

    public AncestorNameFactory(final Stream<EClass> classes) {
        ancestorIndexes.putAll(classes.collect(Collectors.toMap(c -> c, c -> nextAncestorIndex.incrementAndGet())));
    }

    /**
     * Get ancestor (holder of attribute that are not contained by a given class but on of its ancestors) name.
     *
     * @param clazz  ancestor
     * @return ancestor name (alias)
     */
    public synchronized String getAncestorPostfix(final EClass clazz) {
        final int index;
        if (ancestorIndexes.containsKey(clazz)) {
            index = ancestorIndexes.get(clazz);
        } else {
            index = nextAncestorIndex.incrementAndGet();
            ancestorIndexes.put(clazz, index);
        }
        return MessageFormat.format(ANCESTOR_ALIAS_FORMAT, index);
    }
}
