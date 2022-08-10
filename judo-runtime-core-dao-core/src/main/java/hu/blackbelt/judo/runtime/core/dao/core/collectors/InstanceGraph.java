package hu.blackbelt.judo.runtime.core.dao.core.collectors;

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

import lombok.NonNull;

import java.util.ArrayList;
import java.util.Collection;

@lombok.Getter
@lombok.Builder
public class InstanceGraph<ID> {

    @NonNull
    private ID id;

    private final Collection<InstanceReference<ID>> containments = new ArrayList<>();

    private final Collection<InstanceReference<ID>> references = new ArrayList<>();

    private final Collection<InstanceReference<ID>> backReferences = new ArrayList<>();

    @Override
    public String toString() {
        return "[id=" + id +
                (containments.isEmpty() ? "" : ", containments=" + containments) +
                (references.isEmpty() ? "" : ", references=" + references) +
                (backReferences.isEmpty() ? "" : ", backReferences=" + backReferences) +
                "]";
    }
}
