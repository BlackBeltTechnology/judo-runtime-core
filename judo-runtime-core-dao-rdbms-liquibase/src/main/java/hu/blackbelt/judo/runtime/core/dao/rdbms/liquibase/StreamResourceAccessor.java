package hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase;

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

import liquibase.resource.AbstractResourceAccessor;
import liquibase.resource.InputStreamList;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;


@RequiredArgsConstructor
public class StreamResourceAccessor extends AbstractResourceAccessor {

    @NonNull
    private final Map<String, InputStream> streams;

    @Override
    public InputStreamList openStreams(String relativeTo, String streamPath) throws IOException {
        InputStreamList returnList = new InputStreamList();

        streams.entrySet().stream()
                .filter(s -> escape(s.getKey())
                        .equals(escape(streamPath)))
                .map(s -> s.getValue()).forEach(s -> {
                    returnList.add(URI.create(escape(streamPath)), s);
                });
        return returnList;
    }

    @Override
    public SortedSet<String> list(String relativeTo, String path, boolean recursive, boolean includeFiles, boolean includeDirectories) {
        return describeLocations();
    }

    @Override
    public SortedSet<String> describeLocations() {
        SortedSet<String> returnSet = new TreeSet<>();
        streams.keySet().forEach(k ->
                returnSet.add(escape(k))
        );
        return returnSet;
    }

    String escape(String str) {
        return str.replaceAll("[^\\p{IsAlphabetic}\\p{IsDigit}\\-\\_\\.]", "_");
    }
}
