package hu.blackbelt.judo.runtime.core.dispatcher.converters;

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

import com.google.gson.Gson;
import hu.blackbelt.judo.dispatcher.api.FileType;
import hu.blackbelt.mapper.api.Formatter;

import java.util.Map;
import java.util.TreeMap;

public class FileTypeFormatter implements Formatter<FileType> {

    private static final String KEY_ID = "id";
    private static final String KEY_FILENAME = "fileName";
    private static final String KEY_SIZE = "size";
    private static final String KEY_MIME_TYPE = "mimeType";

    @Override
    public String convertValueToString(final FileType fileType) {
        final Map<String, Object> map = new TreeMap<>();
        map.put(KEY_ID, fileType.getId());
        map.put(KEY_FILENAME, fileType.getFileName());
        map.put(KEY_SIZE, fileType.getSize());
        map.put(KEY_MIME_TYPE, fileType.getMimeType());
        return new Gson().toJson(map);
    }

    @Override
    public FileType parseString(final String str) {
        @SuppressWarnings("unchecked")
		final Map<String, Object> map = new Gson().fromJson(str, Map.class);
        return FileType.builder()
                .id((String) map.get(KEY_ID))
                .fileName((String) map.get(KEY_FILENAME))
                .size(map.get(KEY_SIZE) != null ? Double.valueOf(map.get(KEY_SIZE).toString()).longValue() : null)
                .mimeType((String) map.get(KEY_MIME_TYPE))
                .build();
    }

    @Override
    public Class<FileType> getType() {
        return FileType.class;
    }
}
