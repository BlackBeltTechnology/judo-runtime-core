package hu.blackbelt.judo.runtime.core.dispatcher.context;

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

import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkArgument;

@Builder
@Slf4j
public class ThreadContext implements Context {

    public static final String DEBUG_THREAD_FORK = "debugThreadFork";
    public static final String INHERITABLE_CONTEXT = "inheritableContext";

    private static ThreadLocal<Map<String, Object>> THREADLOCAL = new InheritableThreadLocal<>();

    @Builder.Default
    @NonNull
    @Setter
    private Boolean debugThreadFork = false;

    @Builder.Default
    @NonNull
    @Setter
    private Boolean inheritableContext = true;

    @NonNull
    @Setter
    DataTypeManager dataTypeManager;

    public ThreadContext(Boolean debugThreadFork, Boolean inheritableContext, DataTypeManager dataTypeManager) {
        this.dataTypeManager = dataTypeManager;
        this.debugThreadFork = debugThreadFork;
        this.inheritableContext = inheritableContext;
        setupThreadLocal();
    }

    public ThreadContext(DataTypeManager dataTypeManager) {
        this.dataTypeManager = dataTypeManager;
        this.debugThreadFork = false;
        this.inheritableContext = true;
        setupThreadLocal();
    }

    public ThreadContext() {
        this.debugThreadFork = false;
        this.inheritableContext = true;
        setupThreadLocal();
    }

    private void setupThreadLocal() {
        THREADLOCAL.remove();
        if (inheritableContext) {
            THREADLOCAL = new InheritableThreadLocal<>() {
                @Override
                protected Map<String, Object> childValue(Map<String, Object> parentValue) {
                    if (debugThreadFork) {
                        try {
                            throw new IllegalStateException("=== fork === | " + Thread.currentThread().getName());
                        } catch (IllegalStateException e) {
                            log.debug("!!!! FORK !!!!", e);
                        }
                    }
                    return super.childValue(parentValue);
                }
            };
        } else {
            THREADLOCAL = new ThreadLocal<>();
        }
    }

    @Override
    public Object get(final String key) {
        final Map<String, Object> perThreadResources = THREADLOCAL.get();
        if (perThreadResources != null) {
            synchronized (perThreadResources) {
                return perThreadResources.get(key);
            }
        } else {
            return null;
        }
    }

    @Override
    public <T> T getAs(final Class<T> clazz, final String key) {
        return dataTypeManager.getCoercer().coerce(get(key), clazz);
    }

    @Override
    public void put(final String key, final Object value) {
        checkArgument(key != null, "Key must not be null");

        if (value == null) {
            remove(key);
        } else {
            ensureResourcesInitialized().put(key, value);
        }
    }

    @SuppressWarnings("unchecked")
	@Override
    public <T> T putIfAbsent(String key, T value) {
        checkArgument(key != null, "Key must not be null");

        // TODO: It's not compatible with standard Map implementation
        // return (T) ensureResourcesInitialized().putIfAbsent(key, value);

        final Map<String, Object> perThreadResources = ensureResourcesInitialized();
        synchronized (perThreadResources) {
            perThreadResources.putIfAbsent(key, value);
        }
        return (T) get(key);
    }

    @Override
    public Object remove(final String key) {
        final Map<String, Object> perThreadResources = THREADLOCAL.get();
        if (perThreadResources != null) {
            synchronized (perThreadResources) {
                return perThreadResources.remove(key);
            }
        } else {
            return null;
        }
    }

    @Override
    public void removeAll() {
        final Map<String, Object> perThreadResources = THREADLOCAL.get();
        if (perThreadResources != null) {
            synchronized (perThreadResources) {
                perThreadResources.clear();
            }
        }
        THREADLOCAL.remove();
    }

    private Map<String, Object> ensureResourcesInitialized() {
        if (THREADLOCAL.get() == null) {
            THREADLOCAL.set(new TreeMap<>());
        }
        return THREADLOCAL.get();
    }
}
