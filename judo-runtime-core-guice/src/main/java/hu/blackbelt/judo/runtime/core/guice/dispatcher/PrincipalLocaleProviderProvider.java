package hu.blackbelt.judo.runtime.core.guice.dispatcher;

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

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.PrincipalLocaleProvider;
import hu.blackbelt.judo.runtime.core.guice.JudoConfigurationQualifiers;
import hu.blackbelt.osgi.i18n.api.LocaleProvider;

import javax.annotation.Nullable;

public class PrincipalLocaleProviderProvider implements Provider<LocaleProvider> {

    @Inject
    Context context;

    @Inject(optional = true)
    @JudoConfigurationQualifiers.ActorResolverPrincipalLocaleAttribute
    @Nullable
    String principalLocaleAttribute = null;

    @Inject(optional = true)
    @JudoConfigurationQualifiers.ActorResolverSupportedLanguages
    @Nullable
    String supportedLanguages = null;

    @Inject(optional = true)
    @JudoConfigurationQualifiers.ActorResolverDefaultLanguage
    @Nullable
    String defaultLanguage = null;

    @Override
    public LocaleProvider get() {
        return new PrincipalLocaleProvider(context, principalLocaleAttribute, supportedLanguages, defaultLanguage);
    }
}
