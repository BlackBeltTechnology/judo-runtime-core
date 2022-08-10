package hu.blackbelt.judo.runtime.core.dispatcher.security;

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

import hu.blackbelt.judo.runtime.core.accessmanager.api.SignedIdentifier;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.ETypedElement;

import java.util.Map;
import java.util.Optional;

public interface IdentifierSigner {

    String SIGNED_IDENTIFIER_KEY = "__signedIdentifier";

    void signIdentifiers(ETypedElement typedElement, Map<String, Object> payload, boolean immutable);

    Optional<SignedIdentifier> extractSignedIdentifier(EClass transferObjectType, Map<String, Object> payload);
}
