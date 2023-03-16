package hu.blackbelt.judo.runtime.core.dispatcher.querymask;

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

import hu.blackbelt.judo.services.dispatcher.QueryMaskLexer;
import hu.blackbelt.judo.services.dispatcher.QueryMaskParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.eclipse.emf.ecore.EClass;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public abstract class QueryMaskStringParser {

    public static Map<String, Object> parseQueryMask(final EClass clazz, final String maskString) {
        requireNonNull(clazz, "Parameter 'clazz' must not be null");

        if (maskString == null) {
            return null;
        }

        final QueryMaskLexer lexer = new QueryMaskLexer(new ANTLRInputStream(maskString));
        // Get a list of matched tokens
        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        // Pass the tokens to the parser
        final QueryMaskParser parser = new QueryMaskParser(tokens);

        final QueryMaskParserErrorListener errorListener = new QueryMaskParserErrorListener(maskString);
        parser.addErrorListener(errorListener);

        // Specify our entry point
        final QueryMaskParser.ParseContext queryGraphParserSentenceContext = parser.parse();

        if (errorListener.isFail()) {
            throw new IllegalArgumentException(errorListener.getMessage());
        }

        // Walk it and attach our listener
        final ParseTreeWalker walker = new ParseTreeWalker();

        final QueryMaskParserListener listener = new QueryMaskParserListener(clazz);
        walker.walk(listener, queryGraphParserSentenceContext);
        return listener.getResult();
    }
}
