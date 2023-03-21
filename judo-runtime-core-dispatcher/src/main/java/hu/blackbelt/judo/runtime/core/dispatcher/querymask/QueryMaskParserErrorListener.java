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

import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;

import java.util.BitSet;

public class QueryMaskParserErrorListener implements ANTLRErrorListener {

    private boolean fail;

    private String message;

    private final String sentence;


    public QueryMaskParserErrorListener(String sentence) {
        this.sentence = sentence;
    }

    public boolean isFail() {
        return fail;
    }

    public String getMessage() {
        return message;
    }

    public void setFail(boolean pFail, String pMessage) {
        this.fail = pFail;
        this.message = pMessage;
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer,
                            Object offendingSymbol,
                            int line,
                            int charPositionInLine,
                            String msg,
                            RecognitionException e) {
        setFail(true, String.format("Syntax error - Line: %d Col: %d \nMessage: %s \nQuery: %s", line, charPositionInLine, msg, sentence));
    }

    @Override
    public void reportAmbiguity(Parser recognizer,
                                DFA dfa,
                                int startIndex,
                                int stopIndex,
                                boolean exact,
                                BitSet ambigAlts,
                                ATNConfigSet configs) {
        setFail(true, String.format("Ambiguity error \nStart: %d Stop: %d Exact %d \nAmbigous alternatives: %s \nDFA: %s \nQuery: %s",
                startIndex, stopIndex, exact, ambigAlts, dfa, sentence));

    }

    @Override
    public void reportAttemptingFullContext(Parser recognizer,
                                            DFA dfa,
                                            int startIndex,
                                            int stopIndex,
                                            BitSet conflictingAlts,
                                            ATNConfigSet configs) {
        setFail(true, String.format("Attempting full context error. \nStart: %d Stop: %d \nConflicting alternatives: %s \nDFA: %s \nQuery: %s",
                startIndex, stopIndex, conflictingAlts, dfa, sentence));
    }

    @Override
    public void reportContextSensitivity(Parser recognizer,
                                         DFA dfa,
                                         int startIndex,
                                         int stopIndex,
                                         int prediction,
                                         ATNConfigSet configs) {
        setFail(true, String.format("Context sensitivity error. \nStart: %d Stop: %d Predicition: %d \nDFA: %s \nQuery: %s",
                startIndex, stopIndex, prediction, dfa, sentence));
    }
}
