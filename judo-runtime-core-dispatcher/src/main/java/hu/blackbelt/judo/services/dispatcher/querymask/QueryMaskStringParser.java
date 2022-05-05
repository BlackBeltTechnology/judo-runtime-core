package hu.blackbelt.judo.services.dispatcher.querymask;

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
