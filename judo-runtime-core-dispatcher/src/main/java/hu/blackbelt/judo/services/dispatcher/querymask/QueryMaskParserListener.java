package hu.blackbelt.judo.services.dispatcher.querymask;

import com.google.common.base.Strings;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.services.dispatcher.QueryMaskBaseListener;
import hu.blackbelt.judo.services.dispatcher.QueryMaskParser;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class QueryMaskParserListener extends QueryMaskBaseListener {

    public static final String TAB = "\t";
    public static final String SEP = " - ";

    private Deque<Context> contextDeque = new ArrayDeque<>();
    private Context currentContext;

    private Map<String, Object> mask;

    private int indent;

    public QueryMaskParserListener(final EClass clazz) {
        currentContext = new Context(clazz, null);
    }

    public Map<String, Object> getResult() {
        return mask;
    }

    @Override
    public void enterAttribute(QueryMaskParser.AttributeContext ctx) {
        final String attibuteName = ctx.IDENTIFIER().getSymbol().getText();
        log.trace(Strings.repeat(TAB, indent) + "enterAttribute: " + ctx.IDENTIFIER().getSymbol().getText() + SEP + ctx.getText());

        checkArgument(currentContext.clazz.getEAllAttributes().stream().anyMatch(a -> Objects.equals(a.getName(), attibuteName)),
                String.format("Attribute: %s not found on %s ", attibuteName, AsmUtils.getClassifierFQName(currentContext.clazz)));
        currentContext.mask.put(attibuteName, true);
    }

    @Override
    public void enterRelation(QueryMaskParser.RelationContext ctx) {
        final String relationName = ctx.IDENTIFIER().getSymbol().getText();
        log.trace(Strings.repeat(TAB, indent) + "enterRelation: " + ctx.IDENTIFIER().getSymbol().getText() + SEP + ctx.getText());
        indent = indent + 1;
        contextDeque.push(currentContext);

        checkArgument(currentContext.clazz.getEAllReferences().stream().anyMatch(r -> Objects.equals(r.getName(), relationName)),
                String.format("Relation: %s not found on %s ", relationName, AsmUtils.getClassifierFQName(currentContext.clazz)));

        final EClass relationType = currentContext.clazz.getEAllReferences().stream().filter(r -> Objects.equals(r.getName(), relationName))
                .map(r -> r.getEReferenceType())
                .findAny()
                .get();
        final Context relationContext = new Context(relationType, relationName);

        currentContext = relationContext;
    }

    @Override
    public void exitRelation(QueryMaskParser.RelationContext ctx) {
        indent = indent - 1;
        log.trace(Strings.repeat(TAB, indent) + "exitRelation: " + ctx.IDENTIFIER().getSymbol().getText() + SEP + ctx.getText());

        Context upperContext = contextDeque.pop();

        upperContext.mask.put(currentContext.name, currentContext.mask);
        currentContext = upperContext;
    }

    @Override
    public void exitParse(QueryMaskParser.ParseContext ctx) {
        mask = currentContext.mask;
    }

    @Data
    private class Context {
        final EClass clazz;
        final String name;
        final Map<String, Object> mask;

        Context(EClass clazz, String name) {
            this.clazz = clazz;
            this.name = name;
            this.mask = new TreeMap<>();
        }
    }
}
