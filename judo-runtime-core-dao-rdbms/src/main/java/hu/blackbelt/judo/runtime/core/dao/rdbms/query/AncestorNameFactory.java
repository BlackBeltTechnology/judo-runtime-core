package hu.blackbelt.judo.runtime.core.dao.rdbms.query;

import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;

import java.text.MessageFormat;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AncestorNameFactory {

    private final EMap<EClass, Integer> ancestorIndexes = ECollections.asEMap(new ConcurrentHashMap<>());
    private final AtomicInteger nextAncestorIndex = new AtomicInteger(0);
    private static final String ANCESTOR_ALIAS_FORMAT = "_a{0,number,00}";

    public AncestorNameFactory(final Stream<EClass> classes) {
        ancestorIndexes.putAll(classes.collect(Collectors.toMap(c -> c, c -> nextAncestorIndex.incrementAndGet())));
    }

    /**
     * Get ancestor (holder of attribute that are not contained by a given class but on of its ancestors) name.
     *
     * @param clazz  ancestor
     * @return ancestor name (alias)
     */
    public synchronized String getAncestorPostfix(final EClass clazz) {
        final int index;
        if (ancestorIndexes.containsKey(clazz)) {
            index = ancestorIndexes.get(clazz);
        } else {
            index = nextAncestorIndex.incrementAndGet();
            ancestorIndexes.put(clazz, index);
        }
        return MessageFormat.format(ANCESTOR_ALIAS_FORMAT, index);
    }
}
