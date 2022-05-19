package hu.blackbelt.judo.runtime.core.query;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.IntegerExpression;
import hu.blackbelt.judo.meta.expression.LogicalExpression;
import hu.blackbelt.judo.meta.expression.ReferenceExpression;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.collection.*;
import hu.blackbelt.judo.meta.expression.constant.IntegerConstant;
import hu.blackbelt.judo.meta.expression.object.*;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.meta.query.runtime.QueryUtils;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EReference;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.*;

@Slf4j
public abstract class JoinFactory {

    private final AsmModelAdapter modelAdapter;

    @Setter
    private FeatureFactory featureFactory;

    public JoinFactory(final AsmModelAdapter modelAdapter) {
        this.modelAdapter = modelAdapter;
    }

    public PathEnds convertNavigationToJoins(final Context context, final Node container, final ReferenceExpression expr, final boolean ignoreOrderBy) {
        final Context newContext = context.clone();

        final Deque<Navigation> navigations = new LinkedList<>();
        final Node base = extractNavigationsFromExpression(newContext, expr, navigations, ignoreOrderBy);
        if (log.isTraceEnabled()) {
            log.trace("Navigation steps:{}", navigations.stream().map(n -> "\n - " + n).collect(Collectors.joining()));
        }

        if (container != null && base.getType() != null && base.getType().getEAllSuperTypes().contains(container.getType())) {
            log.debug("Down casting (and filtering) result ({} -> {})", AsmUtils.getClassifierFQName(container.getType()), AsmUtils.getClassifierFQName(base.getType()));
            navigations.push(Navigation.builder().cast((EClass) modelAdapter.get(modelAdapter.buildTypeName(base.getType()).get()).get()).build());
        }

        if (container instanceof SubSelect) {
            ((SubSelect) container).setBase(base);
        }

        final PathEnds pathEnds = buildPath(container != null ? container : base, context, base, navigations)
                .base(base)
                .build();

        if (container instanceof SubSelect && pathEnds.getPartner() instanceof Join) {
            ((SubSelect) container).setPartner((Join) pathEnds.getPartner());
        }

        return pathEnds;
    }

    private PathEnds.PathEndsBuilder buildPath(Node container, final Context context, Node lastPartner, final Deque<Navigation> navigations) {
        boolean ordered = false;
        boolean limited = false;

        Integer limit = null;
        Integer offset = null;

        while (!navigations.isEmpty()) {
            final Navigation navigation = navigations.pop();

            final Optional<Navigation> objectSelection;
            if (navigation.getObjectSelector() == null) {
                // look ahead for any() expression
                final List<Navigation> reversedNavigations = new ArrayList<>(navigations);
                Collections.reverse(reversedNavigations);
                objectSelection = reversedNavigations.stream()
                        .filter(n -> n.getObjectSelector() != null)
                        .findFirst();

                // ignore navigations processed by object selector
                if (objectSelection.isPresent()) {
                    final Deque<Navigation> embeddedNavigations = new LinkedList<>();

                    Navigation n = navigation;
                    embeddedNavigations.addLast(n);
                    while (!Objects.equals(n, objectSelection.get())) {
                        n = navigations.pop();
                        embeddedNavigations.addLast(n);
                    }

                    if (log.isTraceEnabled()) {
                        log.trace("  => Embedded navigation steps:{}", embeddedNavigations.stream().map(en -> "\n   - " + en).collect(Collectors.joining()));
                    }
                }
            } else {
                objectSelection = Optional.of(navigation);
            }

            if (objectSelection.isPresent()) {
                final Context newContext = context.clone();
                final Join join = newSubSelectJoinBuilder()
                        .withAlias(QueryUtils.getNextJoinAlias(context.getSourceCounter()))
                        .withPartner(lastPartner)
                        .build();

                newContext.setNode(join);

                final Feature feature = featureFactory.convert(objectSelection.get().getObjectSelector(), newContext, null);
                checkArgument(feature instanceof SubSelectFeature, "Feature of object selector must be a SubSelectFeature");

                ((SubSelectJoin) join).setSubSelect(((SubSelectFeature) feature).getSubSelect());

                container.getJoins().add(join);
                lastPartner = join;
                container = join;
                ordered = false;
                continue;
            } else if (navigation.getFeatureName() != null) {
                final EReference reference = lastPartner.getType().getEAllReferences().stream().filter(r -> Objects.equals(r.getName(), navigation.getFeatureName())).findAny().get();

                checkArgument(reference != null, "Unknown reference: " + navigation.getFeatureName());
                checkArgument(!reference.isDerived(), "Derived references must be resolved by expression builder");
                if (limited) {
                    checkArgument(!reference.isMany(), "Collection navigation is not allowed here");
                    if (ordered) {
                        checkArgument(reference.isRequired(), "Optional navigation is not allowed here");
                    }
                }

                final Join join = newReferencedJoinBuilder()
                        .withAlias(QueryUtils.getNextJoinAlias(context.getSourceCounter()))
                        .withPartner(lastPartner)
                        .withReference(reference)
                        .build();

                // TODO: check if JOIN not included yet

                container.getJoins().add(join);
                lastPartner = join;
                container = join;
                ordered = false; //!reference.isMany(); DO NOT keep order even if transfer relation is single
            } else if (navigation.getContainer() != null) {
                final EClass containerType = navigation.getContainer();

                final EClass lastPartnerType = lastPartner.getType();
                final EList<EReference> references = ECollections.asEList(modelAdapter.getAsmUtils().all(EClass.class)
                        .filter(c -> AsmUtils.equals(containerType, c) || c.getEAllSuperTypes().contains(containerType))
                        .flatMap(c -> c.getEReferences().stream()
                                .filter(r -> r.isContainment() && (AsmUtils.equals(r.getEReferenceType(), lastPartnerType) || lastPartnerType.getEAllSuperTypes().contains(r.getEReferenceType()))))
                        .collect(Collectors.toList()));

                final Join join = newContainerJoinBuilder()
                        .withAlias(QueryUtils.getNextJoinAlias(context.getSourceCounter()))
                        .withPartner(lastPartner)
                        .withReferences(references)
                        .build();

                container.getJoins().add(join);
                lastPartner = join;
                container = join;
                ordered = false;
            } else if (navigation.getCast() != null) {
                checkArgument(!limited, "Filtering is not allowed here");
                final EClass castType = navigation.getCast();

                final Join join = newCastJoinBuilder()
                        .withAlias(QueryUtils.getNextJoinAlias(context.getSourceCounter()))
                        .withPartner(lastPartner)
                        .withType(castType)
                        .build();

                container.getJoins().add(join);
                lastPartner = join;
                container = join;
                ordered = false;
            } else if (navigation.getCondition() != null) {
                checkArgument(!limited, "Filtering is not allowed here");
                checkArgument(navigation.getVariableName() != null, "Missing variable name");

                final Filter filter = newFilterBuilder()
                        .withAlias(navigation.getVariableName())
                        .build();

                if (navigation.getCondition().eContainer() instanceof ObjectFilterExpression) {
                    final ObjectFilterExpression objectFilterExpression = (ObjectFilterExpression) navigation.getCondition().eContainer();
                    if (objectFilterExpression.getObjectExpression() instanceof ObjectVariableReference) {
                        final Join join = newCastJoinBuilder()
                                .withAlias(QueryUtils.getNextJoinAlias(context.getSourceCounter()))
                                .withPartner(lastPartner)
                                .withType(container.getType())
                                .build();
                        container.getJoins().add(join);
                        container = join;
                        lastPartner = join;
                    }
                }

                container.getFilters().add(filter);
                final Context filterContext = context.clone(navigation.getVariableName(), filter);
                filter.setFeature(expressionToFeature(navigation.getCondition(), filterContext, null));
            } else if (navigation.getOrderBy() != null) {
                checkArgument(!limited, "Ordering is not allowed");
                checkArgument(navigation.getVariableName() != null, "Missing variable name");

                final Node c = container;
                navigation.getOrderBy().forEach(orderBy -> {
                    final OrderBy o = newOrderByBuilder()
                            .withAlias(navigation.getVariableName())
                            .withDescending(orderBy.isDescending())
                            .build();
                    c.getOrderBys().add(o);
                    final Context orderByContext = context.clone(navigation.getVariableName(), o);
                    o.setFeature(expressionToFeature(orderBy.getExpression(), orderByContext, null));
                });
                ordered = true;
            } else if (navigation.getLimit() != null) {
                Node query = (Node) container.eContainer();
                while (query != null && !(query instanceof SubSelect)) {
                    query = (Node) query.eContainer();
                }

                if (query != null) {
                    checkArgument(query instanceof SubSelect, "Query must be a subselect");

                    ((SubSelect) query).setLimit(navigation.getLimit());
                    if (navigation.getOffset() != null) {
                        ((SubSelect) query).setOffset(navigation.getOffset());
                    }
                } else {
                    limit = navigation.getLimit();
                    if (navigation.getOrderBy() != null) {
                        offset = navigation.getOffset();
                    }
                }
                limited = navigation.getObjectSelector() == null;
            } else if (navigation.getObjectSelector() != null) {
                throw new IllegalStateException("Object selector is right-associative, must be handled before");
            } else {
                throw new IllegalStateException("Unsupported navigation");
            }
        }

        return PathEnds.builder()
                .partner(lastPartner)
                .ordered(ordered)
                .limit(limit)
                .offset(offset);
    }

    private Node extractNavigationsFromExpression(final Context context, ReferenceExpression expr, final Deque<Navigation> navigations, boolean ignoreOrderBy) {
        Node base = null;
        while (base == null) {
            if (expr instanceof ObjectNavigationExpression) {
                final ObjectNavigationExpression navigationExpression = (ObjectNavigationExpression) expr;
                navigations.push(Navigation.builder().featureName(navigationExpression.getReferenceName()).build());
                expr = navigationExpression.getObjectExpression();
            } else if (expr instanceof CollectionNavigationFromObjectExpression) {
                final CollectionNavigationFromObjectExpression navigationExpression = (CollectionNavigationFromObjectExpression) expr;
                navigations.push(Navigation.builder().featureName(navigationExpression.getReferenceName()).build());
                expr = navigationExpression.getObjectExpression();
            } else if (expr instanceof CollectionNavigationFromCollectionExpression) {
                final CollectionNavigationFromCollectionExpression navigationExpression = (CollectionNavigationFromCollectionExpression) expr;
                navigations.push(Navigation.builder().featureName(navigationExpression.getReferenceName()).build());
                expr = navigationExpression.getCollectionExpression();
            } else if (expr instanceof ObjectNavigationFromCollectionExpression) {
                final ObjectNavigationFromCollectionExpression navigationExpression = (ObjectNavigationFromCollectionExpression) expr;
                navigations.push(Navigation.builder().featureName(navigationExpression.getReferenceName()).build());
                expr = navigationExpression.getCollectionExpression();
            } else if (expr instanceof ObjectVariableReference) {
                final String variableName = ((ObjectVariableReference)expr).getVariableName();
                checkArgument(context.getVariables().containsKey(variableName), "Unknown variable name: " + variableName);

                base = context.getVariables().get(variableName);
                final ObjectVariableReference variableReference = (ObjectVariableReference) expr;
                final EClass typeOfVariable = (EClass) variableReference.getObjectType(modelAdapter);

                if (!(AsmUtils.equals(base.getType(), typeOfVariable) || base.getType().getEAllSuperTypes().contains(typeOfVariable) || typeOfVariable.getEAllSuperTypes().contains(base.getType()))) {
                    log.error("Invalid variable reference type; expected: {}, found: {}", typeOfVariable, base.getType());
                    throw new IllegalStateException("Invalid type of variable reference");
                }
            } else if (expr instanceof CollectionFilterExpression) {
                final CollectionFilterExpression collectionFilterExpression = (CollectionFilterExpression) expr;
                navigations.push(Navigation.builder().condition(collectionFilterExpression.getCondition()).variableName(collectionFilterExpression.getCollectionExpression().getIteratorVariableName()).build());
                expr = collectionFilterExpression.getCollectionExpression();
            } else if (expr instanceof ObjectFilterExpression) {
                final ObjectFilterExpression objectFilterExpression = (ObjectFilterExpression) expr;
                navigations.push(Navigation.builder().condition(objectFilterExpression.getCondition()).variableName(objectFilterExpression.getObjectExpression().getIteratorVariableName()).build());
                expr = objectFilterExpression.getObjectExpression();
            } else if (expr instanceof ImmutableCollection) {
                final ImmutableCollection immutableCollection = (ImmutableCollection) expr;
                final Optional<? extends EClassifier> classifier = modelAdapter.get(immutableCollection.getElementName());
                checkArgument(classifier.get() instanceof EClass, "Invalid collection type");

                base = newSelectBuilder()
                        .withFrom((EClass) classifier.get())
                        .withAlias(QueryUtils.getNextJoinAlias(context.getSourceCounter()))
                        .withMainTarget(newTargetBuilder()
                                .withIndex(context.getTargetCounter().incrementAndGet())
                                .build())
                        .build();
                ((Select) base).getTargets().add(((Select) base).getMainTarget());
                context.setNode(base);
                context.getQueryModelResourceSupport().addContent(base);
            } else if (expr instanceof SortExpression) {
                final SortExpression sortExpression = (SortExpression) expr;
                if (!ignoreOrderBy) {
                    navigations.push(Navigation.builder().orderBy(sortExpression.getOrderBy()).variableName(sortExpression.getCollectionExpression().getIteratorVariableName()).build());
                } else {
                    log.warn("Ordering ({}) is ignored in any() expression, use head()/tail() instead", sortExpression.getOrderBy());
                }
                expr = sortExpression.getCollectionExpression();
            } else if (expr instanceof ObjectSelectorExpression) {
                final ObjectSelectorExpression selectorExpression = (ObjectSelectorExpression) expr;
                navigations.push(Navigation.builder().objectSelector(selectorExpression).build());
                expr = selectorExpression.getCollectionExpression();
            } else if (expr instanceof SubCollectionExpression) {
                final SubCollectionExpression subCollectionExpression = (SubCollectionExpression) expr;
                final IntegerExpression limit = subCollectionExpression.getLength();
                final IntegerExpression offset = subCollectionExpression.getPosition();
                final Navigation.NavigationBuilder nb = Navigation.builder();

                if (limit instanceof IntegerConstant) {
                    nb.limit(((IntegerConstant) limit).getValue().intValue());
                } else if (limit != null) {
                    log.error("Limit value must be constant: {}", limit);
                }

                if (offset instanceof IntegerConstant) {
                    nb.offset(((IntegerConstant) offset).getValue().intValue());
                } else if (offset != null) {
                    log.error("Offset value must be constant: {}", offset);
                }

                navigations.push(nb.build());
                expr = subCollectionExpression.getCollectionExpression();
            } else if (expr instanceof ContainerExpression) {
                final ContainerExpression containerExpression = (ContainerExpression) expr;
                navigations.push(Navigation.builder().container((EClass) modelAdapter.get(containerExpression.getElementName()).get()).build());
                expr = containerExpression.getObjectExpression();
            } else if (expr instanceof CastObject) {
                final CastObject castObject = (CastObject) expr;
                navigations.push(Navigation.builder().cast((EClass) modelAdapter.get(castObject.getElementName()).get()).build());
                expr = castObject.getObjectExpression();
            } else if (expr instanceof CastCollection) {
                final CastCollection castCollection = (CastCollection) expr;
                navigations.push(Navigation.builder().cast((EClass) modelAdapter.get(castCollection.getElementName()).get()).build());
                expr = castCollection.getCollectionExpression();
            } else {
                throw new UnsupportedOperationException("Not supported yet");
            }
            ignoreOrderBy = true;
        }

        return base;
    }

    @lombok.Getter
    @lombok.Builder
    @ToString
    private static class Navigation {

        private EClass container;

        private EClass cast;

        private String featureName;

        private String variableName;

        private LogicalExpression condition;

        private List<OrderByItem> orderBy;

        private Integer limit;

        private Integer offset;

        private ObjectSelectorExpression objectSelector;
    }

    protected abstract Feature expressionToFeature(Expression expression, Context context, FeatureTargetMapping targetMapping);

    @lombok.Getter
    @lombok.Builder
    public static class PathEnds {

        private final Node base;

        private final Node partner;

        private final Integer limit;

        private final Integer offset;

        private final boolean ordered;
    }
}
