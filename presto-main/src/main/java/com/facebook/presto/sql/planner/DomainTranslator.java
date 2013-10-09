/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Domains;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.math.DoubleMath;
import io.airlift.slice.Slice;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.combineDisjunctsWithDefault;
import static com.facebook.presto.sql.planner.LiteralInterpreter.toExpression;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;

public final class DomainTranslator
{
    private DomainTranslator()
    {
    }

    /**
     * Convert a DomainMap into an Expression predicate
     */
    public static Expression toPredicate(Map<Symbol, Domain<?>> domainMap)
    {
        ImmutableList.Builder<Expression> conjunctBuilder = ImmutableList.builder();
        for (Map.Entry<Symbol, Domain<?>> entry : domainMap.entrySet()) {
            QualifiedNameReference reference = new QualifiedNameReference(entry.getKey().toQualifiedName());
            conjunctBuilder.add(domainToPredicate(reference, entry.getValue()));
        }
        return combineConjuncts(conjunctBuilder.build());
    }

    private static Expression domainToPredicate(QualifiedNameReference reference, Domain<?> domain)
    {
        if (domain.getRanges().isEmpty()) {
            return domain.isNullAllowed() ? new IsNullPredicate(reference) : BooleanLiteral.FALSE_LITERAL;
        }

        Range<? extends Comparable<?>> firstRange = domain.getRanges().iterator().next();
        if (firstRange.isAll()) {
            return domain.isNullAllowed() ? TRUE_LITERAL : new IsNotNullPredicate(reference);
        }

        // Add disjuncts for ranges
        List<Expression> disjuncts = new ArrayList<>();
        List<Expression> singleValues = new ArrayList<>();
        for (Range<? extends Comparable<?>> range : domain.getRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(toExpression(range.getLow().getValue()));
            }
            else {
                List<Expression> rangeConjuncts = new ArrayList<>();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.add(new ComparisonExpression(ComparisonExpression.Type.GREATER_THAN, reference, toExpression(range.getLow().getValue())));
                            break;
                        case EXACTLY:
                            rangeConjuncts.add(new ComparisonExpression(ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, reference, toExpression(range.getLow().getValue())));
                            break;
                        case BELOW:
                            throw new IllegalStateException("Low Marker should never use BELOW bound: " + range);
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalStateException("High Marker should never use ABOVE bound: " + range);
                        case EXACTLY:
                            rangeConjuncts.add(new ComparisonExpression(ComparisonExpression.Type.LESS_THAN_OR_EQUAL, reference, toExpression(range.getHigh().getValue())));
                            break;
                        case BELOW:
                            rangeConjuncts.add(new ComparisonExpression(ComparisonExpression.Type.LESS_THAN, reference, toExpression(range.getHigh().getValue())));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add(combineConjuncts(rangeConjuncts));
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(new ComparisonExpression(ComparisonExpression.Type.EQUAL, reference, getOnlyElement(singleValues)));
        }
        else if (singleValues.size() > 1) {
            disjuncts.add(new InPredicate(reference, new InListExpression(singleValues)));
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(new IsNullPredicate(reference));
        }
        return combineDisjunctsWithDefault(disjuncts, TRUE_LITERAL);
    }

    /**
     * Convert an Expression predicate into an ExtractionResult consisting of:
     * 1) A successfully extracted domainMap (NOTE: this may contain symbols beyond those in the Expression)
     * 2) An Expression fragment which represents the part of the original Expression that will need to be re-evaluated
     * after filtering with the domainMap.
     */
    public static ExtractionResult fromPredicate(Expression predicate, Map<Symbol, Type> types)
    {
        return new Visitor(types).process(predicate, null);
    }

    private static class Visitor
            extends AstVisitor<ExtractionResult, Void>
    {
        private final Map<Symbol, Type> types;

        private Visitor(Map<Symbol, Type> types)
        {
            this.types = ImmutableMap.copyOf(checkNotNull(types, "types is null"));
        }

        private ColumnType checkedTypeLookup(Symbol symbol)
        {
            Type type = types.get(symbol);
            checkState(type != null, "Types is missing info for symbol: " + symbol);
            assert type != null;
            return type.getColumnType();
        }

        private Map<Symbol, Domain<?>> noneDomainMap() {
            return ImmutableMap.copyOf(Maps.transformValues(types, new Function<Type, Domain<?>>()
            {
                @Override
                public Domain<?> apply(Type type)
                {
                    return (Domain<?>) Domain.none();
                }
            }));
        }

        @Override
        protected ExtractionResult visitExpression(Expression node, Void context)
        {
            return new ExtractionResult(ImmutableMap.<Symbol, Domain<?>>of(), node);
        }

        @Override
        protected ExtractionResult visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            ExtractionResult leftResult = process(node.getLeft(), context);
            ExtractionResult rightResult = process(node.getRight(), context);

            switch (node.getType()) {
                case AND:
                    return new ExtractionResult(
                            Domains.intersectDomainMaps(leftResult.getDomainMap(), rightResult.getDomainMap()),
                            combineConjuncts(leftResult.getRemainingExpression(), rightResult.getRemainingExpression()));

                case OR:
                    Map<Symbol, Domain<?>> unionedDomainMap = Domains.unionDomainMaps(leftResult.getDomainMap(), rightResult.getDomainMap());

                    // In most cases, the unionedDomainMap is not able to completely reflect all of the constraints of the current Expression node,
                    // and so we can return the current node as the remainingExpression so that all bounds will be double checked again at run time.
                    // However, there are a few cases that we can optimize where we can simplify the remainingExpression.
                    Expression remainingExpression = node;
                    // Since we generally aren't able to process the remaining expressions at this point, we can only make inferences if the remaining expressions
                    // on both side are equal and deterministic
                    if (leftResult.getRemainingExpression().equals(rightResult.getRemainingExpression()) &&
                            DeterminismEvaluator.isDeterministic(leftResult.getRemainingExpression())) {
                        // We can reduce the remainingExpression if there is either only a single same element in both the left and right domainMaps,
                        // or if one domainMap completely encompasses another.
                        boolean matchingSingleSymbolDomains = leftResult.getDomainMap().size() == 1 &&
                                rightResult.getDomainMap().size() == 1 &&
                                getOnlyElement(leftResult.getDomainMap().keySet()).equals(getOnlyElement(rightResult.getDomainMap().keySet()));
                        boolean oneSideIsSuperSet = unionedDomainMap.equals(leftResult.getDomainMap()) || unionedDomainMap.equals(rightResult.getDomainMap());

                        if (matchingSingleSymbolDomains || oneSideIsSuperSet) {
                            remainingExpression = leftResult.getRemainingExpression();
                        }
                    }

                    return new ExtractionResult(unionedDomainMap, remainingExpression);

                default:
                    throw new AssertionError("Unknown type: " + node.getType());
            }
        }

        @Override
        protected ExtractionResult visitNotExpression(NotExpression node, Void context)
        {
            ExtractionResult result = process(node.getValue(), context);

            if (result.getDomainMap().isEmpty()) {
                return new ExtractionResult(ImmutableMap.<Symbol, Domain<?>>of(), new NotExpression(result.getRemainingExpression()));
            }

            // The only meaningful extraction with a NOT is if there is only one result Symbol in the underlying domain and no remainingExpressions
            if (result.getDomainMap().size() == 1 && result.getRemainingExpression().equals(TRUE_LITERAL)) {
                Map.Entry<Symbol, Domain<?>> entry = Iterables.getOnlyElement(result.getDomainMap().entrySet());
                return new ExtractionResult(ImmutableMap.<Symbol, Domain<?>>of(entry.getKey(), entry.getValue().complement()), TRUE_LITERAL);
            }

            return new ExtractionResult(ImmutableMap.<Symbol, Domain<?>>of(), node);
        }

        @Override
        protected ExtractionResult visitComparisonExpression(ComparisonExpression node, Void context)
        {
            if (!isSimpleComparison(node)) {
                return super.visitComparisonExpression(node, context);
            }
            node = normalizeSimpleComparison(node);

            Symbol symbol = Symbol.fromQualifiedName(((QualifiedNameReference) node.getLeft()).getName());
            ColumnType columnType = checkedTypeLookup(symbol);
            Object value = LiteralInterpreter.evaluate(node.getRight());

            // Handle the cases where implicit coercions can happen in comparisons
            // TODO: how to abstract this out
            if (value instanceof Double && columnType == ColumnType.LONG) {
                return process(coerceDoubleToLongComparison(node), context);
            }
            if (value instanceof Long && columnType == ColumnType.DOUBLE) {
                value = ((Long) value).doubleValue();
            }
            if (value instanceof Slice) {
                value = ((Slice) value).toStringUtf8();
            }
            checkState(valueMatchesType(value, columnType), "Object value '%s' does not match type '%'", value, columnType);
            checkState(value == null || value instanceof Comparable, "Expected value to be null or of comparable type: " + value);

            return createComparisonExtractionResult(node.getType(), symbol, castToComparable(value));
        }

        private <T extends Comparable<T>> ExtractionResult createComparisonExtractionResult(ComparisonExpression.Type type, Symbol symbol, T value)
        {
            if (value == null) {
                switch (type) {
                    case EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case NOT_EQUAL:
                        return new ExtractionResult(noneDomainMap(), TRUE_LITERAL);

                    case IS_DISTINCT_FROM:
                        return new ExtractionResult(ImmutableMap.<Symbol, Domain<?>>of(symbol, Domain.create(SortedRangeSet.of(Range.all()), false)), TRUE_LITERAL);

                    default:
                        throw new AssertionError("Unhandled type: " + type);
                }
            }

            Domain<?> domain;
            switch (type) {
                case EQUAL:
                    domain = Domain.create(SortedRangeSet.of(Range.equal(value)), false);
                    break;
                case GREATER_THAN:
                    domain = Domain.create(SortedRangeSet.of(Range.greaterThan(value)), false);
                    break;
                case GREATER_THAN_OR_EQUAL:
                    domain = Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(value)), false);
                    break;
                case LESS_THAN:
                    domain = Domain.create(SortedRangeSet.of(Range.lessThan(value)), false);
                    break;
                case LESS_THAN_OR_EQUAL:
                    domain = Domain.create(SortedRangeSet.of(Range.lessThanOrEqual(value)), false);
                    break;
                case NOT_EQUAL:
                    domain = Domain.create(SortedRangeSet.of(Range.lessThan(value), Range.greaterThan(value)), false);
                    break;
                case IS_DISTINCT_FROM:
                    domain = Domain.create(SortedRangeSet.of(Range.lessThan(value), Range.greaterThan(value)), true);
                    break;
                default:
                    throw new AssertionError("Unhandled type: " + type);
            }
            return new ExtractionResult(ImmutableMap.<Symbol, Domain<?>>of(symbol, domain), TRUE_LITERAL);
        }

        @Override
        protected ExtractionResult visitInPredicate(InPredicate node, Void context)
        {
            if (node.getValue() instanceof QualifiedNameReference && node.getValueList() instanceof InListExpression) {
                Symbol symbol = Symbol.fromQualifiedName(((QualifiedNameReference) node.getValue()).getName());
                ColumnType columnType = checkedTypeLookup(symbol);

                SortedRangeSet.Builder builder = SortedRangeSet.builder();
                InListExpression valueList = (InListExpression) node.getValueList();
                for (Expression listValue : valueList.getValues()) {
                    if (!(listValue instanceof Literal)) {
                        // Do not translate if any values are non-literal
                        // TODO: considering ORing the non-literal values with the constructed domains
                        return super.visitInPredicate(node, context);
                    }
                    Object rawValue = LiteralInterpreter.evaluate(listValue);
                    // Ignore any NULLs in the value list as those can never produce true results
                    if (rawValue != null) {
                        if (rawValue instanceof Double && columnType == ColumnType.LONG) {
                            Long roundedLong = DoubleMath.roundToLong((Double) rawValue, RoundingMode.FLOOR);
                            if (roundedLong.doubleValue() != rawValue) {
                                // Coercion to long changed the double value, and so no match is possible
                                continue;
                            }
                            rawValue = roundedLong;
                        }
                        else if (rawValue instanceof Long && columnType == ColumnType.DOUBLE) {
                            rawValue = ((Long) rawValue).doubleValue();
                        }
                        else if (rawValue instanceof Slice) {
                            rawValue = ((Slice) rawValue).toStringUtf8();
                        }
                        checkState(valueMatchesType(rawValue, columnType), "Object value '%s' does not match type '%'", rawValue, columnType);
                        checkState(rawValue instanceof Comparable, "Expected value to be null or of comparable type: " + rawValue);

                        builder.add(Range.equal(castToComparable(rawValue)));
                    }
                }
                return new ExtractionResult(ImmutableMap.<Symbol, Domain<?>>of(symbol, Domain.create(builder.build(), false)), TRUE_LITERAL);
            }
            return super.visitInPredicate(node, context);
        }

        @Override
        protected ExtractionResult visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            // Re-write as two comparison expressions
            return process(ExpressionUtils.and(
                    new ComparisonExpression(ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, node.getValue(), node.getMin()),
                    new ComparisonExpression(ComparisonExpression.Type.LESS_THAN_OR_EQUAL, node.getValue(), node.getMax())), context);
        }

        @Override
        protected ExtractionResult visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            if (node.getValue() instanceof QualifiedNameReference) {
                Symbol symbol = Symbol.fromQualifiedName(((QualifiedNameReference) node.getValue()).getName());
                return new ExtractionResult(ImmutableMap.<Symbol, Domain<?>>of(symbol, Domain.nullOnly()), TRUE_LITERAL);
            }
            return super.visitIsNullPredicate(node, context);
        }

        @Override
        protected ExtractionResult visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            if (node.getValue() instanceof QualifiedNameReference) {
                Symbol symbol = Symbol.fromQualifiedName(((QualifiedNameReference) node.getValue()).getName());
                return new ExtractionResult(ImmutableMap.<Symbol, Domain<?>>of(symbol,
                        Domain.create(SortedRangeSet.of(Range.all()), false)),
                        TRUE_LITERAL);
            }
            return super.visitIsNotNullPredicate(node, context);
        }

        @Override
        protected ExtractionResult visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            // TODO: test these with AND and OR
            return new ExtractionResult(node.getValue() ? ImmutableMap.<Symbol, Domain<?>>of() : noneDomainMap(), TRUE_LITERAL);
        }

        @Override
        protected ExtractionResult visitNullLiteral(NullLiteral node, Void context)
        {
            // TODO: test these with AND and OR
            return new ExtractionResult(noneDomainMap(), TRUE_LITERAL);
        }
    }

    @SuppressWarnings("rawtypes")
    private static <X> Comparable castToComparable(X value)
    {
        return (Comparable) value;
    }

    public static boolean valueMatchesType(Object value, ColumnType type)
    {
        if (value == null) {
            return true;
        }
        switch (type) {
            case BOOLEAN:
                return value instanceof Boolean;
            case DOUBLE:
                return value instanceof Double;
            case LONG:
                return value instanceof Long;
            case STRING:
                return value instanceof String;
            default:
                throw new AssertionError("Unhandled type: " + type);
        }
    }

    public static boolean isSimpleComparison(ComparisonExpression comparison)
    {
        return (comparison.getLeft() instanceof QualifiedNameReference && comparison.getRight() instanceof Literal) ||
                (comparison.getLeft() instanceof Literal && comparison.getRight() instanceof QualifiedNameReference);
    }

    /**
     * Normalize a simple comparison between a QualifiedNameReference and a Literal such that the QualifiedNameReference will always be on the left and the Literal on the right.
     */
    public static ComparisonExpression normalizeSimpleComparison(ComparisonExpression comparison)
    {
        if (comparison.getLeft() instanceof QualifiedNameReference && comparison.getRight() instanceof Literal) {
            return comparison;
        }
        else if (comparison.getLeft() instanceof Literal && comparison.getRight() instanceof QualifiedNameReference) {
            return new ComparisonExpression(flipComparisonDirection(comparison.getType()), comparison.getRight(), comparison.getLeft());
        }
        else {
            throw new IllegalArgumentException("ComparisonExpression not a simple literal comparison: " + comparison);
        }
    }

    public static ComparisonExpression.Type flipComparisonDirection(ComparisonExpression.Type type)
    {
        switch (type) {
            case LESS_THAN_OR_EQUAL:
                return ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
            case LESS_THAN:
                return ComparisonExpression.Type.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL:
                return ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
            case GREATER_THAN:
                return ComparisonExpression.Type.LESS_THAN;
            default:
                // The remaining types have no direction association
                return type;
        }
    }

    public static Expression coerceDoubleToLongComparison(ComparisonExpression comparison)
    {
        comparison = normalizeSimpleComparison(comparison);

        checkArgument(comparison.getLeft() instanceof QualifiedNameReference, "Left must be a QualifiedNameReference");
        checkArgument(comparison.getRight() instanceof DoubleLiteral, "Right must be a DoubleLiteral");

        QualifiedNameReference reference = (QualifiedNameReference) comparison.getLeft();
        Double value = ((DoubleLiteral) comparison.getRight()).getValue();

        switch (comparison.getType()) {
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
                return new ComparisonExpression(comparison.getType(), reference, toExpression(DoubleMath.roundToLong(value, RoundingMode.CEILING)));

            case GREATER_THAN:
            case LESS_THAN_OR_EQUAL:
                return new ComparisonExpression(comparison.getType(), reference, toExpression(DoubleMath.roundToLong(value, RoundingMode.FLOOR)));

            case EQUAL:
                Long equalValue = DoubleMath.roundToLong(value, RoundingMode.FLOOR);
                if (equalValue.doubleValue() != value) {
                    return BooleanLiteral.FALSE_LITERAL;
                }
                return new ComparisonExpression(comparison.getType(), reference, toExpression(equalValue));

            case NOT_EQUAL:
                Long notEqualValue = DoubleMath.roundToLong(value, RoundingMode.FLOOR);
                if (notEqualValue.doubleValue() != value) {
                    return TRUE_LITERAL;
                }
                return new ComparisonExpression(comparison.getType(), reference, toExpression(notEqualValue));

            case IS_DISTINCT_FROM:
                Long distinctValue = DoubleMath.roundToLong(value, RoundingMode.FLOOR);
                if (distinctValue.doubleValue() != value) {
                    return TRUE_LITERAL;
                }
                return new ComparisonExpression(comparison.getType(), reference, toExpression(distinctValue));

            default:
                throw new AssertionError("Unhandled type: " + comparison.getType());
        }
    }

    public static class ExtractionResult
    {
        private final Map<Symbol, Domain<?>> domainMap;
        private final Expression remainingExpression;

        public ExtractionResult(Map<Symbol, Domain<?>> domainMap, Expression remainingExpression)
        {
            this.domainMap = ImmutableMap.copyOf(checkNotNull(domainMap, "domainMap is null"));
            this.remainingExpression = checkNotNull(remainingExpression, "remainingExpression is null");
        }

        public Map<Symbol, Domain<?>> getDomainMap()
        {
            return domainMap;
        }

        public Expression getRemainingExpression()
        {
            return remainingExpression;
        }
    }
}
