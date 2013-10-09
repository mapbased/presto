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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Domains;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.DomainUtils;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class TableScanNode
        extends PlanNode
{
    private final TableHandle table;
    private final List<Symbol> outputSymbols;
    private final Map<Symbol, ColumnHandle> assignments; // symbol -> column
    private final Optional<List<Partition>> partitions;
    private final boolean partitionsDroppedBySerialization;

    public TableScanNode(PlanNodeId id,  TableHandle table, List<Symbol> outputSymbols, Map<Symbol, ColumnHandle> assignments, Optional<List<Partition>> partitions)
    {
        this(id, table, outputSymbols, assignments, partitions, false);
    }

    @JsonCreator
    public TableScanNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("table") TableHandle table,
            @JsonProperty("outputSymbols") List<Symbol> outputSymbols,
            @JsonProperty("assignments") Map<Symbol, ColumnHandle> assignments)
    {
        this(id, table, outputSymbols, assignments, Optional.<List<Partition>>absent(), true);
    }

    private TableScanNode(PlanNodeId id, TableHandle table, List<Symbol> outputSymbols, Map<Symbol, ColumnHandle> assignments, Optional<List<Partition>> partitions, boolean partitionsDroppedBySerialization)
    {
        super(id);

        checkNotNull(table, "table is null");
        checkNotNull(outputSymbols, "outputSymbols is null");
        checkNotNull(assignments, "assignments is null");
        checkArgument(assignments.keySet().containsAll(outputSymbols), "assignments does not cover all of outputSymbols");
        checkArgument(!assignments.isEmpty(), "assignments is empty");
        checkNotNull(partitions, "partitions is null");

        this.table = table;
        this.outputSymbols = ImmutableList.copyOf(outputSymbols);
        this.assignments = ImmutableMap.copyOf(assignments);
        this.partitions = partitions.isPresent() ? Optional.<List<Partition>>of(ImmutableList.copyOf(partitions.get())) : partitions;
        this.partitionsDroppedBySerialization = partitionsDroppedBySerialization;

        // TODO: can we make this assumption?
        checkArgument(assignments.values().containsAll(getPartitionsDomainSummary().keySet()), "assignments do not include all of the ColumnHandles specified by the Partitions");
    }

    @JsonProperty("table")
    public TableHandle getTable()
    {
        return table;
    }

    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty("assignments")
    public Map<Symbol, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    public Optional<List<Partition>> getPartitions()
    {
        if (partitionsDroppedBySerialization) {
            // If this exception throws, then we might want to consider making Partitions serializable by Jackson
            throw new IllegalStateException("Can't access partitions after passing through serialization");
        }
        return partitions;
    }

    public Map<ColumnHandle, Domain<?>> getPartitionsDomainSummary()
    {
        if (!partitions.isPresent()) {
            return ImmutableMap.of();
        }

        Map<ColumnHandle, Domain<?>> domainMap = null;
        for (Partition partition : partitions.get()) {
            domainMap = (domainMap == null) ? partition.getDomainMap() : Domains.unionDomainMaps(domainMap, partition.getDomainMap());
        }

        return Objects.firstNonNull(domainMap, ImmutableMap.<ColumnHandle, Domain<?>>of());
    }

    public Expression getPartitionsPredicateSummary()
    {
        if (!partitions.isPresent()) {
            return BooleanLiteral.TRUE_LITERAL;
        }

        if (partitions.get().isEmpty()) {
            return BooleanLiteral.FALSE_LITERAL;
        }

        // Generate the predicate representing the domain of all partitions
        ImmutableList.Builder<Expression> disjuncts = ImmutableList.builder();
        for (Partition partition : partitions.get()) {
            Expression predicate = DomainTranslator.toPredicate(DomainUtils.columnHandleToSymbol(partition.getDomainMap(), assignments));
            disjuncts.add(predicate);
        }
        return ExpressionUtils.combineDisjunctsWithDefault(disjuncts.build(), BooleanLiteral.TRUE_LITERAL);
    }

    @JsonProperty("partitionDomainSummary")
    public String getPrintablePartitionDomainSummary()
    {
        // Since partitions are not serializable, we can provide an additional jackson field purely for information purposes (i.e. for logging)
        // If partitions ever become serializable, we can get rid of this method
        return DomainUtils.columnHandleToSymbol(getPartitionsDomainSummary(), assignments).toString();
    }

    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitTableScan(this, context);
    }
}
