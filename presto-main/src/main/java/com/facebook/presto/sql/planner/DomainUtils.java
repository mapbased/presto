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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Domain;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public final class DomainUtils
{
    private DomainUtils()
    {
    }

    public static Map<ColumnHandle, Domain<?>> symbolToColumnHandle(Map<Symbol, Domain<?>> symbolDomainMap, Map<Symbol, ColumnHandle> assignments)
    {
        Preconditions.checkArgument(assignments.keySet().containsAll(symbolDomainMap.keySet()), "assignments does not contain all required symbols");
        ImmutableMap.Builder<ColumnHandle, Domain<?>> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, Domain<?>> entry : symbolDomainMap.entrySet()) {
            builder.put(assignments.get(entry.getKey()), entry.getValue());
        }
        return builder.build();
    }

    public static Map<Symbol, Domain<?>> columnHandleToSymbol(Map<ColumnHandle, Domain<?>> columnHandleDomainMap, Map<Symbol, ColumnHandle> assignments)
    {
        Map<ColumnHandle, Symbol> inverseAssignments = ImmutableBiMap.copyOf(assignments).inverse();
        Preconditions.checkArgument(inverseAssignments.keySet().containsAll(columnHandleDomainMap.keySet()), "assignments does not contain all required column handles");
        ImmutableMap.Builder<Symbol, Domain<?>> builder = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Domain<?>> entry : columnHandleDomainMap.entrySet()) {
            builder.put(inverseAssignments.get(entry.getKey()), entry.getValue());
        }
        return builder.build();
    }
}
