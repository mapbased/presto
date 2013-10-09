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
package com.facebook.presto.spi;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of fetching Partitions in the ConnectorSplitManager interface.
 *
 * Results are comprised of two parts:
 * 1) The actual partitions
 * 2) The domain map representing the values that the connector was not able to pre-evaluate
 * when generating the partitions and will need to be double-checked by the final execution plan.
 */
public class PartitionResult
{
    private final List<Partition> partitions;
    private final Map<ColumnHandle, Domain<?>> undeterminedDomains;

    public PartitionResult(List<Partition> partitions, Map<ColumnHandle, Domain<?>> undeterminedDomains)
    {
        Objects.requireNonNull(partitions, "partitions is null");
        Objects.requireNonNull(undeterminedDomains, "undeterminedDomains is null");
        this.partitions = partitions;
        this.undeterminedDomains = undeterminedDomains;
    }

    public List<Partition> getPartitions()
    {
        return partitions;
    }

    public Map<ColumnHandle, Domain<?>> getUndeterminedDomains()
    {
        return undeterminedDomains;
    }
}
