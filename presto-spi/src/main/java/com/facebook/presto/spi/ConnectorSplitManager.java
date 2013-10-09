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

public interface ConnectorSplitManager
{
    /**
     * Get the unique id of this connector.
     */
    String getConnectorId();

    /**
     * Returns true only if this ConnectorSplitManager can operate on the TableHandle
     */
    boolean canHandle(TableHandle handle);

    /**
     * Gets the Partitions for the specified table.
     *
     * The domainMap indicates the execution filters that will be directly applied to the
     * data stream produced by this connector. In the domainMap, a Domain mapped to a column
     * indicates all possible values that will be needed during execution, allowing the connector
     * to do some early filtering when generating/producing Partitions. A column that is not
     * mentioned in the domainMap implies that no constraints exist for that column.
     * Entries in the domainMap that were able to be pre-evaluated when generating the Partitions
     * should be stripped out of the domainMap; those that could not be pre-evaluated should
     * be reported in the undeterminedDomains of the returned PartitionResult.
     */
    PartitionResult getPartitions(TableHandle table, Map<ColumnHandle, Domain<?>> domainMap);

    /**
     * Gets the Splits for the specified Partitions in the indicated table.
     */
    Iterable<Split> getPartitionSplits(TableHandle table, List<Partition> partitions);
}
