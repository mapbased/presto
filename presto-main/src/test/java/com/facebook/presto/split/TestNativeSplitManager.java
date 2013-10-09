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
package com.facebook.presto.split;

import com.facebook.presto.execution.DataSource;
import com.facebook.presto.metadata.DatabaseShardManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder;
import com.facebook.presto.metadata.NativeMetadata;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeVersion;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.testing.FileUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
import static org.testng.Assert.assertEquals;

public class TestNativeSplitManager
{
    private static final TableMetadata TEST_TABLE = TableMetadataBuilder.tableMetadataBuilder("demo", "test_table")
            .partitionKeyColumn("ds", STRING)
            .column("foo", STRING)
            .column("bar", LONG)
            .build();

    private Handle dummyHandle;
    private File dataDir;
    private NativeSplitManager nativeSplitManager;
    private SplitManager splitManager;
    private TableHandle tableHandle;
    private ColumnHandle dsColumnHandle;
    private ColumnHandle fooColumnHandle;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        dataDir = Files.createTempDir();
        ShardManager shardManager = new DatabaseShardManager(dbi);
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();

        String nodeName = UUID.randomUUID().toString();
        nodeManager.addNode("native", new Node(nodeName, new URI("http://127.0.0.1/"), NodeVersion.UNKNOWN));

        MetadataManager metadataManager = new MetadataManager();
        metadataManager.addConnectorMetadata("local", new NativeMetadata("native", dbi));

        tableHandle = metadataManager.createTable("local", TEST_TABLE);
        dsColumnHandle = metadataManager.getColumnHandle(tableHandle, "ds").get();
        fooColumnHandle = metadataManager.getColumnHandle(tableHandle, "foo").get();

        long shardId1 = shardManager.allocateShard(tableHandle);
        long shardId2 = shardManager.allocateShard(tableHandle);
        long shardId3 = shardManager.allocateShard(tableHandle);
        long shardId4 = shardManager.allocateShard(tableHandle);

        shardManager.commitPartition(tableHandle, "ds=1", ImmutableList.<PartitionKey>of(new NativePartitionKey("ds=1", "ds", ColumnType.STRING, "1")), ImmutableMap.of(shardId1, nodeName,
                shardId2, nodeName,
                shardId3, nodeName));
        shardManager.commitPartition(tableHandle, "ds=2", ImmutableList.<PartitionKey>of(new NativePartitionKey("ds=2", "ds", ColumnType.STRING, "2")), ImmutableMap.of(shardId4, nodeName));

        nativeSplitManager = new NativeSplitManager(nodeManager, shardManager, metadataManager);
        splitManager = new SplitManager(ImmutableSet.<ConnectorSplitManager>of(nativeSplitManager));
    }

    @AfterMethod
    public void teardown()
    {
        dummyHandle.close();
        FileUtils.deleteRecursively(dataDir);
    }

    @Test
    public void testNoPruning()
    {
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, Optional.<Map<ColumnHandle, Domain<?>>>absent());
        assertEquals(partitionResult.getPartitions().size(), 2);

        DataSource dataSource = splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions());
        List<Split> splits = ImmutableList.copyOf(dataSource.getSplits());
        assertEquals(splits.size(), 4);
    }

    @Test
    public void testPruneNoMatch()
    {
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, Optional.<Map<ColumnHandle, Domain<?>>>of(ImmutableMap.<ColumnHandle, Domain<?>>of(dsColumnHandle,
                Domain.singleValue("foo"))));
        assertEquals(partitionResult.getPartitions().size(), 2);

        // ds=3. No partition will match this.
        List<Partition> partitions = splitManager.getPartitions(tableHandle, Optional.<Map<ColumnHandle, Domain<?>>>of(ImmutableMap.<ColumnHandle, Domain<?>>of(dsColumnHandle, Domain.singleValue("3")))).getPartitions();
        DataSource dataSource = splitManager.getPartitionSplits(tableHandle, partitions);
        List<Split> splits = ImmutableList.copyOf(dataSource.getSplits());
        // no splits found
        assertEquals(splits.size(), 0);
    }

    @Test
    public void testPruneMatch()
    {
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, Optional.<Map<ColumnHandle, Domain<?>>>of(ImmutableMap.<ColumnHandle, Domain<?>>of(dsColumnHandle, Domain.singleValue("1"))));
        assertEquals(partitionResult.getPartitions().size(), 2);

        // ds=1. One partition with three splits will match this.
        List<Partition> partitions = splitManager.getPartitions(tableHandle, Optional.<Map<ColumnHandle, Domain<?>>>of(ImmutableMap.<ColumnHandle, Domain<?>>of(dsColumnHandle, Domain.singleValue("1")))).getPartitions();
        DataSource dataSource = splitManager.getPartitionSplits(tableHandle, partitions);
        List<Split> splits = ImmutableList.copyOf(dataSource.getSplits());
        // three splits found
        assertEquals(splits.size(), 3);
    }

    @Test
    public void testNoPruneUnknown()
    {
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, Optional.<Map<ColumnHandle, Domain<?>>>of(ImmutableMap.<ColumnHandle, Domain<?>>of(dsColumnHandle, Domain.singleValue("foo"))));
        assertEquals(partitionResult.getPartitions().size(), 2);

        // foo=bar. Not a prunable column
        List<Partition> partitions = splitManager.getPartitions(tableHandle, Optional.<Map<ColumnHandle, Domain<?>>>of(ImmutableMap.<ColumnHandle, Domain<?>>of(fooColumnHandle, Domain.singleValue("bar")))).getPartitions();
        DataSource dataSource = splitManager.getPartitionSplits(tableHandle, partitions);
        List<Split> splits = ImmutableList.copyOf(dataSource.getSplits());
        // all splits found
        assertEquals(splits.size(), 4);
    }
}
