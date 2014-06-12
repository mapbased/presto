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
package com.facebook.presto.serde;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertTrue;

public class TestSnappyBlockSerde
{
    @Test
    public void testRoundTrip()
    {
        Block block = VARCHAR.createBlockBuilder(new BlockBuilderStatus())
                .appendSlice(Slices.utf8Slice("alice"))
                .appendSlice(Slices.utf8Slice("bob"))
                .appendSlice(Slices.utf8Slice("charlie"))
                .appendSlice(Slices.utf8Slice("dave"))
                .build();

        DynamicSliceOutput compressedOutput = new DynamicSliceOutput(1024);
        Encoder encoder = BlocksFileEncoding.SNAPPY.createBlocksWriter(compressedOutput);

        encoder.append(block);
        BlockEncoding snappyEncoding = encoder.finish();
        Block actualBlock = snappyEncoding.readBlock(compressedOutput.slice().getInput());
        BlockAssertions.assertBlockEquals(actualBlock, block);
    }

    @Test
    public void testLotsOfStuff()
    {
        Block block = VARCHAR.createBlockBuilder(new BlockBuilderStatus())
                .appendSlice(Slices.utf8Slice("alice"))
                .appendSlice(Slices.utf8Slice("bob"))
                .appendSlice(Slices.utf8Slice("charlie"))
                .appendSlice(Slices.utf8Slice("dave"))
                .build()
                .toRandomAccessBlock();

        DynamicSliceOutput encoderOutput = new DynamicSliceOutput(1024);
        Encoder encoder = BlocksFileEncoding.SNAPPY.createBlocksWriter(encoderOutput);

        BlockBuilder expectedBlockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());

        int count = 1000;
        for (int i = 0; i < count; i++) {
            // select a random position
            int position = ThreadLocalRandom.current().nextInt(block.getPositionCount());

            // add to expected block
            block.appendTo(position, expectedBlockBuilder);

            // create block with single value and add to encoder
            encoder.append(block.getSingleValueBlock(position));
        }

        Block expectedBlock = expectedBlockBuilder.build();

        BlockEncoding snappyEncoding = encoder.finish();
        assertTrue(encoderOutput.size() < expectedBlock.getSizeInBytes());

        Block actualBlock = snappyEncoding.readBlock(encoderOutput.slice().getInput());

        BlockAssertions.assertBlockEquals(actualBlock, expectedBlock);
    }
}
