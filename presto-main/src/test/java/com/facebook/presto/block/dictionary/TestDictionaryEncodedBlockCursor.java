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
package com.facebook.presto.block.dictionary;

import com.facebook.presto.block.AbstractTestBlockCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockCursor;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestDictionaryEncodedBlockCursor
        extends AbstractTestBlockCursor
{
    @Override
    protected Block createExpectedValues()
    {
        return createStringsBlock("apple", "apple", "apple", "banana", "banana", "banana", "banana", "banana", "cherry", "cherry", "date");
    }

    @Override
    protected BlockCursor createTestCursor()
    {
        Block dictionary = VARCHAR.createBlockBuilder(new BlockBuilderStatus())
                .appendSlice(Slices.utf8Slice("apple"))
                .appendSlice(Slices.utf8Slice("banana"))
                .appendSlice(Slices.utf8Slice("cherry"))
                .appendSlice(Slices.utf8Slice("date"))
                .build()
                .toRandomAccessBlock();

        return new DictionaryEncodedBlock(dictionary, createLongsBlock(0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 3)).cursor();
    }

    @Test
    public void testCursorType()
    {
        assertInstanceOf(createTestCursor(), DictionaryEncodedBlockCursor.class);
    }
}
