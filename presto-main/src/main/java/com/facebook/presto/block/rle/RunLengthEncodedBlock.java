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
package com.facebook.presto.block.rle;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;

public class RunLengthEncodedBlock
        implements Block
{
    private final Block value;
    private final int positionCount;

    public RunLengthEncodedBlock(Block value, int positionCount)
    {
        this.value = checkNotNull(value, "value is null");
        checkArgument(value.getPositionCount() == 1, "Expected value to contain a single position but has %s positions", value.getPositionCount());

        // value can not be a RunLengthEncodedBlock because this could cause stack overflow in some of the methods
        checkArgument(!(value instanceof RunLengthEncodedBlock), "Value can not be an instance of a %s", getClass().getName());

        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = checkNotNull(positionCount, "positionCount is null");
    }

    public Block getValue()
    {
        return value;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSizeInBytes()
    {
        return value.getSizeInBytes();
    }

    @Override
    public RunLengthBlockEncoding getEncoding()
    {
        return new RunLengthBlockEncoding(value.getEncoding());
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkPositionIndexes(positionOffset, positionOffset + length, positionCount);
        return new RunLengthEncodedBlock(value, length);
    }

    @Override
    public Block toRandomAccessBlock()
    {
        return this;
    }

    @Override
    public Type getType()
    {
        return value.getType();
    }

    @Override
    public boolean getBoolean(int position)
    {
        checkReadablePosition(position);
        return value.getBoolean(0);
    }

    @Override
    public long getLong(int position)
    {
        checkReadablePosition(position);
        return value.getLong(0);
    }

    @Override
    public double getDouble(int position)
    {
        checkReadablePosition(position);
        return value.getDouble(0);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, int position)
    {
        checkReadablePosition(position);
        return value.getObjectValue(session, 0);
    }

    @Override
    public Slice getSlice(int position)
    {
        checkReadablePosition(position);
        return value.getSlice(0);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return value;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return value.isNull(0);
    }

    @Override
    public boolean equalTo(int position, Block otherBlock, int otherPosition)
    {
        checkReadablePosition(position);
        return value.equalTo(0, otherBlock, otherPosition);
    }

    @Override
    public boolean equalTo(int position, BlockCursor cursor)
    {
        checkReadablePosition(position);
        return this.value.equalTo(0, cursor);
    }

    @Override
    public boolean equalTo(int position, Slice otherSlice, int otherOffset)
    {
        checkReadablePosition(position);
        return value.equalTo(0, otherSlice, otherOffset);
    }

    @Override
    public int hash(int position)
    {
        checkReadablePosition(position);
        return value.hash(0);
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, Block otherBlock, int otherPosition)
    {
        checkReadablePosition(position);
        return value.compareTo(sortOrder, 0, otherBlock, otherPosition);
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, BlockCursor cursor)
    {
        checkReadablePosition(position);
        return value.compareTo(sortOrder, 0, cursor);
    }

    @Override
    public int compareTo(int position, Slice otherSlice, int otherOffset)
    {
        checkReadablePosition(position);
        return value.compareTo(0, otherSlice, otherOffset);
    }

    @Override
    public void appendTo(int position, BlockBuilder blockBuilder)
    {
        value.appendTo(0, blockBuilder);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("value", value)
                .add("positionCount", positionCount)
                .toString();
    }

    @Override
    public RunLengthEncodedBlockCursor cursor()
    {
        return new RunLengthEncodedBlockCursor(value, positionCount);
    }

    private void checkReadablePosition(int position)
    {
        checkState(position >= 0 && position < positionCount, "position is not valid");
    }
}
