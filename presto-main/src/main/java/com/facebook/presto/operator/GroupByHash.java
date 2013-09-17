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
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.longs.LongHash.Strategy;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class GroupByHash
{
    private static final long CURRENT_ROW_ADDRESS = 0xFF_FF_FF_FF_FF_FF_FF_FFL;

    private final List<Type> types;
    private final int[] channels;

    private final List<PageBuilder> pages;

    private final PageBuilderHashStrategy hashStrategy;
    private final Long2IntOpenCustomHashMap pagePositionToGroupId;

    private int nextGroupId;

    public GroupByHash(List<Type> types, int[] channels, int expectedSize)
    {
        this.types = checkNotNull(types, "types is null");
        this.channels = channels.clone();

        this.pages = ObjectArrayList.wrap(new PageBuilder[1024], 0);
        this.pages.add(new PageBuilder(types));


        this.hashStrategy = new PageBuilderHashStrategy();
        this.pagePositionToGroupId = new Long2IntOpenCustomHashMap(expectedSize, hashStrategy);
        this.pagePositionToGroupId.defaultReturnValue(-1);
    }

    public long getEstimatedSize()
    {
        // todo
        return 0;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public Page group(Page input)
    {
        // handle empty pages directly since empty blocks are not allowed
        if (input.getPositionCount() == 0) {
            return new Page(input.getPositionCount(), new Block[input.getChannelCount() + 1]);
        }

        // generate the group id channel
        UncompressedBlock groupIds = getGroupIds(input.getBlocks());

        // create output page as copy of input page with one additional channel for the group id
        Block[] output = Arrays.copyOf(input.getBlocks(), input.getChannelCount() + 1);

        // group id channel is the last channel
        output[input.getChannelCount()] = groupIds;

        return new Page(input.getPositionCount(), output);
    }

    public UncompressedBlock getGroupIds(Block... blocks)
    {
        int positionCount = blocks[0].getPositionCount();

        int groupIdBlockSize = SINGLE_LONG.getFixedSize() * positionCount;
        BlockBuilder blockBuilder = new BlockBuilder(TupleInfo.SINGLE_LONG, groupIdBlockSize, Slices.allocate(groupIdBlockSize).getOutput());

        // open cursors for group blocks
        BlockCursor[] cursors = new BlockCursor[channels.length];
        for (int i = 0; i < channels.length; i++) {
            cursors[i] = blocks[channels[i]].cursor();
        }

        // use cursors in hash strategy to provide value for "current" row
        hashStrategy.setCurrentRow(cursors);

        for (int position = 0; position < positionCount; position++) {
            for (BlockCursor cursor : cursors) {
                checkState(cursor.advanceNextPosition());
            }

            int groupId = pagePositionToGroupId.get(CURRENT_ROW_ADDRESS);
            if (groupId < 0) {
                groupId = addNewGroup(cursors);
            }
            blockBuilder.append(groupId);
        }
        return blockBuilder.build();
    }

    private int addNewGroup(BlockCursor... row)
    {
        int pageIndex = pages.size() - 1;
        PageBuilder pageBuilder = pages.get(pageIndex);
        if (!pageBuilder.append(row)) {
            pageBuilder = new PageBuilder(types);
            pages.add(pageBuilder);
            pageIndex++;
            checkState(pageBuilder.append(row), "Could not add row to empty page builder");
        }

        // record group id in hash
        int groupId = nextGroupId++;
        long address = encodeSyntheticAddress(pageIndex, pageBuilder.getPositionCount() - 1);
        pagePositionToGroupId.put(address, groupId);

        return groupId;
    }

    public Long2IntOpenCustomHashMap getPagePositionToGroupId()
    {
        return pagePositionToGroupId;
    }

    public void getValues(long pagePosition, BlockBuilder[] builders)
    {
        PageBuilder page = pages.get(decodeSliceIndex(pagePosition));
        page.getValues(decodePosition(pagePosition), builders);
    }

    private class PageBuilderHashStrategy
            implements Strategy
    {
        private BlockCursor[] currentRow;

        public void setCurrentRow(BlockCursor[] currentRow)
        {
            this.currentRow = currentRow;
        }

        public void addPage(PageBuilder page)
        {
            pages.add(page);
        }

        @Override
        public int hashCode(long sliceAddress)
        {
            if (sliceAddress == CURRENT_ROW_ADDRESS) {
                return hashCurrentRow();
            }
            else {
                return hashPosition(sliceAddress);
            }
        }

        private int hashPosition(long sliceAddress)
        {
            int sliceIndex = decodeSliceIndex(sliceAddress);
            int position = decodePosition(sliceAddress);
            return pages.get(sliceIndex).hashCode(position);
        }

        private int hashCurrentRow()
        {
            int result = 0;
            for (int channel = 0; channel < types.size(); channel++) {
                Type type = types.get(channel);
                BlockCursor cursor = currentRow[channel];
                result = addToHashCode(result, valueHashCode(type, cursor.getRawSlice(), cursor.getRawOffset()));
            }
            return result;
        }

        @Override
        public boolean equals(long leftSliceAddress, long rightSliceAddress)
        {
            // current row always equals itself
            if (leftSliceAddress == CURRENT_ROW_ADDRESS && rightSliceAddress == CURRENT_ROW_ADDRESS) {
                return true;
            }

            // current row == position
            if (leftSliceAddress == CURRENT_ROW_ADDRESS) {
                return positionEqualsCurrentRow(decodeSliceIndex(rightSliceAddress), decodePosition(rightSliceAddress));
            }

            // position == current row
            if (rightSliceAddress == CURRENT_ROW_ADDRESS) {
                return positionEqualsCurrentRow(decodeSliceIndex(leftSliceAddress), decodePosition(leftSliceAddress));
            }

            // position == position
            return positionEqualsPosition(
                    decodeSliceIndex(leftSliceAddress), decodePosition(leftSliceAddress),
                    decodeSliceIndex(rightSliceAddress), decodePosition(rightSliceAddress));
        }

        private boolean positionEqualsCurrentRow(int sliceIndex, int position)
        {
            return pages.get(sliceIndex).equals(position, currentRow);
        }

        private boolean positionEqualsPosition(int leftSliceIndex, int leftPosition, int rightSliceIndex, int rightPosition)
        {
            return pages.get(leftSliceIndex).equals(leftPosition, pages.get(rightSliceIndex), rightPosition);
        }
    }

    private static class PageBuilder
    {
        private final List<ChannelBuilder> channels;
        private int positionCount;
        private boolean full;

        public PageBuilder(List<Type> types)
        {
            ImmutableList.Builder<ChannelBuilder> builder = ImmutableList.builder();
            for (Type type : types) {
                builder.add(new ChannelBuilder(type));
            }
            channels = builder.build();
        }

        public int getPositionCount()
        {
            return positionCount;
        }

        private boolean append(BlockCursor... row)
        {
            // don't add row if already full
            if (full) {
                return false;
            }

            // append to each channel
            for (int channel = 0; channel < row.length; channel++) {
                if (!channels.get(channel).append(row[channel])) {
                    full = true;
                    return false;
                }
            }
            positionCount++;
            return true;
        }

        public void getValues(int position, BlockBuilder[] builders)
        {
            for (int i = 0; i < channels.size(); i++) {
                ChannelBuilder channel = channels.get(i);
                channel.getValue(position, builders[i]);
            }
        }

        public int hashCode(int position)
        {
            int result = 0;
            for (ChannelBuilder channel : channels) {
                result = addToHashCode(result, channel.hashCode(position));
            }
            return result;
        }

        public boolean equals(int thisPosition, PageBuilder that, int thatPosition)
        {
            for (int i = 0; i < channels.size(); i++) {
                ChannelBuilder thisBlock = this.channels.get(i);
                ChannelBuilder thatBlock = that.channels.get(i);
                if (!thisBlock.equals(thisPosition, thatBlock, thatPosition)) {
                    return false;
                }
            }
            return true;
        }

        public boolean equals(int position, BlockCursor... row)
        {
            for (int i = 0; i < channels.size(); i++) {
                ChannelBuilder thisBlock = this.channels.get(i);
                if (!thisBlock.equals(position, row[i])) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class ChannelBuilder
    {
        public static final DataSize DEFAULT_MAX_BLOCK_SIZE = new DataSize(64, Unit.KILOBYTE);

        private final Type type;
        private final SliceOutput sliceOutput;
        private final Slice slice;
        private final IntArrayList positionOffsets;

        public ChannelBuilder(Type type)
        {
            checkNotNull(type, "type is null");

            this.type = type;
            this.slice = Slices.allocate(Ints.checkedCast(DEFAULT_MAX_BLOCK_SIZE.toBytes()));
            this.sliceOutput = slice.getOutput();
            this.positionOffsets = new IntArrayList(1024);
        }

        public boolean equals(int position, ChannelBuilder rightBuilder, int rightPosition)
        {
            Slice leftSlice = slice;
            checkArgument(position >= 0 && position < positionOffsets.size());
            int leftOffset = positionOffsets.getInt(position);

            Slice rightSlice = rightBuilder.slice;
            checkArgument(rightPosition >= 0 && rightPosition < rightBuilder.positionOffsets.size());
            int rightOffset = rightBuilder.positionOffsets.getInt(rightPosition);

            return valueEquals(type, leftSlice, leftOffset, rightSlice, rightOffset);
        }

        public boolean equals(int position, BlockCursor cursor)
        {
            checkArgument(position >= 0 && position < positionOffsets.size());

            int offset = positionOffsets.getInt(position);

            Slice rightSlice = cursor.getRawSlice();
            int rightOffset = cursor.getRawOffset();
            return valueEquals(type, slice, offset, rightSlice, rightOffset);
        }

        public void getValue(int position, BlockBuilder builder)
        {
            checkArgument(position >= 0 && position < positionOffsets.size());

            int offset = positionOffsets.getInt(position);

            if (slice.getByte(offset) != 0) {
                builder.appendNull();
            }
            else if (type == Type.FIXED_INT_64) {
                builder.append(slice.getLong(offset + SIZE_OF_BYTE));
            }
            else if (type == Type.DOUBLE) {
                builder.append(slice.getDouble(offset + SIZE_OF_BYTE));
            }
            else if (type == Type.BOOLEAN) {
                builder.append(slice.getByte(offset + SIZE_OF_BYTE) != 0);
            }
            else if (type == Type.VARIABLE_BINARY) {
                int sliceLength = getVariableBinaryLength(slice, offset);
                builder.append(slice.slice(offset + SIZE_OF_BYTE + SIZE_OF_INT, sliceLength));
            }
            else {
                throw new IllegalArgumentException("Unsupported type " + type);
            }
        }

        public int hashCode(int position)
        {
            checkArgument(position >= 0 && position < positionOffsets.size());
            return valueHashCode(type, slice, positionOffsets.getInt(position));
        }

        public boolean append(BlockCursor cursor)
        {
            int writableBytes = sliceOutput.writableBytes() - SIZE_OF_BYTE;

            boolean isNull = cursor.isNull(0);

            if (type == Type.FIXED_INT_64) {
                if (writableBytes < SIZE_OF_LONG) {
                    return false;
                }

                positionOffsets.add(sliceOutput.size());
                sliceOutput.writeByte(isNull ? 1 : 0);
                sliceOutput.appendLong(isNull ? 0 : cursor.getLong(0));
            }
            else if (type == Type.DOUBLE) {
                if (writableBytes < SIZE_OF_DOUBLE) {
                    return false;
                }

                positionOffsets.add(sliceOutput.size());
                sliceOutput.writeByte(isNull ? 1 : 0);
                sliceOutput.appendDouble(isNull ? 0 : cursor.getDouble(0));
            }
            else if (type == Type.BOOLEAN) {
                if (writableBytes < SIZE_OF_BYTE) {
                    return false;
                }

                positionOffsets.add(sliceOutput.size());
                sliceOutput.writeByte(isNull ? 1 : 0);
                sliceOutput.writeByte(!isNull && cursor.getBoolean(0) ? 1 : 0);
            }
            else if (type == Type.VARIABLE_BINARY) {
                int sliceLength = isNull ? 0 : getVariableBinaryLength(cursor.getRawSlice(), cursor.getRawOffset());
                if (writableBytes < SIZE_OF_INT + sliceLength) {
                    return false;
                }

                int startingOffset = sliceOutput.size();
                positionOffsets.add(startingOffset);
                sliceOutput.writeByte(isNull ? 1 : 0);
                sliceOutput.appendInt(sliceLength + SIZE_OF_BYTE + SIZE_OF_INT);
                if (!isNull) {
                    sliceOutput.writeBytes(cursor.getRawSlice(), cursor.getRawOffset() + SIZE_OF_BYTE + SIZE_OF_INT, sliceLength);
                }
            }
            else {
                throw new IllegalArgumentException("Unsupported type " + type);
            }
            return true;
        }

        public UncompressedBlock build()
        {
            checkState(!positionOffsets.isEmpty(), "Cannot build an empty block");

            return new UncompressedBlock(positionOffsets.size(), new TupleInfo(type), sliceOutput.slice());
        }
    }

    private static int addToHashCode(int result, int hashCode)
    {
        result = 31 * result + hashCode;
        return result;
    }

    private static int valueHashCode(Type type, Slice slice, int offset)
    {
        boolean isNull = slice.getByte(offset) != 0;
        if (isNull) {
            return 0;
        }

        if (type == Type.FIXED_INT_64) {
            return Longs.hashCode(slice.getLong(offset + SIZE_OF_BYTE));
        }
        else if (type == Type.DOUBLE) {
            long longValue = Double.doubleToLongBits(slice.getDouble(offset + SIZE_OF_BYTE));
            return Longs.hashCode(longValue);
        }
        else if (type == Type.BOOLEAN) {
            return slice.getByte(offset + SIZE_OF_BYTE) != 0 ? 1 : 0;
        }
        else if (type == Type.VARIABLE_BINARY) {
            int sliceLength = getVariableBinaryLength(slice, offset);
            return slice.hashCode(offset + SIZE_OF_BYTE + SIZE_OF_INT, sliceLength);
        }
        else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    private static int getVariableBinaryLength(Slice slice, int offset)
    {
        return slice.getInt(offset + SIZE_OF_BYTE) - SIZE_OF_INT - SIZE_OF_BYTE;
    }

    private static boolean valueEquals(Type type, Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        // check if null flags are the same
        boolean leftIsNull = leftSlice.getByte(leftOffset) != 0;
        boolean rightIsNull = rightSlice.getByte(rightOffset) != 0;
        if (leftIsNull != rightIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (leftIsNull) {
            return true;
        }

        if (type == Type.FIXED_INT_64 || type == Type.DOUBLE) {
            long leftValue = leftSlice.getLong(leftOffset + SIZE_OF_BYTE);
            long rightValue = rightSlice.getLong(rightOffset + SIZE_OF_BYTE);
            return leftValue == rightValue;
        }
        else if (type == Type.BOOLEAN) {
            boolean leftValue = leftSlice.getByte(leftOffset + SIZE_OF_BYTE) != 0;
            boolean rightValue = rightSlice.getByte(rightOffset + SIZE_OF_BYTE) != 0;
            return leftValue == rightValue;
        }
        else if (type == Type.VARIABLE_BINARY) {
            int leftLength = getVariableBinaryLength(leftSlice, leftOffset);
            int rightLength = getVariableBinaryLength(rightSlice, rightOffset);
            return leftSlice.equals(leftOffset + SIZE_OF_BYTE + SIZE_OF_INT, leftLength,
                    rightSlice, rightOffset + SIZE_OF_BYTE + SIZE_OF_INT, rightLength);
        }
        else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
    }
}
