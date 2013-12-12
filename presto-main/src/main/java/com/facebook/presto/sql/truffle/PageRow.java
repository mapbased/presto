package com.facebook.presto.sql.truffle;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Page;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkState;

public final class PageRow
        implements Row
{
    private final BlockCursor[] columns;
    private final int rows;
    private int currentRow;
    private boolean filtered;

    public PageRow(Page page)
    {
        this.columns = new BlockCursor[page.getChannelCount()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = page.getBlock(i).cursor();
        }
        rows = page.getPositionCount();
    }

    public boolean advanceNext()
    {
        filtered = false;

        boolean expectedResult = currentRow < rows;
        currentRow++;

        for (BlockCursor column : columns) {
            boolean result = column.advanceNextPosition();
            checkState(expectedResult == result);
        }
        return expectedResult;
    }

    @Override
    public boolean getBoolean(int column)
    {
        return columns[column].getBoolean();
    }

    @Override
    public long getLong(int column)
    {
        return columns[column].getLong();
    }

    @Override
    public double getDouble(int column)
    {
        return columns[column].getDouble();
    }

    @Override
    public Slice getSlice(int column)
    {
        return columns[column].getSlice();
    }

    @Override
    public boolean isNull(int column)
    {
        return columns[column].isNull();
    }

    @Override
    public boolean isFiltered()
    {
        return filtered;
    }

    @Override
    public void setFiltered()
    {
        this.filtered = true;
    }
}
