package com.facebook.presto.sql.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.google.common.base.Charsets.UTF_8;

public class FilterRowNode
        extends RowNode
{
    private static final Slice MIN_SHIP_DATE = Slices.copiedBuffer("1994-01-01", UTF_8);
    private static final Slice MAX_SHIP_DATE = Slices.copiedBuffer("1995-01-01", UTF_8);
    private static final int SHIP_DATE = 2;
    private static final int DISCOUNT = 1;
    private static final int QUANTITY = 3;

    @Child
    private RowNode source;

    public FilterRowNode(RowNode source)
    {
        this.source = source;
    }

    @Override
    public Row executeRow(VirtualFrame virtualFrame)
    {
        Row row = source.executeRow(virtualFrame);
        if (row != null && !row.isFiltered() && !filter(row)) {
            row.setFiltered();
        }
        return row;
    }

    private boolean filter(Row row)
    {
        return !row.isNull(SHIP_DATE) && row.getSlice(SHIP_DATE).compareTo(MIN_SHIP_DATE) >= 0 &&
                !row.isNull(SHIP_DATE) && row.getSlice(SHIP_DATE).compareTo(MAX_SHIP_DATE) < 0 &&
                !row.isNull(DISCOUNT) && row.getDouble(DISCOUNT) >= 0.05 &&
                !row.isNull(DISCOUNT) && row.getDouble(DISCOUNT) <= 0.07 &&
                !row.isNull(QUANTITY) && row.getDouble(QUANTITY) < 24;
    }

}
