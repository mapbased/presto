package com.facebook.presto.sql.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;

public abstract class RowNode
    extends SqlNode
{
    public abstract Row executeRow(VirtualFrame virtualFrame);
}
