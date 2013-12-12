package com.facebook.presto.sql.truffle;

import com.facebook.presto.operator.RandomAccessPage;
import com.oracle.truffle.api.frame.VirtualFrame;

public abstract class PageNode
    extends SqlNode
{
    public abstract RandomAccessPage executeRow(VirtualFrame virtualFrame);
}
