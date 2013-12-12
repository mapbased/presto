package com.facebook.presto.sql.truffle;

import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.RandomAccessPage;
import com.oracle.truffle.api.frame.VirtualFrame;

public class SourcePageNode
        extends PageNode
{
    @Override
    public RandomAccessPage executeRow(VirtualFrame virtualFrame)
    {
        MyArguments arguments = virtualFrame.getArguments(MyArguments.class);
        Operator operator = arguments.getSourceOperator();
        Page page = operator.getOutput();
        if (page == null) {
            return null;
        }
        return new RandomAccessPage(page);
    }
}
