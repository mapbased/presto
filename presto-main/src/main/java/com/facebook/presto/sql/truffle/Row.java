package com.facebook.presto.sql.truffle;

import io.airlift.slice.Slice;

public interface Row
{

    boolean getBoolean(int column);

    long getLong(int column);

    double getDouble(int column);

    Slice getSlice(int column);

    boolean isNull(int column);

    boolean isFiltered();

    void setFiltered();
}
