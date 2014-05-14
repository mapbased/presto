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
package com.facebook.presto.hive;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveBooleanParser.isFalse;
import static com.facebook.presto.hive.HiveBooleanParser.isTrue;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.NumberParser.parseDouble;
import static com.facebook.presto.hive.NumberParser.parseLong;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.Math.max;
import static java.lang.Math.min;

class OrcVectorHiveRecordCursor
        extends HiveRecordCursor
{
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    private final RecordReader recordReader;
//    private final DateTimeZone sessionTimeZone;

    @SuppressWarnings("FieldCanBeLocal") // include names for debugging
    private final String[] names;
    private final Type[] types;
    private final HiveType[] hiveTypes;

    private final int[] hiveColumnIndexes;

    private final boolean[] isPartitionColumn;

    private VectorizedRowBatch batch;
    private int batchIndex;

    private final ColumnVector[] columns;

    private final long totalBytes;
    private long completedBytes;
    private boolean closed;

    public OrcVectorHiveRecordCursor(RecordReader recordReader,
            long totalBytes,
            Properties splitSchema,
            List<HivePartitionKey> partitionKeys,
            List<HiveColumnHandle> columns,
            DateTimeZone sessionTimeZone)
    {
        checkNotNull(recordReader, "recordReader is null");
        checkArgument(totalBytes >= 0, "totalBytes is negative");
        checkNotNull(splitSchema, "splitSchema is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "columns is empty");
        checkNotNull(sessionTimeZone, "sessionTimeZone is null");

        this.recordReader = recordReader;
        this.totalBytes = totalBytes;

        int size = columns.size();

        this.names = new String[size];
        this.types = new Type[size];
        this.hiveTypes = new HiveType[size];

        this.columns = new ColumnVector[size];

        this.hiveColumnIndexes = new int[size];

        this.isPartitionColumn = new boolean[size];

        for (int i = 0; i < columns.size(); i++) {
            HiveColumnHandle column = columns.get(i);

            names[i] = column.getName();
            types[i] = column.getType();
            hiveTypes[i] = column.getHiveType();

            hiveColumnIndexes[i] = column.getHiveColumnIndex();
            isPartitionColumn[i] = column.isPartitionKey();
        }

        // parse requested partition columns
        Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey.nameGetter());
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            if (column.isPartitionKey()) {
                HivePartitionKey partitionKey = partitionKeysByName.get(column.getName());
                checkArgument(partitionKey != null, "Unknown partition key %s", column.getName());

                byte[] bytes = partitionKey.getValue().getBytes(Charsets.UTF_8);

                if (types[columnIndex].equals(BOOLEAN)) {
                    LongColumnVector columnVector = new LongColumnVector();
                    this.columns[columnIndex] = columnVector;
                    if (isTrue(bytes, 0, bytes.length)) {
                        columnVector.fill(1);
                    }
                    else if (isFalse(bytes, 0, bytes.length)) {
                        columnVector.fill(1);
                    }
                    else {
                        String valueString = new String(bytes, Charsets.UTF_8);
                        throw new IllegalArgumentException(String.format("Invalid partition value '%s' for BOOLEAN partition key %s", valueString, names[columnIndex]));
                    }
                }
                else if (types[columnIndex].equals(BIGINT)) {
                    LongColumnVector columnVector = new LongColumnVector();
                    this.columns[columnIndex] = columnVector;
                    if (bytes.length == 0) {
                        throw new IllegalArgumentException(String.format("Invalid partition value '' for BIGINT partition key %s", names[columnIndex]));
                    }
                    columnVector.fill(parseLong(bytes, 0, bytes.length));
                }
                else if (types[columnIndex].equals(DOUBLE)) {
                    DoubleColumnVector columnVector = new DoubleColumnVector();
                    this.columns[columnIndex] = columnVector;
                    if (bytes.length == 0) {
                        throw new IllegalArgumentException(String.format("Invalid partition value '' for DOUBLE partition key %s", names[columnIndex]));
                    }
                    columnVector.fill(parseDouble(bytes, 0, bytes.length));
                }
                else if (types[columnIndex].equals(VARCHAR)) {
                    BytesColumnVector columnVector = new BytesColumnVector();
                    this.columns[columnIndex] = columnVector;
                    columnVector.fill(bytes);
                }
                else if (types[columnIndex].equals(DATE)) {
                    LongColumnVector columnVector = new LongColumnVector();
                    this.columns[columnIndex] = columnVector;
                    long millis = ISODateTimeFormat.date().withZone(DateTimeZone.UTC).parseMillis(partitionKey.getValue());
                    long days = millis / MILLIS_IN_DAY;
                    columnVector.fill(days);
                }
                else {
                    throw new UnsupportedOperationException("Unsupported column type: " + types[columnIndex]);
                }
            }
        }
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        if (!closed) {
            updateCompletedBytes();
        }
        return completedBytes;
    }

    private void updateCompletedBytes()
    {
        try {
            long newCompletedBytes = (long) (totalBytes * recordReader.getProgress());
            completedBytes = min(totalBytes, max(completedBytes, newCompletedBytes));
        }
        catch (IOException ignored) {
        }
    }

    @Override
    public Type getType(int field)
    {
        return types[field];
    }

    @Override
    public boolean advanceNextPosition()
    {
        batchIndex++;
        if (batch != null && batchIndex < batch.size) {
            return true;
        }

        try {
            if (closed || !recordReader.hasNext()) {
                close();
                return false;
            }

            batch = recordReader.nextBatch(batch);
            batchIndex = 0;

            // reset column vectors
            for (int i = 0; i < columns.length; i++) {
                if (!isPartitionColumn[i]) {
                    columns[i] = null;
                }
            }

            return true;
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR.toErrorCode(), e);
        }
    }

    @Override
    public boolean getBoolean(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, boolean.class);
        LongColumnVector column = (LongColumnVector) getColumnVector(fieldId);
        int valueIndex = column.isRepeating ? 0 : batchIndex;
        return column.vector[valueIndex] != 0;
    }

    @Override
    public long getLong(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, long.class);
        LongColumnVector column = (LongColumnVector) getColumnVector(fieldId);
        int valueIndex = column.isRepeating ? 0 : batchIndex;
        long value = column.vector[valueIndex];
        if (hiveTypes[fieldId] == HiveType.TIMESTAMP) {
            value /= 1_000_000L;
        }
        if (hiveTypes[fieldId] == HiveType.DATE) {
            value *= MILLIS_IN_DAY;
        }
        return value;
    }

    @Override
    public double getDouble(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, double.class);
        DoubleColumnVector column = (DoubleColumnVector) getColumnVector(fieldId);
        int valueIndex = column.isRepeating ? 0 : batchIndex;
        return column.vector[valueIndex];
    }

    @Override
    public Slice getSlice(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, Slice.class);
        BytesColumnVector column = (BytesColumnVector) getColumnVector(fieldId);
        int valueIndex = column.isRepeating ? 0 : batchIndex;
        if (column.length[valueIndex] == 0) {
            return Slices.EMPTY_SLICE;
        }
        return Slices.wrappedBuffer(Arrays.copyOfRange(column.vector[valueIndex], column.start[valueIndex], column.start[valueIndex] + column.length[valueIndex]));
    }

    @Override
    public boolean isNull(int fieldId)
    {
        checkState(!closed, "Cursor is closed");
        ColumnVector column = getColumnVector(fieldId);
        int valueIndex = column.isRepeating ? 0 : batchIndex;
        return column.isNull[valueIndex];
    }

    private ColumnVector getColumnVector(int fieldId)
    {
        if (columns[fieldId] == null) {
            columns[fieldId] = batch.cols[hiveColumnIndexes[fieldId]];
        }
        return columns[fieldId];
    }

    private void validateType(int fieldId, Class<?> javaType)
    {
        if (types[fieldId].getJavaType() != javaType) {
            // we don't use Preconditions.checkArgument because it requires boxing fieldId, which affects inner loop performance
            throw new IllegalArgumentException(String.format("Expected field to be %s, actual %s (field %s)", javaType.getName(), types[fieldId].getJavaType().getName(), fieldId));
        }
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        updateCompletedBytes();

        try {
            recordReader.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
