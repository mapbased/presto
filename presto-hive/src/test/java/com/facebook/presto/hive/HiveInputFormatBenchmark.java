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

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import sun.misc.Unsafe;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveWriteFile.writeFile;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings("deprecation")
public final class HiveInputFormatBenchmark
{
    private static final int LOOPS = 1;

    private HiveInputFormatBenchmark()
    {
    }

    public static void main(String[] args)
            throws Exception
    {
        List<BenchmarkFile> benchmarkFiles = ImmutableList.of(
                new BenchmarkFile(
                        "text",
                        new File("target/presto_test.txt"),
                        new TextInputFormat(),
                        new HiveIgnoreKeyTextOutputFormat<>(),
                        new LazySimpleSerDe(),
                        true),

//                new BenchmarkFile(
//                        "text no-crc",
//                        new File("target/presto_test.txt"),
//                        new TextInputFormat(),
//                        new HiveIgnoreKeyTextOutputFormat<>(),
//                        new LazySimpleSerDe(),
//                        false),

                new BenchmarkFile(
                        "sequence",
                        new File("target/presto_test.sequence"),
                        new SequenceFileInputFormat<Object, Writable>(),
                        new HiveSequenceFileOutputFormat<>(),
                        new LazySimpleSerDe(),
                        true),

//                new BenchmarkFile(
//                        "sequence no-crc",
//                        new File("target/presto_test.sequence"),
//                        new SequenceFileInputFormat<Object, Writable>(),
//                        new HiveSequenceFileOutputFormat<>(),
//                        new LazySimpleSerDe(),
//                        false),

                new BenchmarkFile(
                        "rc text",
                        new File("target/presto_test.rc"),
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new ColumnarSerDe(),
                        true),

//                new BenchmarkFile(
//                        "rc text no-crc",
//                        new File("target/presto_test.rc"),
//                        new RCFileInputFormat<>(),
//                        new RCFileOutputFormat(),
//                        new ColumnarSerDe(),
//                        false),

                new BenchmarkFile(
                        "rc binary",
                        new File("target/presto_test.rc-binary"),
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new LazyBinaryColumnarSerDe(),
                        true)
//
//                new BenchmarkFile(
//                        "rc binary no-crc",
//                        new File("target/presto_test.rc-binary"),
//                        new RCFileInputFormat<>(),
//                        new RCFileOutputFormat(),
//                        new LazyBinaryColumnarSerDe(),
//                        false)
        );

        JobConf jobConf = new JobConf();
//        while (System.currentTimeMillis() > 0) {
//            benchmark(jobConf, benchmarkFiles.get(3), 4);
//        }

        System.out.println("============ WARM UP ============");
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            benchmark(jobConf, benchmarkFile, 7);
        }

        System.out.println();
        System.out.println();
        System.out.println("============ BENCHMARK ============");
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            benchmark(jobConf, benchmarkFile, 4);
        }
    }

    private static void benchmark(JobConf jobConf, BenchmarkFile benchmarkFile, int loopCount)
            throws Exception
    {
        System.out.println(benchmarkFile.getName());

        Object value = null;

        long start = System.nanoTime();

        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadString(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
        }
        logDuration("string", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadStringText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        if (benchmarkFile.getDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadStringColumnarText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        if (benchmarkFile.getDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadStringColumnarBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        logDuration("b_string", start, loopCount, value);
        
//        start = System.nanoTime();
//        for (int loops = 0; loops < loopCount; loops++) {
//            benchmarkReadSmallint(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
//        }
//        logDuration("smallint", start, loopCount, value);

        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadInt(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
        }
        logDuration("int", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadIntText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        if (benchmarkFile.getDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadIntColumnarText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        if (benchmarkFile.getDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadIntColumnarBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        logDuration("b_int", start, loopCount, value);


        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadBigint(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
        }
        logDuration("bigint", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBigintText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        if (benchmarkFile.getDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBigintColumnarText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        if (benchmarkFile.getDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBigintColumnarBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        logDuration("b_bigint", start, loopCount, value);


//        start = System.nanoTime();
//        for (int loops = 0; loops < loopCount; loops++) {
//            benchmarkReadFloat(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
//        }
//        logDuration("float", start, loopCount, value);

        start = System.nanoTime();
        // todo rc file seems to be invalid
        value = null;
        if (!(benchmarkFile.getDeserializer() instanceof LazyBinaryColumnarSerDe)) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadDouble(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        logDuration("double", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadDoubleText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        if (benchmarkFile.getDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadDoubleColumnarText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        // todo rc file seems to be invalid
        if (benchmarkFile.getDeserializer() instanceof LazyBinaryColumnarSerDe) {
            value = null;
//            for (int loops = 0; loops < loopCount; loops++) {
//                value = benchmarkReadDoubleColumnarBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
//            }
        }
        logDuration("b_double", start, loopCount, value);

//        start = System.nanoTime();
//        for (int loops = 0; loops < loopCount; loops++) {
//            benchmarkReadBoolean(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
//        }
//        logDuration("boolean", start, loopCount, value);
//
//        start = System.nanoTime();
//        for (int loops = 0; loops < loopCount; loops++) {
//            benchmarkReadBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
//        }
//        logDuration("binary", start, loopCount, value);
//
//        start = System.nanoTime();
//        for (int loops = 0; loops < loopCount; loops++) {
//            benchmarkRead3Columns(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
//        }
//        logDuration("three", start, loopCount, value);
//
//        start = System.nanoTime();
//        for (int loops = 0; loops < loopCount; loops++) {
//            benchmarkReadAllColumns(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
//        }
//        logDuration("all", start, loopCount, value);
    }

    private static void logDuration(String label, long start, int loopCount, Object value)
    {
        long end = System.nanoTime();
        long nanos = end - start;
        Duration duration = new Duration(1.0 * nanos / loopCount, NANOSECONDS).convertTo(SECONDS);
        System.out.printf("%10s %6s %s\n", label, duration, value);
    }

    private static <K, V extends Writable> void benchmarkReadAllColumns(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        PrimitiveObjectInspector smallintFieldInspector = (PrimitiveObjectInspector) smallintField.getFieldObjectInspector();

        StructField intField = rowInspector.getStructFieldRef("t_int");
        PrimitiveObjectInspector intFieldInspector = (PrimitiveObjectInspector) intField.getFieldObjectInspector();

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        PrimitiveObjectInspector bigintFieldInspector = (PrimitiveObjectInspector) bigintField.getFieldObjectInspector();

        StructField floatField = rowInspector.getStructFieldRef("t_float");
        PrimitiveObjectInspector floatFieldInspector = (PrimitiveObjectInspector) floatField.getFieldObjectInspector();

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        PrimitiveObjectInspector doubleFieldInspector = (PrimitiveObjectInspector) doubleField.getFieldObjectInspector();

        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        PrimitiveObjectInspector booleanFieldInspector = (PrimitiveObjectInspector) booleanField.getFieldObjectInspector();

        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        PrimitiveObjectInspector binaryFieldInspector = (PrimitiveObjectInspector) binaryField.getFieldObjectInspector();

        for (int i = 0; i < LOOPS; i++) {
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            long stringLengthSum = 0;
            long smallintSum = 0;
            long intSum = 0;
            long bigintSum = 0;
            double floatSum = 0;
            double doubleSum = 0;
            long booleanSum = 0;
            long binaryLengthSum = 0;

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringLengthSum += stringValue.length();
                }

                Object smallintData = rowInspector.getStructFieldData(rowData, smallintField);
                if (smallintData != null) {
                    Object smallintPrimitive = smallintFieldInspector.getPrimitiveJavaObject(smallintData);
                    short shortValue = ((Number) smallintPrimitive).shortValue();
                    smallintSum += shortValue;
                }

                Object intData = rowInspector.getStructFieldData(rowData, intField);
                if (intData != null) {
                    Object intPrimitive = intFieldInspector.getPrimitiveJavaObject(intData);
                    int intValue = ((Number) intPrimitive).intValue();
                    intSum += intValue;
                }

                Object bigintData = rowInspector.getStructFieldData(rowData, bigintField);
                if (bigintData != null) {
                    Object bigintPrimitive = bigintFieldInspector.getPrimitiveJavaObject(bigintData);
                    long bigintValue = ((Number) bigintPrimitive).longValue();
                    bigintSum += bigintValue;
                }

                Object floatData = rowInspector.getStructFieldData(rowData, floatField);
                if (floatData != null) {
                    Object floatPrimitive = floatFieldInspector.getPrimitiveJavaObject(floatData);
                    float floatValue = ((Number) floatPrimitive).floatValue();
                    floatSum += floatValue;
                }

                Object doubleData = rowInspector.getStructFieldData(rowData, doubleField);
                if (doubleData != null) {
                    Object doublePrimitive = doubleFieldInspector.getPrimitiveJavaObject(doubleData);
                    double doubleValue = ((Number) doublePrimitive).doubleValue();
                    doubleSum += doubleValue;
                }

                Object booleanData = rowInspector.getStructFieldData(rowData, booleanField);
                if (booleanData != null) {
                    Object booleanPrimitive = booleanFieldInspector.getPrimitiveJavaObject(booleanData);
                    boolean booleanValue = ((Boolean) booleanPrimitive);
                    booleanSum += booleanValue ? 1 : 0;
                }

                Object binaryData = rowInspector.getStructFieldData(rowData, binaryField);
                if (binaryData != null) {
                    Object binaryPrimitive = binaryFieldInspector.getPrimitiveJavaObject(binaryData);
                    byte[] binaryValue = (byte[]) binaryPrimitive;
                    binaryLengthSum += binaryValue.length;
                }
            }
            recordReader.close();
        }
    }

    private static <K, V extends Writable> void benchmarkRead3Columns(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        StructField stringField = rowInspector.getStructFieldRef("t_string");
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        PrimitiveObjectInspector doubleFieldInspector = (PrimitiveObjectInspector) doubleField.getFieldObjectInspector();
        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        PrimitiveObjectInspector bigintFieldInspector = (PrimitiveObjectInspector) bigintField.getFieldObjectInspector();

        for (int i = 0; i < LOOPS; i++) {
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            long stringLengthSum = 0;
            double doubleSum = 0;
            long bigintSum = 0;

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringLengthSum += stringValue.length();
                }

                Object doubleData = rowInspector.getStructFieldData(rowData, doubleField);
                if (doubleData != null) {
                    Object doublePrimitive = doubleFieldInspector.getPrimitiveJavaObject(doubleData);
                    double doubleValue = ((Number) doublePrimitive).doubleValue();
                    doubleSum += doubleValue;
                }

                Object bigintData = rowInspector.getStructFieldData(rowData, bigintField);
                if (bigintData != null) {
                    Object bigintPrimitive = bigintFieldInspector.getPrimitiveJavaObject(bigintData);
                    long bigintValue = ((Number) bigintPrimitive).longValue();
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
    }

    private static <K, V extends Writable> long benchmarkReadString(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringLengthSum += stringValue.length();
                }

            }
            recordReader.close();
        }
        return stringLengthSum;
    }

    private static <K, V extends Writable> long benchmarkReadStringText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        int[] startPosition = new int[13];

        long stringSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int start = startPosition[fieldIndex];
                int length = startPosition[fieldIndex + 1] - start - 1;

                if (!isNull(bytes, start, length)) {
                    byte[] stringValue = Arrays.copyOfRange(bytes, start, start + length);
                    stringSum += stringValue.length;
                }
            }
            recordReader.close();
        }
        return stringSum;
    }

    private static <K, V extends Writable> long benchmarkReadStringColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        long stringSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    byte[] stringValue = Arrays.copyOfRange(bytes, start, start + length);
                    stringSum += stringValue.length;
                }
            }
            recordReader.close();
        }
        return stringSum;
    }

    private static <K, V extends Writable> long benchmarkReadStringColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        long stringSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    byte[] stringValue = Arrays.copyOfRange(bytes, start, start + length);
                    stringSum += stringValue.length;
                }
            }
            recordReader.close();
        }
        return stringSum;
    }

    private static <K, V extends Writable> void benchmarkReadSmallint(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        PrimitiveObjectInspector smallintFieldInspector = (PrimitiveObjectInspector) smallintField.getFieldObjectInspector();

        for (int i = 0; i < LOOPS; i++) {
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            long smallintSum = 0;

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object smallintData = rowInspector.getStructFieldData(rowData, smallintField);
                if (smallintData != null) {
                    Object smallintPrimitive = smallintFieldInspector.getPrimitiveJavaObject(smallintData);
                    short shortValue = ((Number) smallintPrimitive).shortValue();
                    smallintSum += shortValue;
                }
            }
            recordReader.close();
        }
    }

    private static <K, V extends Writable> long benchmarkReadInt(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        StructField intField = rowInspector.getStructFieldRef("t_int");
        PrimitiveObjectInspector intFieldInspector = (PrimitiveObjectInspector) intField.getFieldObjectInspector();

        long intSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            intSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object intData = rowInspector.getStructFieldData(rowData, intField);
                if (intData != null) {
                    Object intPrimitive = intFieldInspector.getPrimitiveJavaObject(intData);
                    int intValue = ((Number) intPrimitive).intValue();
                    intSum += intValue;
                }
            }
            recordReader.close();
        }
        return intSum;
    }

    private static <K, V extends Writable> long benchmarkReadIntText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField intField = rowInspector.getStructFieldRef("t_int");
        int fieldIndex = allStructFieldRefs.indexOf(intField);

        int[] startPosition = new int[13];

        long intSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            intSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int start = startPosition[fieldIndex];
                int length = startPosition[fieldIndex + 1] - start - 1;

                if (!isNull(bytes, start, length)) {
                    long intValue = NumberParser.parseLong(bytes, start, length);
                    intSum += intValue;
                }
            }
            recordReader.close();
        }
        return intSum;
    }

    private static <K, V extends Writable> long benchmarkReadIntColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField intField = rowInspector.getStructFieldRef("t_int");
        int fieldIndex = allStructFieldRefs.indexOf(intField);

        long intSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            intSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    long intValue = NumberParser.parseLong(bytes, start, length);
                    intSum += intValue;
                }
            }
            recordReader.close();
        }
        return intSum;
    }

    private static <K, V extends Writable> long benchmarkReadIntColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField intField = rowInspector.getStructFieldRef("t_int");
        int fieldIndex = allStructFieldRefs.indexOf(intField);

        long intSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            intSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    int intValue = readVInt(bytes, start);
                    intSum += intValue;
                }
            }
            recordReader.close();
        }
        return intSum;
    }

    public static int readVInt(byte[] bytes, int offset)
    {
        byte firstByte = bytes[offset];
        int size = WritableUtils.decodeVIntSize(firstByte);
        if (size == 1) {
            return firstByte;
        }
        int i = 0;
        for (int idx = 0; idx < size - 1; idx++) {
            byte b = bytes[offset + 1 + idx];
            i = i << 8;
            i = i | (b & 0xFF);
        }
        return WritableUtils.isNegativeVInt(firstByte) ? ~i : i;
    }

    private static <K, V extends Writable> long benchmarkReadBigint(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        PrimitiveObjectInspector bigintFieldInspector = (PrimitiveObjectInspector) bigintField.getFieldObjectInspector();

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object bigintData = rowInspector.getStructFieldData(rowData, bigintField);
                if (bigintData != null) {
                    Object bigintPrimitive = bigintFieldInspector.getPrimitiveJavaObject(bigintData);
                    long bigintValue = ((Number) bigintPrimitive).longValue();
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    private static <K, V extends Writable> long benchmarkReadBigintText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        int[] startPosition = new int[13];

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int start = startPosition[fieldIndex];
                int length = startPosition[fieldIndex + 1] - start - 1;

                if (!isNull(bytes, start, length)) {
                    long bigintValue = NumberParser.parseLong(bytes, start, length);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    private static <K, V extends Writable> long benchmarkReadBigintColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    long bigintValue = NumberParser.parseLong(bytes, start, length);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    private static <K, V extends Writable> long benchmarkReadBigintColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    long bigintValue = readVBigint(bytes, start);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    public static long readVBigint(byte[] bytes, int offset)
    {
        byte firstByte = bytes[offset];
        int size = WritableUtils.decodeVIntSize(firstByte);
        if (size == 1) {
            return firstByte;
        }
        long i = 0;
        for (int idx = 0; idx < size - 1; idx++) {
            byte b = bytes[offset + 1 + idx];
            i = i << 8;
            i = i | (b & 0xFF);
        }
        return WritableUtils.isNegativeVInt(firstByte) ? ~i : i;
    }

    private static <K, V extends Writable> void benchmarkReadFloat(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        StructField floatField = rowInspector.getStructFieldRef("t_float");
        PrimitiveObjectInspector floatFieldInspector = (PrimitiveObjectInspector) floatField.getFieldObjectInspector();

        for (int i = 0; i < LOOPS; i++) {
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            double floatSum = 0;

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object floatData = rowInspector.getStructFieldData(rowData, floatField);
                if (floatData != null) {
                    Object floatPrimitive = floatFieldInspector.getPrimitiveJavaObject(floatData);
                    float floatValue = ((Number) floatPrimitive).floatValue();
                    floatSum += floatValue;
                }
            }
            recordReader.close();
        }
    }

    private static <K, V extends Writable> double benchmarkReadDouble(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        PrimitiveObjectInspector doubleFieldInspector = (PrimitiveObjectInspector) doubleField.getFieldObjectInspector();

        double doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object doubleData = rowInspector.getStructFieldData(rowData, doubleField);
                if (doubleData != null) {
                    Object doublePrimitive = doubleFieldInspector.getPrimitiveJavaObject(doubleData);
                    double doubleValue = ((Number) doublePrimitive).doubleValue();
                    doubleSum += doubleValue;
                }
            }
            recordReader.close();
        }
        return doubleSum;
    }

    private static <K, V extends Writable> double benchmarkReadDoubleText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);

        int[] startPosition = new int[13];

        long doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int start = startPosition[fieldIndex];
                int length = startPosition[fieldIndex + 1] - start - 1;

                if (!isNull(bytes, start, length)) {
                    double doubleValue = NumberParser.parseDouble(bytes, start, length);
                    doubleSum += doubleValue;
                }
            }
            recordReader.close();
        }
        return doubleSum;
    }

    private static <K, V extends Writable> double benchmarkReadDoubleColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);

        double doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    double doubleValue = NumberParser.parseDouble(bytes, start, length);
                    doubleSum += doubleValue;
                }
            }
            recordReader.close();
        }
        return doubleSum;
    }

    private static <K, V extends Writable> double benchmarkReadDoubleColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);

        double doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    long longBits = unsafe.getLong(((BytesRefArrayWritable) value).get(6).getData(), (long) Unsafe.ARRAY_BYTE_BASE_OFFSET);
                    double doubleValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                    doubleSum += doubleValue;
                }
            }
            recordReader.close();
        }
        return doubleSum;
    }

    private static <K, V extends Writable> void benchmarkReadBoolean(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        PrimitiveObjectInspector booleanFieldInspector = (PrimitiveObjectInspector) booleanField.getFieldObjectInspector();

        for (int i = 0; i < LOOPS; i++) {
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            long booleanSum = 0;

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object booleanData = rowInspector.getStructFieldData(rowData, booleanField);
                if (booleanData != null) {
                    Object booleanPrimitive = booleanFieldInspector.getPrimitiveJavaObject(booleanData);
                    boolean booleanValue = ((Boolean) booleanPrimitive);
                    booleanSum += booleanValue ? 1 : 0;
                }
            }
            recordReader.close();
        }
    }

    private static <K, V extends Writable> void benchmarkReadBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        PrimitiveObjectInspector binaryFieldInspector = (PrimitiveObjectInspector) binaryField.getFieldObjectInspector();

        for (int i = 0; i < LOOPS; i++) {
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            long binaryLengthSum = 0;

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object binaryData = rowInspector.getStructFieldData(rowData, binaryField);
                if (binaryData != null) {
                    Object binaryPrimitive = binaryFieldInspector.getPrimitiveJavaObject(binaryData);
                    byte[] binaryValue = (byte[]) binaryPrimitive;
                    binaryLengthSum += binaryValue.length;
                }
            }
            recordReader.close();
        }
    }

    private static void parseTextFields(byte[] bytes, int start, int length, int[] startPosition)
    {
        byte separator = 1;
//        byte separator = oi.getSeparator();
//        boolean lastColumnTakesRest = oi.getLastColumnTakesRest();
//        boolean isEscaped = oi.isEscaped();
//        byte escapeChar = oi.getEscapeChar();

//        if (fields == null) {
//            List<? extends StructField> fieldRefs = ((StructObjectInspector) oi)
//                    .getAllStructFieldRefs();
//            fields = new LazyObject[fieldRefs.size()];
//            for (int i = 0; i < fields.length; i++) {
//                fields[i] = LazyFactory.createLazyObject(fieldRefs.get(i)
//                        .getFieldObjectInspector());
//            }
//            fieldInited = new boolean[fields.length];
//            // Extra element to make sure we have the same formula to compute the
//            // length of each element of the array.
//            startPosition = new int[fields.length + 1];
//        }

        final int structByteEnd = start + length;
        int fieldId = 0;
        int fieldByteBegin = start;
        int fieldByteEnd = start;

        // Go through all bytes in the byte[]
        while (fieldByteEnd <= structByteEnd) {
            if (fieldByteEnd == structByteEnd || bytes[fieldByteEnd] == separator) {
                // Reached the end of a field?
//                if (lastColumnTakesRest && fieldId == fields.length - 1) {
//                    fieldByteEnd = structByteEnd;
//                }
                startPosition[fieldId] = fieldByteBegin;
                fieldId++;

                if (fieldId == startPosition.length - 1 || fieldByteEnd == structByteEnd) {
                    // All fields have been parsed, or bytes have been parsed.
                    // We need to set the startPosition of fields.length to ensure we
                    // can use the same formula to calculate the length of each field.
                    // For missing fields, their starting positions will all be the same,
                    // which will make their lengths to be -1 and uncheckedGetField will
                    // return these fields as NULLs.
                    for (int i = fieldId; i < startPosition.length; i++) {
                        startPosition[i] = fieldByteEnd + 1;
                    }
                    break;
                }

                fieldByteBegin = fieldByteEnd + 1;
                fieldByteEnd++;
            }
            else {
//                if (isEscaped && bytes[fieldByteEnd] == escapeChar
//                        && fieldByteEnd + 1 < structByteEnd) {
//                    // ignore the char after escape_char
//                    fieldByteEnd += 2;
//                }
//                else {
                fieldByteEnd++;
//                }
            }
        }

//        // Extra bytes at the end?
//        if (!extraFieldWarned && fieldByteEnd < structByteEnd) {
//            extraFieldWarned = true;
//            LOG.warn("Extra bytes detected at the end of the row! Ignoring similar "
//                    + "problems.");
//        }
//
//        // Missing fields?
//        if (!missingFieldWarned && fieldId < fields.length) {
//            missingFieldWarned = true;
//            LOG.info("Missing fields! Expected " + fields.length + " fields but "
//                    + "only got " + fieldId + "! Ignoring similar problems.");
//        }
//
//        Arrays.fill(fieldInited, false);
//        parsed = true;
    }
//
//    private static Deserializer initializeDeserializer(Deserializer deserializer)
//            throws SerDeException
//    {
//        Properties tableProperties = new Properties();
//        tableProperties.setProperty(
//                "columns",
//                "t_string,t_tinyint,t_smallint,t_int,t_bigint,t_float,t_double,t_map,t_boolean,t_timestamp,t_binary,t_array_string,t_complex");
//        tableProperties.setProperty(
//                "columns.types",
//                "string:tinyint:smallint:int:bigint:float:double:map<string,string>:boolean:timestamp:binary:array<string>:map<int,array<struct<s_string:string,s_double:double>>>");
//
//        deserializer.initialize(new Configuration(), tableProperties);
//
//        return deserializer;
//    }
//
//    private static Deserializer initializeBinaryDeserializer(Deserializer deserializer)
//            throws SerDeException
//    {
//        Properties tableProperties = new Properties();
//        tableProperties.setProperty(
//                "columns",
//                "t_string,t_tinyint,t_smallint,t_int,t_bigint,t_float,t_double,t_map,t_boolean,t_timestamp,t_binary,t_array_string,t_complex");
//        tableProperties.setProperty(
//                "columns.types",
//                Joiner.on(":").join(Collections.nCopies(13, "string")));
//
//        deserializer.initialize(new Configuration(), tableProperties);
//
//        return deserializer;
//    }
//
//    private static FileSplit createFileSplit(String remotePath, String localFile)
//            throws IOException
//    {
//        File file = new File(localFile);
//        if (!file.exists()) {
//            Configuration config = new Configuration();
//            config.setClass("hadoop.rpc.socket.factory.class.default", SocksSocketFactory.class, SocketFactory.class);
//            config.set("hadoop.socks.server", "localhost:1080");
//
//            Path path = new Path(remotePath);
//            ByteStreams.copy(newHdfsInputStreamSupplier(config, path), Files.newOutputStreamSupplier(file));
//        }
//        return new FileSplit(new Path(file.toURI()), 0, file.length(), new String[0]);
//    }
//
//    public static InputSupplier<FSDataInputStream> newHdfsInputStreamSupplier(final Configuration configuration, final Path path)
//    {
//        return new InputSupplier<FSDataInputStream>()
//        {
//            @Override
//            public FSDataInputStream getInput()
//                    throws IOException
//            {
//                return path.getFileSystem(configuration).open(path);
//            }
//        };
//    }

    private static boolean isNull(byte[] bytes, int start, int length)
    {
        return length == 2 && bytes[start] == '\\' && bytes[start + 1] == 'N';
    }

    private static class BenchmarkFile
    {
        private final String name;
        private final InputFormat<?, ? extends Writable> inputFormat;
        private final Deserializer deserializer;
        private final FileSplit fileSplit;

        public BenchmarkFile(
                String name,
                File file,
                InputFormat<?, ? extends Writable> inputFormat,
                HiveOutputFormat<?, ?> outputFormat,
                SerDe serDe,
                boolean verifyChecksum)
                throws Exception
        {
            this.name = name;
            this.inputFormat = inputFormat;

            file.getParentFile().mkdirs();

            Properties tableProperties = new Properties();
            tableProperties.setProperty(
                    "columns",
                    "t_string,t_tinyint,t_smallint,t_int,t_bigint,t_float,t_double,t_map,t_boolean,t_timestamp,t_binary,t_array_string,t_complex");
            tableProperties.setProperty(
                    "columns.types",
                    "string:tinyint:smallint:int:bigint:float:double:map<string,string>:boolean:timestamp:binary:array<string>:map<int,array<struct<s_string:string,s_double:double>>>");
            serDe.initialize(new Configuration(), tableProperties);

            if (!file.exists()) {
                writeFile(tableProperties, file, outputFormat, serDe);
            }

            this.deserializer = serDe;
            Path path = new Path(file.toURI());
            path.getFileSystem(new Configuration()).setVerifyChecksum(verifyChecksum);
            this.fileSplit = new FileSplit(path, 0, file.length(), new String[0]);
        }

        private String getName()
        {
            return name;
        }

        private InputFormat<?, ? extends Writable> getInputFormat()
        {
            return inputFormat;
        }

        private Deserializer getDeserializer()
        {
            return deserializer;
        }

        private FileSplit getFileSplit()
        {
            return fileSplit;
        }
    }

    private static final Unsafe unsafe;

    static {
        try {
            // fetch theUnsafe object
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }

            // make sure the VM thinks bytes are only one byte wide
            if (Unsafe.ARRAY_BYTE_INDEX_SCALE != 1) {
                throw new IllegalStateException("Byte array index scale must be 1, but is " + Unsafe.ARRAY_BYTE_INDEX_SCALE);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
