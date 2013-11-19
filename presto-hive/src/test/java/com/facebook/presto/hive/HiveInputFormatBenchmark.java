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

import com.facebook.hive.orc.OrcInputFormat;
import com.facebook.hive.orc.OrcOutputFormat;
import com.facebook.hive.orc.OrcSerde;
import com.facebook.hive.orc.lazy.OrcLazyObject;
import com.facebook.hive.orc.lazy.OrcLazyRow;
import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.hive.shaded.org.apache.commons.codec.binary.Base64;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Progressable;
import org.joda.time.DateTime;
import sun.misc.Unsafe;

import java.io.File;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveBooleanParser.isFalse;
import static com.facebook.presto.hive.HiveBooleanParser.isTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_CODEC;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_TYPE;

@SuppressWarnings("deprecation")
public class HiveInputFormatBenchmark
{
    private static final int LOOPS = 1;

    public static void main(String[] args)
            throws Exception
    {
        HadoopNative.requireHadoopNative();

        List<BenchmarkFile> benchmarkFiles = ImmutableList.of(
                new BenchmarkFile(
                        "dwrf",
                        new File("target/presto_test.dwrf"),
                        new OrcInputFormat(),
                        new OrcOutputFormat(),
                        new OrcSerde(),
                        null,
                        true),

                new BenchmarkFile(
                        "text",
                        new File("target/presto_test.txt"),
                        new TextInputFormat(),
                        new HiveIgnoreKeyTextOutputFormat<>(),
                        new LazySimpleSerDe(),
                        null,
                        true),

                new BenchmarkFile(
                        "text gzip",
                        new File("target/presto_test.txt.gz"),
                        new TextInputFormat(),
                        new HiveIgnoreKeyTextOutputFormat<>(),
                        new LazySimpleSerDe(),
                        "gzip",
                        true),

                new BenchmarkFile(
                        "text snappy",
                        new File("target/presto_test.txt.snappy"),
                        new TextInputFormat(),
                        new HiveIgnoreKeyTextOutputFormat<>(),
                        new LazySimpleSerDe(),
                        "snappy",
                        true),

                new BenchmarkFile(
                        "sequence",
                        new File("target/presto_test.sequence"),
                        new SequenceFileInputFormat<Object, Writable>(),
                        new HiveSequenceFileOutputFormat<>(),
                        new LazySimpleSerDe(),
                        null,
                        true),

                new BenchmarkFile(
                        "sequence gzip",
                        new File("target/presto_test.sequence.gz"),
                        new SequenceFileInputFormat<Object, Writable>(),
                        new HiveSequenceFileOutputFormat<>(),
                        new LazySimpleSerDe(),
                        "gzip",
                        true),

                new BenchmarkFile(
                        "sequence snappy",
                        new File("target/presto_test.sequence.snappy"),
                        new SequenceFileInputFormat<Object, Writable>(),
                        new HiveSequenceFileOutputFormat<>(),
                        new LazySimpleSerDe(),
                        "snappy",
                        true),

                new BenchmarkFile(
                        "rc text",
                        new File("target/presto_test.rc"),
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new ColumnarSerDe(),
                        null,
                        true),

                new BenchmarkFile(
                        "rc text gzip",
                        new File("target/presto_test.rc.gz"),
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new ColumnarSerDe(),
                        "gzip",
                        true),

                new BenchmarkFile(
                        "rc text snappy",
                        new File("target/presto_test.rc.snappy"),
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new ColumnarSerDe(),
                        "snappy",
                        true),

                new BenchmarkFile(
                        "rc binary",
                        new File("target/presto_test.rc-binary"),
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new LazyBinaryColumnarSerDe(),
                        null,
                        true),

                new BenchmarkFile(
                        "rc binary gzip",
                        new File("target/presto_test.rc-binary.gz"),
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new LazyBinaryColumnarSerDe(),
                        "gzip",
                        true),

                new BenchmarkFile(
                        "rc binary snappy",
                        new File("target/presto_test.rc-binary.snappy"),
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new LazyBinaryColumnarSerDe(),
                        "snappy",
                        true)
        );

        JobConf jobConf = new JobConf();
        System.out.println("============ WARM UP ============");
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            benchmark(jobConf, benchmarkFile, 5);
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
        System.out.println();
        System.out.println(benchmarkFile.getName());

        Object value = null;

        long start;

        //
        // string
        //
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
        else if (benchmarkFile.getDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadStringColumnarText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadStringColumnarBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadStringOrc(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getDeserializer().getClass().getName());
        }
        logDuration("p_string", start, loopCount, value);

        //
        // smallint
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadSmallint(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
        }
        logDuration("smallint", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadSmallintText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadSmallintColumnarText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadSmallintColumnarBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadSmallintOrc(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getDeserializer().getClass().getName());
        }
        logDuration("p_smallint", start, loopCount, value);


        //
        // int
        //
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
        else if (benchmarkFile.getDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadIntColumnarText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadIntColumnarBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadIntOrc(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        logDuration("p_int", start, loopCount, value);


        //
        // bigint
        //
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
        else if (benchmarkFile.getDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBigintColumnarText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBigintColumnarBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBigintOrc(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getDeserializer().getClass().getName());
        }
        logDuration("p_bigint", start, loopCount, value);


        //
        // float
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadFloat(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
        }
        logDuration("float", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadFloatText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadFloatColumnarText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadFloatColumnarBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadFloatOrc(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getDeserializer().getClass().getName());
        }
        logDuration("p_float", start, loopCount, value);


        //
        // double
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadDouble(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
        }
        logDuration("double", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadDoubleText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadDoubleColumnarText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadDoubleColumnarBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadDoubleOrc(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getDeserializer().getClass().getName());
        }
        logDuration("p_double", start, loopCount, value);


        //
        // boolean
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadBoolean(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
        }
        logDuration("boolean", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBooleanText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBooleanColumnarText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBooleanColumnarBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBooleanOrc(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getDeserializer().getClass().getName());
        }
        logDuration("p_boolean", start, loopCount, value);



        //
        // binary
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
        }
        logDuration("binary", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBinaryText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBinaryColumnarText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBinaryColumnarBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBinaryOrc(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getDeserializer().getClass().getName());
        }
        logDuration("p_binary", start, loopCount, value);



        //
        // three columns
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkRead3Columns(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
        }
        logDuration("three", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkRead3ColumnsText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkRead3ColumnsColumnarText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkRead3ColumnsColumnarBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkRead3ColumnsOrc(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getDeserializer().getClass().getName());
        }
        logDuration("p_three", start, loopCount, value);



        //
        // all columns
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadAllColumns(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
        }
        logDuration("all", start, loopCount, value);
        start = System.nanoTime();
        if (benchmarkFile.getDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadAllColumnsText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadAllColumnsColumnarText(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadAllColumnsColumnarBinary(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else if (benchmarkFile.getDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadAllColumnsOrc(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            }
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getDeserializer().getClass().getName());
        }
        logDuration("p_all", start, loopCount, value);
    }

    private static void logDuration(String label, long start, int loopCount, Object value)
    {
        long end = System.nanoTime();
        long nanos = end - start;
        Duration duration = new Duration(1.0 * nanos / loopCount, NANOSECONDS).convertTo(SECONDS);
        System.out.printf("%10s %6s %s\n", label, duration, value);
    }

    private static <K, V extends Writable> List<Object> benchmarkReadAllColumns(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int smallintFieldIndex = allStructFieldRefs.indexOf(smallintField);
        PrimitiveObjectInspector smallintFieldInspector = (PrimitiveObjectInspector) smallintField.getFieldObjectInspector();

        StructField intField = rowInspector.getStructFieldRef("t_int");
        int intFieldIndex = allStructFieldRefs.indexOf(intField);
        PrimitiveObjectInspector intFieldInspector = (PrimitiveObjectInspector) intField.getFieldObjectInspector();

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);
        PrimitiveObjectInspector bigintFieldInspector = (PrimitiveObjectInspector) bigintField.getFieldObjectInspector();

        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int floatFieldIndex = allStructFieldRefs.indexOf(floatField);
        PrimitiveObjectInspector floatFieldInspector = (PrimitiveObjectInspector) floatField.getFieldObjectInspector();

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);
        PrimitiveObjectInspector doubleFieldInspector = (PrimitiveObjectInspector) doubleField.getFieldObjectInspector();

        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int booleanFieldIndex = allStructFieldRefs.indexOf(booleanField);
        PrimitiveObjectInspector booleanFieldInspector = (PrimitiveObjectInspector) booleanField.getFieldObjectInspector();

        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int binaryFieldIndex = allStructFieldRefs.indexOf(binaryField);
        PrimitiveObjectInspector binaryFieldInspector = (PrimitiveObjectInspector) binaryField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(
                stringFieldIndex,
                smallintFieldIndex,
                intFieldIndex,
                bigintFieldIndex,
                floatFieldIndex,
                doubleFieldIndex,
                booleanFieldIndex,
                binaryFieldIndex));

        long stringLengthSum = 0;
        long smallintSum = 0;
        long intSum = 0;
        long bigintSum = 0;
        double floatSum = 0;
        double doubleSum = 0;
        long booleanSum = 0;
        long binaryLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            smallintSum = 0;
            intSum = 0;
            bigintSum = 0;
            floatSum = 0;
            doubleSum = 0;
            booleanSum = 0;
            binaryLengthSum = 0;

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
                    booleanSum += booleanValue ? 1 : 2;
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
        return ImmutableList.<Object>of(stringLengthSum, smallintSum, intSum, bigintSum, floatSum, doubleSum, booleanSum, binaryLengthSum);
    }

    private static <K, V extends Writable> List<Object> benchmarkReadAllColumnsText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);

        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int smallintFieldIndex = allStructFieldRefs.indexOf(smallintField);

        StructField intField = rowInspector.getStructFieldRef("t_int");
        int intFieldIndex = allStructFieldRefs.indexOf(intField);

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int floatFieldIndex = allStructFieldRefs.indexOf(floatField);

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);

        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int booleanFieldIndex = allStructFieldRefs.indexOf(booleanField);

        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int binaryFieldIndex = allStructFieldRefs.indexOf(binaryField);

        int[] startPosition = new int[13];

        long stringSum = 0;
        long smallintSum = 0;
        long intSum = 0;
        long bigintSum = 0;
        double floatSum = 0;
        double doubleSum = 0;
        long booleanSum = 0;
        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            smallintSum = 0;
            intSum = 0;
            bigintSum = 0;
            floatSum = 0;
            doubleSum = 0;
            booleanSum = 0;
            binarySum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int stringStart = startPosition[stringFieldIndex];
                int stringLength = startPosition[stringFieldIndex + 1] - stringStart - 1;
                if (!isNull(bytes, stringStart, stringLength)) {
                    byte[] stringValue = Arrays.copyOfRange(bytes, stringStart, stringStart + stringLength);
                    stringSum += stringValue.length;
                }

                int smallintStart = startPosition[smallintFieldIndex];
                int smallintLength = startPosition[smallintFieldIndex + 1] - smallintStart - 1;
                if (!isNull(bytes, smallintStart, smallintLength)) {
                    long smallintValue = NumberParser.parseLong(bytes, smallintStart, smallintLength);
                    smallintSum += smallintValue;
                }

                int intStart = startPosition[intFieldIndex];
                int intLength = startPosition[intFieldIndex + 1] - intStart - 1;
                if (!isNull(bytes, intStart, intLength)) {
                    long intValue = NumberParser.parseLong(bytes, intStart, intLength);
                    intSum += intValue;
                }

                int bigintStart = startPosition[bigintFieldIndex];
                int bigintLength = startPosition[bigintFieldIndex + 1] - bigintStart - 1;
                if (!isNull(bytes, bigintStart, bigintLength)) {
                    long bigintValue = NumberParser.parseLong(bytes, bigintStart, bigintLength);
                    bigintSum += bigintValue;
                }

                int floatStart = startPosition[floatFieldIndex];
                int floatLength = startPosition[floatFieldIndex + 1] - floatStart - 1;
                if (!isNull(bytes, floatStart, floatLength)) {
                    float floatValue = parseFloat(bytes, floatStart, floatLength);
                    floatSum += floatValue;
                }

                int doubleStart = startPosition[doubleFieldIndex];
                int doubleLength = startPosition[doubleFieldIndex + 1] - doubleStart - 1;
                if (!isNull(bytes, doubleStart, doubleLength)) {
                    double doubleValue = NumberParser.parseDouble(bytes, doubleStart, doubleLength);
                    doubleSum += doubleValue;
                }

                int booleanStart = startPosition[booleanFieldIndex];
                int booleanLength = startPosition[booleanFieldIndex + 1] - booleanStart - 1;
                if (isTrue(bytes, booleanStart, booleanLength)) {
                    booleanSum += 1;
                }
                else if (isFalse(bytes, booleanStart, booleanLength)) {
                    booleanSum += 2;
                }
                else {
                    // null
                }

                int binaryStart = startPosition[binaryFieldIndex];
                int binaryLength = startPosition[binaryFieldIndex + 1] - binaryStart - 1;
                if (!isNull(bytes, binaryStart, binaryLength)) {
                    byte[] binaryValue = Arrays.copyOfRange(bytes, binaryStart, binaryStart + binaryLength);
                    binaryValue = Base64.decodeBase64(binaryValue);
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, smallintSum, intSum, bigintSum, floatSum, doubleSum, booleanSum, binarySum);
    }

    private static <K, V extends Writable> List<Object> benchmarkReadAllColumnsColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);

        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int smallintFieldIndex = allStructFieldRefs.indexOf(smallintField);

        StructField intField = rowInspector.getStructFieldRef("t_int");
        int intFieldIndex = allStructFieldRefs.indexOf(intField);

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int floatFieldIndex = allStructFieldRefs.indexOf(floatField);

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);

        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int booleanFieldIndex = allStructFieldRefs.indexOf(booleanField);

        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int binaryFieldIndex = allStructFieldRefs.indexOf(binaryField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(
                stringFieldIndex,
                smallintFieldIndex,
                intFieldIndex,
                bigintFieldIndex,
                floatFieldIndex,
                doubleFieldIndex,
                booleanFieldIndex,
                binaryFieldIndex));

        long stringSum = 0;
        long smallintSum = 0;
        long intSum = 0;
        long bigintSum = 0;
        double floatSum = 0;
        double doubleSum = 0;
        long booleanSum = 0;
        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            smallintSum = 0;
            intSum = 0;
            bigintSum = 0;
            floatSum = 0;
            doubleSum = 0;
            booleanSum = 0;
            binarySum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;

                BytesRefWritable stringBytesRefWritable = row.unCheckedGet(stringFieldIndex);
                byte[] stringBytes = stringBytesRefWritable.getData();
                int stringStart = stringBytesRefWritable.getStart();
                int stringLength = stringBytesRefWritable.getLength();
                if (!isNull(stringBytes, stringStart, stringLength)) {
                    byte[] stringValue = Arrays.copyOfRange(stringBytes, stringStart, stringStart + stringLength);
                    stringSum += stringValue.length;
                }

                BytesRefWritable smallintBytesRefWritable = row.unCheckedGet(smallintFieldIndex);
                byte[] smallintBytes = smallintBytesRefWritable.getData();
                int smallintStart = smallintBytesRefWritable.getStart();
                int smallintLength = smallintBytesRefWritable.getLength();
                if (!isNull(smallintBytes, smallintStart, smallintLength)) {
                    long smallintValue = NumberParser.parseLong(smallintBytes, smallintStart, smallintLength);
                    smallintSum += smallintValue;
                }

                BytesRefWritable intBytesRefWritable = row.unCheckedGet(intFieldIndex);
                byte[] intBytes = intBytesRefWritable.getData();
                int intStart = intBytesRefWritable.getStart();
                int intLength = intBytesRefWritable.getLength();
                if (!isNull(intBytes, intStart, intLength)) {
                    long intValue = NumberParser.parseLong(intBytes, intStart, intLength);
                    intSum += intValue;
                }

                BytesRefWritable bigintBytesRefWritable = row.unCheckedGet(bigintFieldIndex);
                byte[] bigintBytes = bigintBytesRefWritable.getData();
                int bigintStart = bigintBytesRefWritable.getStart();
                int bigintLength = bigintBytesRefWritable.getLength();
                if (!isNull(bigintBytes, bigintStart, bigintLength)) {
                    long bigintValue = NumberParser.parseLong(bigintBytes, bigintStart, bigintLength);
                    bigintSum += bigintValue;
                }

                BytesRefWritable floatBytesRefWritable = row.unCheckedGet(floatFieldIndex);
                byte[] floatBytes = floatBytesRefWritable.getData();
                int floatStart = floatBytesRefWritable.getStart();
                int floatLength = floatBytesRefWritable.getLength();
                if (!isNull(floatBytes, floatStart, floatLength)) {
                    float floatValue = parseFloat(floatBytes, floatStart, floatLength);
                    floatSum += floatValue;
                }

                BytesRefWritable doubleBytesRefWritable = row.unCheckedGet(doubleFieldIndex);
                byte[] doubleBytes = doubleBytesRefWritable.getData();
                int doubleStart = doubleBytesRefWritable.getStart();
                int doubleLength = doubleBytesRefWritable.getLength();
                if (!isNull(doubleBytes, doubleStart, doubleLength)) {
                    double doubleValue = NumberParser.parseDouble(doubleBytes, doubleStart, doubleLength);
                    doubleSum += doubleValue;
                }

                BytesRefWritable booleanBytesRefWritable = row.unCheckedGet(booleanFieldIndex);
                byte[] booleanBytes = booleanBytesRefWritable.getData();
                int booleanStart = booleanBytesRefWritable.getStart();
                int booleanLength = booleanBytesRefWritable.getLength();
                if (isTrue(booleanBytes, booleanStart, booleanLength)) {
                    booleanSum += 1;
                }
                else if (isFalse(booleanBytes, booleanStart, booleanLength)) {
                    booleanSum += 2;
                }
                else {
                    // null
                }

                BytesRefWritable binaryBytesRefWritable = row.unCheckedGet(binaryFieldIndex);
                byte[] binaryBytes = binaryBytesRefWritable.getData();
                int binaryStart = binaryBytesRefWritable.getStart();
                int binaryLength = binaryBytesRefWritable.getLength();
                if (!isNull(binaryBytes, binaryStart, binaryLength)) {
                    byte[] binaryValue = Arrays.copyOfRange(binaryBytes, binaryStart, binaryStart + binaryLength);
                    binaryValue = Base64.decodeBase64(binaryValue);
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, smallintSum, intSum, bigintSum, floatSum, doubleSum, booleanSum, binarySum);
    }

    private static <K, V extends Writable> List<Object> benchmarkReadAllColumnsColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);

        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int smallintFieldIndex = allStructFieldRefs.indexOf(smallintField);

        StructField intField = rowInspector.getStructFieldRef("t_int");
        int intFieldIndex = allStructFieldRefs.indexOf(intField);

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int floatFieldIndex = allStructFieldRefs.indexOf(floatField);

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);

        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int booleanFieldIndex = allStructFieldRefs.indexOf(booleanField);

        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int binaryFieldIndex = allStructFieldRefs.indexOf(binaryField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(
                stringFieldIndex,
                smallintFieldIndex,
                intFieldIndex,
                bigintFieldIndex,
                floatFieldIndex,
                doubleFieldIndex,
                booleanFieldIndex,
                binaryFieldIndex));

        long stringSum = 0;
        long smallintSum = 0;
        long intSum = 0;
        long bigintSum = 0;
        double floatSum = 0;
        double doubleSum = 0;
        long booleanSum = 0;
        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            smallintSum = 0;
            intSum = 0;
            bigintSum = 0;
            floatSum = 0;
            doubleSum = 0;
            booleanSum = 0;
            binarySum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;

                BytesRefWritable stringBytesRefWritable = row.unCheckedGet(stringFieldIndex);
                byte[] stringBytes = stringBytesRefWritable.getData();
                int stringStart = stringBytesRefWritable.getStart();
                int stringLength = stringBytesRefWritable.getLength();
                // todo how are string nulls encoded in binary
                if (!isNull(stringBytes, stringStart, stringLength)) {
                    byte[] stringValue = Arrays.copyOfRange(stringBytes, stringStart, stringStart + stringLength);
                    stringSum += stringValue.length;
                }

                BytesRefWritable smallintBytesRefWritable = row.unCheckedGet(smallintFieldIndex);
                byte[] smallintBytes = smallintBytesRefWritable.getData();
                int smallintStart = smallintBytesRefWritable.getStart();
                int smallintLength = smallintBytesRefWritable.getLength();
                if (smallintLength != 0) {
                    short smallintValue = unsafe.getShort(smallintBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + smallintStart);
                    smallintValue = Short.reverseBytes(smallintValue);
                    smallintSum += smallintValue;
                }

                BytesRefWritable intBytesRefWritable = row.unCheckedGet(intFieldIndex);
                byte[] intBytes = intBytesRefWritable.getData();
                int intStart = intBytesRefWritable.getStart();
                int intLength = intBytesRefWritable.getLength();
                if (intLength != 0) {
                    int intValue = readVInt(intBytes, intStart, intLength);
                    intSum += intValue;
                }

                BytesRefWritable bigintBytesRefWritable = row.unCheckedGet(bigintFieldIndex);
                byte[] bigintBytes = bigintBytesRefWritable.getData();
                int bigintStart = bigintBytesRefWritable.getStart();
                int bigintLength = bigintBytesRefWritable.getLength();
                if (bigintLength != 0) {
                    long bigintValue = readVBigint(bigintBytes, bigintStart, bigintLength);
                    bigintSum += bigintValue;
                }

                BytesRefWritable floatBytesRefWritable = row.unCheckedGet(floatFieldIndex);
                byte[] floatBytes = floatBytesRefWritable.getData();
                int floatStart = floatBytesRefWritable.getStart();
                int floatLength = floatBytesRefWritable.getLength();
                if (floatLength != 0) {
                    int intBits = unsafe.getInt(floatBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + floatStart);
                    float floatValue = Float.intBitsToFloat(Integer.reverseBytes(intBits));
                    floatSum += floatValue;
                }

                BytesRefWritable doubleBytesRefWritable = row.unCheckedGet(doubleFieldIndex);
                byte[] doubleBytes = doubleBytesRefWritable.getData();
                int doubleStart = doubleBytesRefWritable.getStart();
                int doubleLength = doubleBytesRefWritable.getLength();
                if (doubleLength != 0) {
                    long longBits = unsafe.getLong(doubleBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + doubleStart);
                    double doubleValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                    doubleSum += doubleValue;
                }

                BytesRefWritable booleanBytesRefWritable = row.unCheckedGet(booleanFieldIndex);
                byte[] booleanBytes = booleanBytesRefWritable.getData();
                int booleanStart = booleanBytesRefWritable.getStart();
                int booleanLength = booleanBytesRefWritable.getLength();
                if (booleanLength != 0) {
                    byte val = booleanBytes[booleanStart];
                    booleanSum += val != 0 ? 1 : 2;
                }

                BytesRefWritable binaryBytesRefWritable = row.unCheckedGet(binaryFieldIndex);
                byte[] binaryBytes = binaryBytesRefWritable.getData();
                int binaryStart = binaryBytesRefWritable.getStart();
                int binaryLength = binaryBytesRefWritable.getLength();
                if (!isNull(binaryBytes, binaryStart, binaryLength)) {
                    byte[] binaryValue = Arrays.copyOfRange(binaryBytes, binaryStart, binaryStart + binaryLength);
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, smallintSum, intSum, bigintSum, floatSum, doubleSum, booleanSum, binarySum);
    }

    private static <K, V extends Writable> List<Object> benchmarkReadAllColumnsOrc(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);

        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int smallintFieldIndex = allStructFieldRefs.indexOf(smallintField);

        StructField intField = rowInspector.getStructFieldRef("t_int");
        int intFieldIndex = allStructFieldRefs.indexOf(intField);

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int floatFieldIndex = allStructFieldRefs.indexOf(floatField);

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);

        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int booleanFieldIndex = allStructFieldRefs.indexOf(booleanField);

        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int binaryFieldIndex = allStructFieldRefs.indexOf(binaryField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(
                stringFieldIndex,
                smallintFieldIndex,
                intFieldIndex,
                bigintFieldIndex,
                floatFieldIndex,
                doubleFieldIndex,
                booleanFieldIndex,
                binaryFieldIndex));

        long stringSum = 0;
        long smallintSum = 0;
        long intSum = 0;
        long bigintSum = 0;
        double floatSum = 0;
        double doubleSum = 0;
        long booleanSum = 0;
        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            smallintSum = 0;
            intSum = 0;
            bigintSum = 0;
            floatSum = 0;
            doubleSum = 0;
            booleanSum = 0;
            binarySum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject stringLazyObject = row.getFieldValue(stringFieldIndex);
                Text text = (Text) stringLazyObject.materialize();
                if (text != null) {
                    byte[] stringValue = Arrays.copyOfRange(text.getBytes(), 0, text.getLength());
                    stringSum += stringValue.length;
                }

                OrcLazyObject smallintLazyObject = row.getFieldValue(smallintFieldIndex);
                ShortWritable smallintValue = (ShortWritable) smallintLazyObject.materialize();
                if (smallintValue != null) {
                    smallintSum += smallintValue.get();
                }

                OrcLazyObject intLazyObject = row.getFieldValue(intFieldIndex);
                IntWritable intValue = (IntWritable) intLazyObject.materialize();
                if (intValue != null) {
                    intSum += intValue.get();
                }

                OrcLazyObject bigintLazyObject = row.getFieldValue(bigintFieldIndex);
                LongWritable bigintValue = (LongWritable) bigintLazyObject.materialize();
                if (bigintValue != null) {
                    bigintSum += bigintValue.get();
                }

                OrcLazyObject floatLazyObject = row.getFieldValue(floatFieldIndex);
                FloatWritable floatValue = (FloatWritable) floatLazyObject.materialize();
                if (floatValue != null) {
                    floatSum += floatValue.get();
                }

                OrcLazyObject doubleLazyObject = row.getFieldValue(doubleFieldIndex);
                DoubleWritable doubleValue = (DoubleWritable) doubleLazyObject.materialize();
                if (doubleValue != null) {
                    doubleSum += doubleValue.get();
                }

                OrcLazyObject booleanLazyObject = row.getFieldValue(booleanFieldIndex);
                BooleanWritable booleanValue = (BooleanWritable) booleanLazyObject.materialize();
                if (booleanValue != null) {
                    booleanSum += booleanValue.get() ? 1 : 2;
                }

                OrcLazyObject binaryLazyObject = row.getFieldValue(binaryFieldIndex);
                BytesWritable bytesWritable = (BytesWritable) binaryLazyObject.materialize();
                if (bytesWritable != null) {
                    byte[] binaryValue = Arrays.copyOfRange(bytesWritable.getBytes(), 0, bytesWritable.getLength());
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, smallintSum, intSum, bigintSum, floatSum, doubleSum, booleanSum, binarySum);
    }

    private static <K, V extends Writable> List<Object> benchmarkRead3Columns(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);
        PrimitiveObjectInspector doubleFieldInspector = (PrimitiveObjectInspector) doubleField.getFieldObjectInspector();

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);
        PrimitiveObjectInspector bigintFieldInspector = (PrimitiveObjectInspector) bigintField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(stringFieldIndex, doubleFieldIndex, bigintFieldIndex));

        long stringSum = 0;
        double doubleSum = 0;
        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            doubleSum = 0;
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringSum += stringValue.length();
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
        return ImmutableList.<Object>of(stringSum, doubleSum, bigintSum);
    }

    private static <K, V extends Writable> List<Object> benchmarkRead3ColumnsText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        int[] startPosition = new int[13];

        long stringSum = 0;
        double doubleSum = 0;
        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            doubleSum = 0;
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int stringStart = startPosition[stringFieldIndex];
                int stringLength = startPosition[stringFieldIndex + 1] - stringStart - 1;
                if (!isNull(bytes, stringStart, stringLength)) {
                    byte[] stringValue = Arrays.copyOfRange(bytes, stringStart, stringStart + stringLength);
                    stringSum += stringValue.length;
                }

                int doubleStart = startPosition[doubleFieldIndex];
                int doubleLength = startPosition[doubleFieldIndex + 1] - doubleStart - 1;
                if (!isNull(bytes, doubleStart, doubleLength)) {
                    double doubleValue = NumberParser.parseDouble(bytes, doubleStart, doubleLength);
                    doubleSum += doubleValue;
                }

                int bigintStart = startPosition[bigintFieldIndex];
                int bigintLength = startPosition[bigintFieldIndex + 1] - bigintStart - 1;
                if (!isNull(bytes, bigintStart, bigintLength)) {
                    long bigintValue = NumberParser.parseLong(bytes, bigintStart, bigintLength);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, doubleSum, bigintSum);
    }

    private static <K, V extends Writable> List<Object> benchmarkRead3ColumnsColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);
        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(stringFieldIndex, doubleFieldIndex, bigintFieldIndex));

        long stringSum = 0;
        double doubleSum = 0;
        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            doubleSum = 0;
            bigintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;

                BytesRefWritable stringBytesRefWritable = row.unCheckedGet(stringFieldIndex);
                byte[] stringBytes = stringBytesRefWritable.getData();
                int stringStart = stringBytesRefWritable.getStart();
                int stringLength = stringBytesRefWritable.getLength();
                if (!isNull(stringBytes, stringStart, stringLength)) {
                    byte[] stringValue = Arrays.copyOfRange(stringBytes, stringStart, stringStart + stringLength);
                    stringSum += stringValue.length;
                }

                BytesRefWritable doubleBytesRefWritable = row.unCheckedGet(doubleFieldIndex);
                byte[] doubleBytes = doubleBytesRefWritable.getData();
                int doubleStart = doubleBytesRefWritable.getStart();
                int doubleLength = doubleBytesRefWritable.getLength();
                if (!isNull(doubleBytes, doubleStart, doubleLength)) {
                    double doubleValue = NumberParser.parseDouble(doubleBytes, doubleStart, doubleLength);
                    doubleSum += doubleValue;
                }

                BytesRefWritable bigintBytesRefWritable = row.unCheckedGet(bigintFieldIndex);
                byte[] bigintBytes = bigintBytesRefWritable.getData();
                int bigintStart = bigintBytesRefWritable.getStart();
                int bigintLength = bigintBytesRefWritable.getLength();
                if (!isNull(bigintBytes, bigintStart, bigintLength)) {
                    long bigintValue = NumberParser.parseLong(bigintBytes, bigintStart, bigintLength);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, doubleSum, bigintSum);
    }

    private static <K, V extends Writable> List<Object> benchmarkRead3ColumnsColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);
        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(stringFieldIndex, doubleFieldIndex, bigintFieldIndex));

        long stringSum = 0;
        double doubleSum = 0;
        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            doubleSum = 0;
            bigintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;

                BytesRefWritable stringBytesRefWritable = row.unCheckedGet(stringFieldIndex);
                byte[] stringBytes = stringBytesRefWritable.getData();
                int stringStart = stringBytesRefWritable.getStart();
                int stringLength = stringBytesRefWritable.getLength();
                // todo how are string nulls encoded in binary
                if (!isNull(stringBytes, stringStart, stringLength)) {
                    byte[] stringValue = Arrays.copyOfRange(stringBytes, stringStart, stringStart + stringLength);
                    stringSum += stringValue.length;
                }

                BytesRefWritable doubleBytesRefWritable = row.unCheckedGet(doubleFieldIndex);
                byte[] doubleBytes = doubleBytesRefWritable.getData();
                int doubleStart = doubleBytesRefWritable.getStart();
                int doubleLength = doubleBytesRefWritable.getLength();
                if (doubleLength != 0) {
                    long longBits = unsafe.getLong(doubleBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + doubleStart);
                    double doubleValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                    doubleSum += doubleValue;
                }

                BytesRefWritable bigintBytesRefWritable = row.unCheckedGet(bigintFieldIndex);
                byte[] bigintBytes = bigintBytesRefWritable.getData();
                int bigintStart = bigintBytesRefWritable.getStart();
                int bigintLength = bigintBytesRefWritable.getLength();
                if (bigintLength != 0) {
                    long bigintValue = readVBigint(bigintBytes, bigintStart, bigintLength);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, doubleSum, bigintSum);
    }

    private static <K, V extends Writable> List<Object> benchmarkRead3ColumnsOrc(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);
        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(stringFieldIndex, doubleFieldIndex, bigintFieldIndex));

        long stringSum = 0;
        double doubleSum = 0;
        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            doubleSum = 0;
            bigintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject stringLazyObject = row.getFieldValue(stringFieldIndex);
                Text text = (Text) stringLazyObject.materialize();
                if (text != null) {
                    byte[] stringValue = Arrays.copyOfRange(text.getBytes(), 0, text.getLength());
                    stringSum += stringValue.length;
                }

                OrcLazyObject bigintLazyObject = row.getFieldValue(bigintFieldIndex);
                LongWritable bigintValue = (LongWritable) bigintLazyObject.materialize();
                if (bigintValue != null) {
                    bigintSum += bigintValue.get();
                }

                OrcLazyObject doubleLazyObject = row.getFieldValue(doubleFieldIndex);
                DoubleWritable doubleValue = (DoubleWritable) doubleLazyObject.materialize();
                if (doubleValue != null) {
                    doubleSum += doubleValue.get();
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, doubleSum, bigintSum);
    }

    private static <K, V extends Writable> long benchmarkReadString(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

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

    private static <K, V extends Writable> long benchmarkReadStringOrc(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                Text text = (Text) orcLazyObject.materialize();
                if (text != null) {
                    byte[] stringValue = Arrays.copyOfRange(text.getBytes(), 0, text.getLength());
                    stringSum += stringValue.length;
                }
            }
            recordReader.close();
        }
        return stringSum;
    }

    private static <K, V extends Writable> long benchmarkReadSmallint(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int fieldIndex = allStructFieldRefs.indexOf(smallintField);
        PrimitiveObjectInspector smallintFieldInspector = (PrimitiveObjectInspector) smallintField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long smallintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            smallintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

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
        return smallintSum;
    }

    private static <K, V extends Writable> long benchmarkReadSmallintText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int fieldIndex = allStructFieldRefs.indexOf(smallintField);

        int[] startPosition = new int[13];

        long smallintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            smallintSum = 0;
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
                    long smallintValue = NumberParser.parseLong(bytes, start, length);
                    smallintSum += smallintValue;
                }
            }
            recordReader.close();
        }
        return smallintSum;
    }

    private static <K, V extends Writable> long benchmarkReadSmallintColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int fieldIndex = allStructFieldRefs.indexOf(smallintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long smallintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            smallintSum = 0;

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
                    long smallintValue = NumberParser.parseLong(bytes, start, length);
                    smallintSum += smallintValue;
                }
            }
            recordReader.close();
        }
        return smallintSum;
    }

    private static <K, V extends Writable> long benchmarkReadSmallintColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int fieldIndex = allStructFieldRefs.indexOf(smallintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long smallintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            smallintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (length != 0) {
                    short smallintValue = unsafe.getShort(bytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + start);
                    smallintValue = Short.reverseBytes(smallintValue);
                    smallintSum += smallintValue;
                }
            }
            recordReader.close();
        }
        return smallintSum;
    }

    private static <K, V extends Writable> long benchmarkReadSmallintOrc(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int fieldIndex = allStructFieldRefs.indexOf(smallintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long smallintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            smallintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                ShortWritable smallintValue = (ShortWritable) orcLazyObject.materialize();
                if (smallintValue != null) {
                    smallintSum += smallintValue.get();
                }
            }
            recordReader.close();
        }
        return smallintSum;
    }

    private static <K, V extends Writable> long benchmarkReadInt(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField intField = rowInspector.getStructFieldRef("t_int");
        int fieldIndex = allStructFieldRefs.indexOf(intField);
        PrimitiveObjectInspector intFieldInspector = (PrimitiveObjectInspector) intField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

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

                if (length != 0) {
                    int intValue = readVInt(bytes, start, length);
                    intSum += intValue;
                }
            }
            recordReader.close();
        }
        return intSum;
    }

    public static int readVInt(byte[] bytes, int offset, int length)
    {
        if (length == 1) {
            return bytes[offset];
        }

        int i = 0;
        for (int idx = 0; idx < length - 1; idx++) {
            byte b = bytes[offset + 1 + idx];
            i = i << 8;
            i = i | (b & 0xFF);
        }
        return WritableUtils.isNegativeVInt(bytes[offset]) ? ~i : i;
    }

    private static <K, V extends Writable> long benchmarkReadIntOrc(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField intField = rowInspector.getStructFieldRef("t_int");
        int fieldIndex = allStructFieldRefs.indexOf(intField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long intSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            intSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                IntWritable intValue = (IntWritable) orcLazyObject.materialize();
                if (intValue != null) {
                    intSum += intValue.get();
                }
            }
            recordReader.close();
        }
        return intSum;
    }

    private static <K, V extends Writable> long benchmarkReadBigint(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);
        PrimitiveObjectInspector bigintFieldInspector = (PrimitiveObjectInspector) bigintField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

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

                if (length != 0) {
                    long bigintValue = readVBigint(bytes, start, length);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }
    public static long readVBigint(byte[] bytes, int offset, int length)
    {
        if (length == 1) {
            return bytes[offset];
        }

        long i = 0;
        for (int idx = 0; idx < length - 1; idx++) {
            byte b = bytes[offset + 1 + idx];
            i = i << 8;
            i = i | (b & 0xFF);
        }
        return WritableUtils.isNegativeVInt(bytes[offset]) ? ~i : i;
    }

    private static <K, V extends Writable> long benchmarkReadBigintOrc(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                LongWritable bigintValue = (LongWritable) orcLazyObject.materialize();
                if (bigintValue != null) {
                    bigintSum += bigintValue.get();
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    private static <K, V extends Writable> double benchmarkReadFloat(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int fieldIndex = allStructFieldRefs.indexOf(floatField);
        PrimitiveObjectInspector floatFieldInspector = (PrimitiveObjectInspector) floatField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double floatSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            floatSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


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
        return floatSum;
    }

    private static <K, V extends Writable> double benchmarkReadFloatText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int fieldIndex = allStructFieldRefs.indexOf(floatField);

        int[] startPosition = new int[13];

        double floatSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            floatSum = 0;
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
                    float floatValue = parseFloat(bytes, start, length);
                    floatSum += floatValue;
                }
            }
            recordReader.close();
        }
        return floatSum;
    }

    private static <K, V extends Writable> double benchmarkReadFloatColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int fieldIndex = allStructFieldRefs.indexOf(floatField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double floatSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            floatSum = 0;

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
                    float floatValue = parseFloat(bytes, start, length);
                    floatSum += floatValue;
                }
            }
            recordReader.close();
        }
        return floatSum;
    }

    public static float parseFloat(byte[] bytes, int start, int length)
    {
        char[] chars = new char[length];
        for (int pos = 0; pos < length; pos++) {
            chars[pos] = (char) bytes[start + pos];
        }
        String string = new String(chars);
        return Float.parseFloat(string);
    }

    private static <K, V extends Writable> double benchmarkReadFloatColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int fieldIndex = allStructFieldRefs.indexOf(floatField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double floatSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            floatSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (length != 0) {
                    int intBits = unsafe.getInt(bytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + start);
                    float floatValue = Float.intBitsToFloat(Integer.reverseBytes(intBits));
                    floatSum += floatValue;
                }
            }
            recordReader.close();
        }
        return floatSum;
    }

    private static <K, V extends Writable> double benchmarkReadFloatOrc(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int fieldIndex = allStructFieldRefs.indexOf(floatField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double floatSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            floatSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                FloatWritable floatValue = (FloatWritable) orcLazyObject.materialize();
                if (floatValue != null) {
                    floatSum += floatValue.get();
                }
            }
            recordReader.close();
        }
        return floatSum;
    }

    private static <K, V extends Writable> double benchmarkReadDouble(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);
        PrimitiveObjectInspector doubleFieldInspector = (PrimitiveObjectInspector) doubleField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

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

        double doubleSum = 0;
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

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

                if (length != 0) {
                    long longBits = unsafe.getLong(bytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + start);
                    double doubleValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                    doubleSum += doubleValue;
                }
            }
            recordReader.close();
        }
        return doubleSum;
    }

    private static <K, V extends Writable> double benchmarkReadDoubleOrc(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                DoubleWritable doubleValue = (DoubleWritable) orcLazyObject.materialize();
                if (doubleValue != null) {
                    doubleSum += doubleValue.get();
                }
            }
            recordReader.close();
        }
        return doubleSum;
    }

    private static <K, V extends Writable> long benchmarkReadBoolean(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int fieldIndex = allStructFieldRefs.indexOf(booleanField);
        PrimitiveObjectInspector booleanFieldInspector = (PrimitiveObjectInspector) booleanField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long booleanSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            booleanSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object booleanData = rowInspector.getStructFieldData(rowData, booleanField);
                if (booleanData != null) {
                    Object booleanPrimitive = booleanFieldInspector.getPrimitiveJavaObject(booleanData);
                    boolean booleanValue = ((Boolean) booleanPrimitive);
                    booleanSum += booleanValue ? 1 : 2;
                }
            }
            recordReader.close();
        }
        return booleanSum;
    }

    private static <K, V extends Writable> long benchmarkReadBooleanText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int fieldIndex = allStructFieldRefs.indexOf(booleanField);

        int[] startPosition = new int[13];

        long booleanSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            booleanSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int start = startPosition[fieldIndex];
                int length = startPosition[fieldIndex + 1] - start - 1;

                if (isTrue(bytes, start, length)) {
                    booleanSum += 1;
                }
                else if (isFalse(bytes, start, length)) {
                    booleanSum += 2;
                }
                else {
                    // null
                }
            }
            recordReader.close();
        }
        return booleanSum;
    }

    private static <K, V extends Writable> long benchmarkReadBooleanColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int fieldIndex = allStructFieldRefs.indexOf(booleanField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long booleanSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            booleanSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (isTrue(bytes, start, length)) {
                    booleanSum += 1;
                }
                else if (isFalse(bytes, start, length)) {
                    booleanSum += 2;
                }
                else {
                    // null
                }
            }
            recordReader.close();
        }
        return booleanSum;
    }

    private static <K, V extends Writable> long benchmarkReadBooleanColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int fieldIndex = allStructFieldRefs.indexOf(booleanField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long booleanSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            booleanSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (length != 0) {
                    byte val = bytes[start];
                    booleanSum += val != 0 ? 1 : 2;
                }
            }
            recordReader.close();
        }
        return booleanSum;
    }

    private static <K, V extends Writable> long benchmarkReadBooleanOrc(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int fieldIndex = allStructFieldRefs.indexOf(booleanField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long booleanSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            booleanSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                BooleanWritable booleanValue = (BooleanWritable) orcLazyObject.materialize();
                if (booleanValue != null) {
                    booleanSum += booleanValue.get() ? 1 : 2;
                }
            }
            recordReader.close();
        }
        return booleanSum;
    }

    private static <K, V extends Writable> long benchmarkReadBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int fieldIndex = allStructFieldRefs.indexOf(binaryField);
        PrimitiveObjectInspector binaryFieldInspector = (PrimitiveObjectInspector) binaryField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long binaryLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            binaryLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


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
        return binaryLengthSum;
    }

    private static <K, V extends Writable> long benchmarkReadBinaryText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int fieldIndex = allStructFieldRefs.indexOf(binaryField);

        int[] startPosition = new int[13];

        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            binarySum = 0;
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
                    byte[] binaryValue = Arrays.copyOfRange(bytes, start, start + length);
                    binaryValue = Base64.decodeBase64(binaryValue);
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return binarySum;
    }

    private static <K, V extends Writable> long benchmarkReadBinaryColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int fieldIndex = allStructFieldRefs.indexOf(binaryField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            binarySum = 0;

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
                    byte[] binaryValue = Arrays.copyOfRange(bytes, start, start + length);
                    binaryValue = Base64.decodeBase64(binaryValue);
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return binarySum;
    }

    private static <K, V extends Writable> long benchmarkReadBinaryColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int fieldIndex = allStructFieldRefs.indexOf(binaryField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            binarySum = 0;

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
                    byte[] binaryValue = Arrays.copyOfRange(bytes, start, start + length);
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return binarySum;
    }

    private static <K, V extends Writable> long benchmarkReadBinaryOrc(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int fieldIndex = allStructFieldRefs.indexOf(binaryField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            binarySum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();


            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                BytesWritable bytesWritable = (BytesWritable) orcLazyObject.materialize();
                if (bytesWritable != null) {
                    byte[] binaryValue = Arrays.copyOfRange(bytesWritable.getBytes(), 0, bytesWritable.getLength());
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return binarySum;
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
                String compressionCodec,
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
                writeFile(tableProperties, file, outputFormat, serDe, compressionCodec);
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


    public static void writeFile(Properties tableProperties, File outputFile, HiveOutputFormat<?, ?> outputFormat, SerDe serDe, String compressionCodec)
            throws Exception
    {
        JobConf jobConf = new JobConf();
        if (compressionCodec != null) {
            CompressionCodec codec = new CompressionCodecFactory(new Configuration()).getCodecByName(compressionCodec);
            jobConf.set(COMPRESS_CODEC, codec.getClass().getName());
            jobConf.set(COMPRESS_TYPE, CompressionType.BLOCK.toString());
        }

        RecordWriter recordWriter = outputFormat.getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compressionCodec != null,
                tableProperties,
                new Progressable()
                    {
                        @Override
                        public void progress()
                        {
                        }
                    }
        );

        serDe.initialize(new Configuration(), tableProperties);

        // Deserialize
        List<String> fieldNames = ImmutableList.of("t_string",
                "t_tinyint",
                "t_smallint",
                "t_int",
                "t_bigint",
                "t_float",
                "t_double",
                "t_map",
                "t_boolean",
                "t_timestamp",
                "t_binary",
                "t_array_string",
                "t_complex");

        List<ObjectInspector> fieldInspectors = ImmutableList.of(
                javaStringObjectInspector,
                javaByteObjectInspector,
                javaShortObjectInspector,
                javaIntObjectInspector,
                javaLongObjectInspector,
                javaFloatObjectInspector,
                javaDoubleObjectInspector,
                getStandardMapObjectInspector(javaStringObjectInspector, javaStringObjectInspector),
                javaBooleanObjectInspector,
                javaTimestampObjectInspector,
                javaByteArrayObjectInspector,
                getStandardListObjectInspector(javaStringObjectInspector),
                getStandardMapObjectInspector(
                        javaStringObjectInspector,
                        getStandardListObjectInspector(
                                getStandardStructObjectInspector(
                                        ImmutableList.of("s_string", "s_double"),
                                        ImmutableList.<ObjectInspector>of(javaStringObjectInspector, javaDoubleObjectInspector)
                                )
                        )
                )
        );

        SettableStructObjectInspector settableStructObjectInspector = getStandardStructObjectInspector(fieldNames, fieldInspectors);
        writeData(recordWriter, serDe, "file", 7, settableStructObjectInspector);
    }

    private static void writeData(RecordWriter recordWriter, SerDe serDe, String fileType, int baseValue, SettableStructObjectInspector objectInspector)
            throws Exception
    {
        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

        for (int rowNumber = 0; rowNumber < 1_000_000; rowNumber++) {
            if (rowNumber % 19 == 0) {
                objectInspector.setStructFieldData(row, fields.get(0), null);
            }
            else {
                objectInspector.setStructFieldData(row, fields.get(0), fileType + " test");
            }

            objectInspector.setStructFieldData(row, fields.get(1), ((byte) (baseValue + 1 + rowNumber)));
            objectInspector.setStructFieldData(row, fields.get(2), (short) (baseValue + 2 + rowNumber));
            objectInspector.setStructFieldData(row, fields.get(3), baseValue + 3 + rowNumber);

            if (rowNumber % 13 == 0) {
                objectInspector.setStructFieldData(row, fields.get(4), null);
            }
            else {
                objectInspector.setStructFieldData(row, fields.get(4), (long) baseValue + 4 + rowNumber);
            }

            objectInspector.setStructFieldData(row, fields.get(5), (float) (baseValue + 5.1 + rowNumber));
            objectInspector.setStructFieldData(row, fields.get(6), baseValue + 6.2 + rowNumber);

            objectInspector.setStructFieldData(row, fields.get(7), null);

            if (rowNumber % 3 == 2) {
                objectInspector.setStructFieldData(row, fields.get(8), null);
            }
            else {
                objectInspector.setStructFieldData(row, fields.get(8), rowNumber % 3 != 0);
            }

            if (rowNumber % 17 == 0) {
                objectInspector.setStructFieldData(row, fields.get(9), null);
            }
            else {
                long seconds = MILLISECONDS.toSeconds(new DateTime(2011, 5, 6, 7, 8, 9, 123).getMillis());
                objectInspector.setStructFieldData(row, fields.get(9), new Timestamp(seconds * 1000));
            }

            if (rowNumber % 23 == 0) {
                objectInspector.setStructFieldData(row, fields.get(10), null);
            }
            else {
                objectInspector.setStructFieldData(row, fields.get(10), (fileType + " test").getBytes(Charsets.UTF_8));
            }

            objectInspector.setStructFieldData(row, fields.get(11), null);
            objectInspector.setStructFieldData(row, fields.get(12), null);

            Writable record = serDe.serialize(row, objectInspector);
            recordWriter.write(record);
        }
        recordWriter.close(false);
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
