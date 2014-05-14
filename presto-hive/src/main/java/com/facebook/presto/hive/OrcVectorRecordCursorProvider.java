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

import com.facebook.presto.spi.ConnectorSession;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveUtil.getDeserializer;
import static com.google.common.collect.Iterables.all;

public class OrcVectorRecordCursorProvider
        implements HiveRecordCursorProvider
{
    private final boolean allowStringColumns;

    public OrcVectorRecordCursorProvider()
    {
        this(false);
    }

    // todo remove this when ORC fixes HIVE-7044
    OrcVectorRecordCursorProvider(boolean allowStringColumns)
    {
        this.allowStringColumns = allowStringColumns;
    }

    @Override
    public Optional<HiveRecordCursor> createHiveRecordCursor(
            String clientId,
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            Properties schema,
            List<HiveColumnHandle> columns,
            List<HivePartitionKey> partitionKeys,
            DateTimeZone hiveStorageTimeZone)
    {
        @SuppressWarnings("deprecation")
        Deserializer deserializer = getDeserializer(schema);
        if (!(deserializer instanceof OrcSerde)) {
            return Optional.absent();
        }

        // are all columns supported by the Orc vector code
        if (!all(columns, isOrcVectorSupportedType(allowStringColumns))) {
            return Optional.absent();
        }

        RecordReader recordReader;
        try {
            StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
            boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
            for (HiveColumnHandle column : columns) {
                include[column.getHiveColumnIndex() + 1] = true;
            }

            FileSystem fileSystem = path.getFileSystem(configuration);
            Reader reader = OrcFile.createReader(fileSystem, path);
            recordReader = reader.rows(start, length, include);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        return Optional.<HiveRecordCursor>of(new OrcVectorHiveRecordCursor(
                recordReader,
                length,
                schema,
                partitionKeys,
                columns,
                DateTimeZone.forID(session.getTimeZoneKey().getId())));
    }

    private static Predicate<HiveColumnHandle> isOrcVectorSupportedType(final boolean allowStringColumns)
    {
        return new Predicate<HiveColumnHandle>()
        {
            @Override
            public boolean apply(HiveColumnHandle columnHandle)
            {
                HiveType hiveType = columnHandle.getHiveType();
                switch (hiveType) {
                    case BOOLEAN:
                    case BYTE:
                    case SHORT:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case TIMESTAMP:
                    case DATE:
                        return true;
                    case STRING:
                        // todo Orc code does not properly deserialize a column of empty strings HIVE-7044
                        return allowStringColumns;
                    case STRUCT:
                        // not implemented in Presto
                    case BINARY:
                        // not supported in Orc for some reason
                    case LIST:
                        // not supported in Orc
                    case MAP:
                        // not supported in Orc
                    default:
                        return false;
                }
            }
        };
    }
}
