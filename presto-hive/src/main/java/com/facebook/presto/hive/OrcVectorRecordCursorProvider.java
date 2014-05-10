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

public class OrcVectorRecordCursorProvider
        implements HiveRecordCursorProvider
{
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
}
