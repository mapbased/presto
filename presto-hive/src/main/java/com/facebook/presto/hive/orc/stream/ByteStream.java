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
package com.facebook.presto.hive.orc.stream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static com.facebook.presto.hive.orc.stream.OrcStreamUtils.MIN_REPEAT_SIZE;
import static com.facebook.presto.hive.orc.stream.OrcStreamUtils.readFully;

public class ByteStream
{
    private final InputStream input;
    private final byte[] buffer = new byte[MIN_REPEAT_SIZE + 127];
    private int length;
    private int offset;

    public ByteStream(InputStream input)
            throws IOException
    {
        this.input = input;
    }

    private void readNextBlock()
            throws IOException
    {
        int control = input.read();
        if (control == -1) {
            throw new EOFException("Read past end of buffer RLE byte from " + input);
        }

        offset = 0;

        // if byte high bit is not set, this is a repetition; otherwise it is a literal sequence
        if ((control & 0x80) == 0) {
            length = control + MIN_REPEAT_SIZE;

            // read the repeated value
            int value = input.read();
            if (value == -1) {
                throw new EOFException("Reading RLE byte got EOF");
            }

            // fill buffer with the value
            Arrays.fill(buffer, 0, length, (byte) value);
        }
        else {
            // length is 2's compliment of byte
            length = 0x100 - control;

            // read the literals into the buffer
            readFully(input, buffer, 0, length);
        }
    }

    public void skip(long items)
            throws IOException
    {
        while (items > 0) {
            if (offset == length) {
                readNextBlock();
            }
            long consume = Math.min(items, length - offset);
            offset += consume;
            items -= consume;
        }
    }

    public byte next()
        throws IOException
    {
        if (offset == length) {
            readNextBlock();
        }
        return buffer[offset++];
    }

    public void nextVector(long items, long[] vector)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            vector[i] = next();
        }
    }

    public void nextVector(long items, long[] vector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = next();
            }
        }
    }
}
