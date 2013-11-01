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
package com.facebook.presto.operator;

import com.facebook.presto.block.uncompressed.UncompressedLongBlock;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;

public class GroupByIdBlock
        extends UncompressedLongBlock
{
    private final long maxGroupId;

    public GroupByIdBlock(long maxGroupId, Slice slice)
    {
        super(slice);
        this.maxGroupId = maxGroupId;
    }

    public long getMaxGroupId()
    {
        return maxGroupId;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("maxGroupId", maxGroupId)
                .add("positionCount", getPositionCount())
                .toString();
    }
}
