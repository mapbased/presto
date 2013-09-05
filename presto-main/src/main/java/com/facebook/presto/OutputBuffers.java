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
package com.facebook.presto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class OutputBuffers
{
    private final long version;
    private final boolean noMoreBufferIds;
    private final Map<String, PagePartitionFunction> buffers;

    public OutputBuffers(long version, boolean noMoreBufferIds, String... buffers)
    {
        this(version, noMoreBufferIds, ImmutableSet.copyOf(buffers));
    }

    public OutputBuffers(long version, boolean noMoreBufferIds, Set<String> buffers)
    {
        this.version = version;
        this.noMoreBufferIds = noMoreBufferIds;
        this.buffers = Maps.toMap(buffers, new Function<String, PagePartitionFunction>() {
            @Override
            public PagePartitionFunction apply(String input)
            {
                return new UnpartitionedPagePartitionFunction();
            }
        });
    }

    @JsonCreator
    public OutputBuffers(
            @JsonProperty("version") long version,
            @JsonProperty("noMoreBufferIds") boolean noMoreBufferIds,
            @JsonProperty("buffers") Map<String, PagePartitionFunction> buffers)
    {
        this.version = version;
        this.buffers = ImmutableMap.copyOf(checkNotNull(buffers, "buffers is null"));
        this.noMoreBufferIds = noMoreBufferIds;
    }

    @JsonProperty
    public long getVersion()
    {
        return version;
    }

    @JsonProperty
    public boolean isNoMoreBufferIds()
    {
        return noMoreBufferIds;
    }

    @JsonProperty
    public Map<String, PagePartitionFunction> getBuffers()
    {
        return buffers;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(version, noMoreBufferIds, buffers);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final OutputBuffers other = (OutputBuffers) obj;
        return Objects.equal(this.version, other.version) &&
                Objects.equal(this.noMoreBufferIds, other.noMoreBufferIds) &&
                Objects.equal(this.buffers, other.buffers);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("version", version)
                .add("noMoreBufferIds", noMoreBufferIds)
                .add("bufferIds", buffers)
                .toString();
    }
}
