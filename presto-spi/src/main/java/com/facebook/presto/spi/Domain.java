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
package com.facebook.presto.spi;

import java.util.Objects;

/**
 * Defines the possible values of a variable in terms of valid ranges and nullability.
 * An empty list of Ranges denotes that the variable does not belong to any range
 * (but null may still be a possible value depending on the value of nullAllowed).
 */
public class Domain<T extends Comparable<? super T>>
{
    private final SortedRangeSet<T> ranges;
    private final boolean nullAllowed;

    public Domain(SortedRangeSet<T> ranges, boolean nullAllowed)
    {
        this.ranges = Objects.requireNonNull(ranges, "ranges is null");
        this.nullAllowed = nullAllowed;
    }

    public static <T extends Comparable<? super T>> Domain<T> create(SortedRangeSet<T> ranges, boolean nullAllowed)
    {
        return new Domain<>(ranges, nullAllowed);
    }

    public static <T extends Comparable<? super T>> Domain<T> none()
    {
        return new Domain<>(SortedRangeSet.<T>of(), false);
    }

    public static <T extends Comparable<? super T>> Domain<T> nullOnly()
    {
        return new Domain<>(SortedRangeSet.<T>of(), true);
    }

    public static <T extends Comparable<? super T>> Domain<T> singleValue(T value)
    {
        Objects.requireNonNull(value, "value is null");
        return new Domain<>(SortedRangeSet.of(Range.equal(value)), false);
    }

    public SortedRangeSet<T> getRanges()
    {
        return ranges;
    }

    public boolean isNullAllowed()
    {
        return nullAllowed;
    }

    public boolean includesValue(T value)
    {
        Objects.requireNonNull(value, "value is null");
        return ranges.includesMarker(Marker.exactly(value));
    }

    public Domain<T> intersect(Domain<T> other)
    {
        SortedRangeSet<T> intersectedRanges = this.getRanges().intersect(other.getRanges());
        boolean nullAllowed = this.isNullAllowed() && other.isNullAllowed();
        return new Domain<>(intersectedRanges, nullAllowed);
    }

    public Domain<T> union(Domain<T> other)
    {
        SortedRangeSet<T> unionRanges = new SortedRangeSet.Builder<T>()
                .addAll(this.getRanges())
                .addAll(other.getRanges())
                .build();

        boolean nullAllowed = this.isNullAllowed() || other.isNullAllowed();
        return new Domain<>(unionRanges, nullAllowed);
    }

    public Domain<T> complement()
    {
        return new Domain<>(ranges.complement(), !nullAllowed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ranges, nullAllowed);
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
        final Domain<?> other = (Domain<?>) obj;
        return Objects.equals(this.ranges, other.ranges) &&
                Objects.equals(this.nullAllowed, other.nullAllowed);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Domain{");
        sb.append("ranges=").append(ranges);
        sb.append(", nullAllowed=").append(nullAllowed);
        sb.append('}');
        return sb.toString();
    }
}
