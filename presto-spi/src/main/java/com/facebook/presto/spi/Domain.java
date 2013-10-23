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

import static com.facebook.presto.spi.Domains.valueAsType;

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

    public static <T extends Comparable<? super T>> Domain<T> none(Class<T> type)
    {
        return new Domain<>(SortedRangeSet.none(type), false);
    }

    public static <T extends Comparable<? super T>> Domain<T> all(Class<T> type)
    {
        return new Domain<>(SortedRangeSet.of(Range.all(type)), true);
    }

    public static <T extends Comparable<? super T>> Domain<T> onlyNull(Class<T> type)
    {
        return new Domain<>(SortedRangeSet.none(type), true);
    }

    public static <T extends Comparable<? super T>> Domain<T> notNull(Class<T> type)
    {
        return new Domain<>(SortedRangeSet.all(type), false);
    }

    public static <T extends Comparable<? super T>> Domain<T> valueSingle(ColumnValue<T> value)
    {
        return single(value.get());
    }

    public static <T extends Comparable<? super T>> Domain<T> single(T value)
    {
        return new Domain<>(SortedRangeSet.of(Range.equal(value)), false);
    }

    public Class<T> getType()
    {
        return ranges.getType();
    }

    public SortedRangeSet<T> getRanges()
    {
        return ranges;
    }

    public boolean isNullAllowed()
    {
        return nullAllowed;
    }

    public boolean includesValue(Comparable<?> value)
    {
        Objects.requireNonNull(value, "value is null");
        return ranges.includesMarker(Marker.exactly(valueAsType(getType(), value)));
    }

    public Domain<T> intersect(Domain<?> other)
    {
        Domain<T> typedDomain = other.asType(getType());
        SortedRangeSet<T> intersectedRanges = this.getRanges().intersect(typedDomain.getRanges());
        boolean nullAllowed = this.isNullAllowed() && other.isNullAllowed();
        return new Domain<>(intersectedRanges, nullAllowed);
    }

    public Domain<T> union(Domain<?> other)
    {
        SortedRangeSet<T> unionRanges = new SortedRangeSet.Builder<>(getType())
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

    @SuppressWarnings("unchecked")
    public <X extends Comparable<? super X>> Domain<X> asType(Class<X> type)
    {
        if (!type.equals(getType())) {
            throw new IllegalArgumentException(String.format("Can not cast type %s into type %s for Domain", getType(), type));
        }
        return (Domain<X>) this;
    }

    @Override
    public int hashCode()
    {
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
