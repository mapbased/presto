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

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static com.facebook.presto.spi.Domains.extractType;

/**
 * A Range of values across the continuous space defined by the Comparable T.
 */
public class Range<T extends Comparable<? super T>>
{
    private final Marker<T> low;
    private final Marker<T> high;

    public Range(Marker<T> low, Marker<T> high)
    {
        Objects.requireNonNull(low, "value is null");
        Objects.requireNonNull(high, "value is null");
        if (!low.getType().equals(high.getType())) {
            throw new IllegalArgumentException(String.format("Marker types do not match: %s vs %s", low.getType(), high.getType()));
        }
        if (low.isUpperUnbounded()) {
            throw new IllegalArgumentException("low cannot be upper unbounded");
        }
        if (high.isLowerUnbounded()) {
            throw new IllegalArgumentException("high cannot be lower unbounded");
        }
        if (low.compareTo(high) > 0) {
            throw new IllegalArgumentException("low must be less than or equal to high");
        }
        this.low = low;
        this.high = high;
    }

    public static <T extends Comparable<? super T>> Range<T> all(Class<T> type)
    {
        return new Range<>(Marker.lowerUnbounded(type), Marker.upperUnbounded(type));
    }

    public static <T extends Comparable<? super T>> Range<T> valueGreaterThan(ColumnValue<T> low)
    {
        return greaterThan(low.get());
    }

    public static <T extends Comparable<? super T>> Range<T> valueGreaterThanOrEqual(ColumnValue<T> low)
    {
        return greaterThanOrEqual(low.get());
    }

    public static <T extends Comparable<? super T>> Range<T> valueLessThan(ColumnValue<T> high)
    {
        return lessThan(high.get());
    }

    public static <T extends Comparable<? super T>> Range<T> valueLessThanOrEqual(ColumnValue<T> high)
    {
        return lessThanOrEqual(high.get());
    }

    public static <T extends Comparable<? super T>> Range<T> valueEqual(ColumnValue<T> value)
    {
        return equal(value.get());
    }

    public static <T extends Comparable<? super T>> Range<T> valueRange(ColumnValue<T> low, boolean lowInclusive, ColumnValue<T> high, boolean highInclusive)
    {
        return range(low.get(), lowInclusive, high.get(), highInclusive);
    }

    public static <T extends Comparable<? super T>> Range<T> greaterThan(T low)
    {
        return new Range<>(Marker.above(low), Marker.upperUnbounded(extractType(low)));
    }

    public static <T extends Comparable<? super T>> Range<T> greaterThanOrEqual(T low)
    {
        return new Range<>(Marker.exactly(low), Marker.upperUnbounded(extractType(low)));
    }

    public static <T extends Comparable<? super T>> Range<T> lessThan(T high)
    {
        return new Range<>(Marker.lowerUnbounded(extractType(high)), Marker.below(high));
    }

    public static <T extends Comparable<? super T>> Range<T> lessThanOrEqual(T high)
    {
        return new Range<>(Marker.lowerUnbounded(extractType(high)), Marker.exactly(high));
    }

    public static <T extends Comparable<? super T>> Range<T> equal(T value)
    {
        return new Range<>(Marker.exactly(value), Marker.exactly(value));
    }

    public static <T extends Comparable<? super T>> Range<T> range(T low, boolean lowInclusive, T high, boolean highInclusive)
    {
        Marker<T> lowMarker = lowInclusive ? Marker.exactly(low) : Marker.above(low);
        Marker<T> highMarker = highInclusive ? Marker.exactly(high) : Marker.below(high);
        return new Range<>(lowMarker, highMarker);
    }

    public static <T extends Comparable<? super T>> Range<T> span(Range<T> range1, Range<T> range2)
    {
        Marker<T> lowMarker = Collections.min(Arrays.asList(range1.getLow(), range2.getLow()));
        Marker<T> highMarker = Collections.max(Arrays.asList(range1.getHigh(), range2.getHigh()));
        return new Range<>(lowMarker, highMarker);
    }

    public static <T extends Comparable<? super T>> boolean overlaps(Range<T> range1, Range<T> range2)
    {
        int lowCompare = range1.getLow().compareTo(range2.getLow());
        if (lowCompare == 0) {
            return true;
        }

        Range<T> lowerLowRange = (lowCompare > 0) ? range2 : range1;
        Range<T> higherLowRange = (lowCompare > 0) ? range1 : range2;

        return lowerLowRange.getHigh().compareTo(higherLowRange.getLow()) >= 0;
    }

    public static <T extends Comparable<? super T>> Range<T> intersect(Range<T> range1, Range<T> range2)
    {
        if (overlaps(range1, range2)) {
            Marker<T> lowMarker = Collections.max(Arrays.asList(range1.getLow(), range2.getLow()));
            Marker<T> highMarker = Collections.min(Arrays.asList(range1.getHigh(), range2.getHigh()));
            return new Range<>(lowMarker, highMarker);
        }
        else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public <X extends Comparable<? super X>> Range<X> asType(Class<X> type)
    {
        if (!type.equals(getType())) {
            throw new IllegalArgumentException(String.format("Can not cast type %s into type %s for Range", getType(), type));
        }
        return (Range<X>) this;
    }

    public Class<T> getType()
    {
        return low.getType();
    }

    public Marker<T> getLow()
    {
        return low;
    }

    public Marker<T> getHigh()
    {
        return high;
    }

    public boolean isSingleValue()
    {
        return !low.isLowerUnbounded() &&
                !high.isUpperUnbounded() &&
                low.getBound() == Marker.Bound.EXACTLY &&
                high.getBound() == Marker.Bound.EXACTLY &&
                low.getValue() == high.getValue();
    }

    public boolean isAll()
    {
        return low.isLowerUnbounded() && high.isUpperUnbounded();
    }

    public boolean contains(Marker<T> marker)
    {
        Objects.requireNonNull(marker, "marker is null");
        if (!getType().equals(marker.getType())) {
            throw new IllegalArgumentException(String.format("Mismatched types: %s and %s", getType(), marker.getType()));
        }
        return low.compareTo(marker) <= 0 && high.compareTo(marker) >= 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(low, high);
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
        final Range<?> other = (Range<?>) obj;
        return Objects.equals(this.low, other.low) &&
                Objects.equals(this.high, other.high);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        if (isSingleValue()) {
            sb.append('[').append(low.getValue()).append(']');
        }
        else {
            sb.append((low.getBound() == Marker.Bound.EXACTLY) ? '[' : '(');
            sb.append(low.isLowerUnbounded() ? "min" : low.getValue());
            sb.append(", ");
            sb.append(high.isUpperUnbounded() ? "max" : high.getValue());
            sb.append((high.getBound() == Marker.Bound.EXACTLY) ? ']' : ')');
        }
        return sb.toString();
    }
}
