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

import static com.facebook.presto.spi.Domains.extractType;

/**
 * A point on the continuous space defined by the Comparable T.
 * Each point may be just below, exact, or just above than the specified value according to the Bound.
 */
public class Marker<T extends Comparable<? super T>>
        implements Comparable<Marker<T>>
{
    public static enum Bound
    {
        BELOW,
        EXACTLY,
        ABOVE
    }

    private final Class<T> type;
    private final T value;
    private final Bound bound;

    /**
     * LOWER UNBOUNDED is specified with a null value and a ABOVE bound
     * UPPER UNBOUNDED is specified with a null value and a BELOW bound
     */
    private Marker(Class<T> type, T value, Bound bound)
    {
        Objects.requireNonNull(type, "type is null");
        Objects.requireNonNull(bound, "bound is null");
        if (value == null && bound == Bound.EXACTLY) {
            throw new IllegalArgumentException("Can not be equal to unbounded");
        }
        if (value != null && !type.isInstance(value)) {
            throw new IllegalArgumentException(String.format("value (%s) must be of specified type (%s)", value, type));
        }
        this.type = type;
        this.value = value;
        this.bound = bound;
    }

    public static <T extends Comparable<? super T>> Marker<T> upperUnbounded(Class<T> type)
    {
        return new Marker<>(type, null, Bound.BELOW);
    }

    public static <T extends Comparable<? super T>>  Marker<T> lowerUnbounded(Class<T> type)
    {
        return new Marker<>(type, null, Bound.ABOVE);
    }

    public static <T extends Comparable<? super T>> Marker<T> valueAbove(ColumnValue<T> value)
    {
        return above(value.get());
    }

    public static <T extends Comparable<? super T>> Marker<T> valueExactly(ColumnValue<T> value)
    {
        return exactly(value.get());
    }

    public static <T extends Comparable<? super T>> Marker<T> valueBelow(ColumnValue<T> value)
    {
        return below(value.get());
    }

    public static <T extends Comparable<? super T>> Marker<T> above(T value)
    {
        Objects.requireNonNull(value, "value is null");
        return new Marker<>(extractType(value), value, Bound.ABOVE);
    }

    public static <T extends Comparable<? super T>> Marker<T> exactly(T value)
    {
        Objects.requireNonNull(value, "value is null");
        return new Marker<>(extractType(value), value, Bound.EXACTLY);
    }

    public static <T extends Comparable<? super T>> Marker<T> below(T value)
    {
        Objects.requireNonNull(value, "value is null");
        return new Marker<>(extractType(value), value, Bound.BELOW);
    }

    public Class<T> getType()
    {
        return type;
    }

    public T getValue()
    {
        if (value == null) {
            throw new IllegalStateException("Can not get value for unbounded");
        }
        return value;
    }

    public Bound getBound()
    {
        return bound;
    }

    public boolean isUpperUnbounded()
    {
        return value == null && bound == Bound.BELOW;
    }

    public boolean isLowerUnbounded()
    {
        return value == null && bound == Bound.ABOVE;
    }

    private void checkTypeCompatibility(Marker<?> marker)
    {
        if (!type.equals(marker.getType())) {
            throw new IllegalArgumentException(String.format("Mismatched Marker types: %s and %s", type, marker.getType()));
        }
    }

    public boolean isAdjacent(Marker<T> other)
    {
        checkTypeCompatibility(other);
        if (isUpperUnbounded() || isLowerUnbounded() || other.isUpperUnbounded() || other.isLowerUnbounded()) {
            return false;
        }
        if (value.compareTo(other.value) != 0) {
            return false;
        }
        if (bound == Bound.EXACTLY) {
            return other.bound != Bound.EXACTLY;
        }
        return other.bound == Bound.EXACTLY;
    }

    public Marker<T> greaterAdjacent()
    {
        if (value == null) {
            throw new IllegalStateException("No marker adjacent to unbounded");
        }
        switch (bound) {
            case BELOW:
                return new Marker<>(type, value, Bound.EXACTLY);
            case EXACTLY:
                return new Marker<>(type, value, Bound.ABOVE);
            case ABOVE:
                throw new IllegalStateException("No greater marker adjacent to an ABOVE bound");
            default:
                throw new AssertionError("Unsupported type: " + bound);
        }
    }

    public Marker<T> lesserAdjacent()
    {
        if (value == null) {
            throw new IllegalStateException("No marker adjacent to unbounded");
        }
        switch (bound) {
            case BELOW:
                throw new IllegalStateException("No lesser marker adjacent to a BELOW bound");
            case EXACTLY:
                return new Marker<>(type, value, Bound.BELOW);
            case ABOVE:
                return new Marker<>(type, value, Bound.EXACTLY);
            default:
                throw new AssertionError("Unsupported type: " + bound);
        }
    }

    @Override
    public int compareTo(Marker<T> o)
    {
        checkTypeCompatibility(o);
        if (isUpperUnbounded()) {
            return o.isUpperUnbounded() ? 0 : 1;
        }
        if (isLowerUnbounded()) {
            return o.isLowerUnbounded() ? 0 : -1;
        }
        if (o.isUpperUnbounded()) {
            return -1;
        }
        if (o.isLowerUnbounded()) {
            return 1;
        }
        // INVARIANT: value and o.value not null

        int compare = value.compareTo(o.value);
        if (compare == 0) {
            if (bound == o.bound) {
                return 0;
            }
            if (bound == Bound.BELOW) {
                return -1;
            }
            if (bound == Bound.ABOVE) {
                return 1;
            }
            // INVARIANT: bound == EXACTLY
            return (o.bound == Bound.BELOW) ? 1 : -1;
        }
        return compare;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, value, bound);
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
        final Marker other = (Marker) obj;
        return Objects.equals(this.type, other.type) && Objects.equals(this.value, other.value) && Objects.equals(this.bound, other.bound);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Marker{");
        sb.append("type=").append(type);
        sb.append(", value=").append(value);
        sb.append(", bound=").append(bound);
        sb.append('}');
        return sb.toString();
    }
}
