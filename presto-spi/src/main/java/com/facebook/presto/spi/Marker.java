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

public class Marker<T extends Comparable<? super T>>
        implements Comparable<Marker<T>>
{
    public static enum Bound
    {
        BELOW,
        EXACTLY,
        ABOVE
    }

    private final T value;
    private final Bound bound;

    /**
     * UPPER UNBOUNDED is specified with a null value and a BELOW bound
     * LOWER UNBOUNDED is specified with a null value and a ABOVE bound
     */
    private Marker(T value, Bound bound)
    {
        if (value == null && bound == Bound.EXACTLY) {
            throw new IllegalArgumentException("Can not be equal to unbounded");
        }
        this.value = value;
        this.bound = Objects.requireNonNull(bound, "bound is null");
    }

    public static <T extends Comparable<? super T>> Marker<T> upperUnbounded()
    {
        return new Marker<>(null, Bound.BELOW);
    }

    public static <T extends Comparable<? super T>>  Marker<T> lowerUnbounded()
    {
        return new Marker<>(null, Bound.ABOVE);
    }

    public static <T extends Comparable<? super T>> Marker<T> above(T value)
    {
        Objects.requireNonNull(value, "value is null");
        return new Marker<>(value, Bound.ABOVE);
    }

    public static <T extends Comparable<? super T>> Marker<T> exactly(T value)
    {
        Objects.requireNonNull(value, "value is null");
        return new Marker<>(value, Bound.EXACTLY);
    }

    public static <T extends Comparable<? super T>> Marker<T> below(T value)
    {
        Objects.requireNonNull(value, "value is null");
        return new Marker<>(value, Bound.BELOW);
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

    public boolean isAdjacent(Marker<T> other)
    {
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
            throw new IllegalStateException("No marker adjacent to infinity");
        }
        switch (bound) {
            case BELOW:
                return new Marker<>(value, Bound.EXACTLY);
            case EXACTLY:
                return new Marker<>(value, Bound.ABOVE);
            case ABOVE:
                throw new IllegalStateException("No greater marker adjacent to an ABOVE bound");
            default:
                throw new AssertionError("Unsupported type: " + bound);
        }
    }

    public Marker<T> lesserAdjacent()
    {
        if (value == null) {
            throw new IllegalStateException("No marker adjacent to infinity");
        }
        switch (bound) {
            case BELOW:
                throw new IllegalStateException("No lesser marker adjacent to a BELOW bound");
            case EXACTLY:
                return new Marker<>(value, Bound.BELOW);
            case ABOVE:
                return new Marker<>(value, Bound.EXACTLY);
            default:
                throw new AssertionError("Unsupported type: " + bound);
        }
    }

    @Override
    public int compareTo(Marker <T> o)
    {
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
        return Objects.hash(value, bound);
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
        final Marker<?> other = (Marker<?>) obj;
        return Objects.equals(this.value, other.value) &&
                Objects.equals(this.bound, other.bound);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Marker{");
        sb.append("value=").append(value);
        sb.append(", bound=").append(bound);
        sb.append('}');
        return sb.toString();
    }
}
