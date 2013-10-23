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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import static java.util.Collections.unmodifiableCollection;

/***
 * A set containing zero or more Ranges of type T over the continuous space defined by the Comparable T.
 * Ranges are coalesced into the most compact representation of non-overlapping Ranges. This structure
 * allows iteration across these compacted Ranges in increasing order, as well as other common
 * set-related operation.
 */
public class SortedRangeSet<T extends Comparable<? super T>>
        implements Iterable<Range<T>>
{
    private final Class<T> type;
    private final NavigableMap<Marker<T>, Range<T>> lowIndexedRanges;

    private SortedRangeSet(Class<T> type, NavigableMap<Marker<T>, Range<T>> lowIndexedRanges)
    {
        this.type = Objects.requireNonNull(type, "type is null");
        this.lowIndexedRanges = Objects.requireNonNull(lowIndexedRanges, "lowIndexedRanges is null");
    }

    public static <T extends Comparable<? super T>> SortedRangeSet<T> none(Class<T> type)
    {
        return copyOf(type, Collections.<Range<?>>emptyList());
    }

    public static <T extends Comparable<? super T>> SortedRangeSet<T> all(Class<T> type)
    {
        return copyOf(type, Arrays.asList(Range.all(type)));
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet
     */
    public static <T extends Comparable<? super T>> SortedRangeSet<T> of(Range<T> first, Range<?>... ranges)
    {
        List<Range<?>> rangeList = new ArrayList<>();
        rangeList.add(first);
        rangeList.addAll(Arrays.asList(ranges));
        return copyOf(first.getType(), rangeList);
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet
     */
    public static <T extends Comparable<? super T>> SortedRangeSet<T> copyOf(Class<T> type, Iterable<? extends Range<?>> ranges)
    {
        return new Builder<>(type).addAll(ranges).build();
    }

    public SortedRangeSet<T> intersect(SortedRangeSet<T> other)
    {
        if (!type.equals(other.getType())) {
            throw new IllegalArgumentException(String.format("SortedRangeSet type mismatch: %s vs %s", type, other.getType()));
        }

        Builder<T> builder = new Builder<>(type);

        Iterator<Range<T>> iter1 = iterator();
        Iterator<Range<T>> iter2 = other.iterator();

        if (iter1.hasNext() && iter2.hasNext()) {
            Range<T> range1 = iter1.next();
            Range<T> range2 = iter2.next();

            while (true) {
                Range<T> intersect = Range.intersect(range1, range2);
                if (intersect != null) {
                    builder.add(intersect);
                }

                if (range1.getHigh().compareTo(range2.getHigh()) <= 0) {
                    if (!iter1.hasNext()) {
                        break;
                    }
                    range1 = iter1.next();
                }
                else {
                    if (!iter2.hasNext()) {
                        break;
                    }
                    range2 = iter2.next();
                }
            }
        }

        return builder.build();
    }

    public SortedRangeSet<T> complement()
    {
        Builder<T> builder = new Builder<>(type);

        if (lowIndexedRanges.isEmpty()) {
            // If the Range is empty
            return builder.add(Range.all(type)).build();
        }

        Iterator<Range<T>> rangeIterator = lowIndexedRanges.values().iterator();

        Range<T> firstRange = rangeIterator.next();
        if (!firstRange.getLow().isLowerUnbounded()) {
            builder.add(new Range<>(Marker.lowerUnbounded(type), firstRange.getLow().lesserAdjacent()));
        }

        Range<T> previousRange = firstRange;
        while (rangeIterator.hasNext()) {
            Range<T> currentRange = rangeIterator.next();

            Marker<T> lowMarker = previousRange.getHigh().greaterAdjacent();
            Marker<T> highMarker = currentRange.getLow().lesserAdjacent();
            builder.add(new Range<>(lowMarker, highMarker));

            previousRange = currentRange;
        }

        Range<T> lastRange = previousRange;
        if (!lastRange.getHigh().isUpperUnbounded()) {
            builder.add(new Range<>(lastRange.getHigh().greaterAdjacent(), Marker.upperUnbounded(type)));
        }

        return builder.build();
    }

    public Class<T> getType()
    {
        return type;
    }

    public int size()
    {
        return lowIndexedRanges.size();
    }

    public boolean isEmpty()
    {
        return lowIndexedRanges.isEmpty();
    }

    public boolean includesMarker(Marker<T> marker)
    {
        Objects.requireNonNull(marker, "marker is null");
        if (!type.equals(marker.getType())) {
            throw new IllegalArgumentException(String.format("Marker type mismatch: %s vs %s", type, marker.getType()));
        }
        Map.Entry<Marker<T>, Range<T>> floorEntry = lowIndexedRanges.floorEntry(marker);
        return floorEntry != null && floorEntry.getValue().contains(marker);
    }

    @Override
    public Iterator<Range<T>> iterator()
    {
        return unmodifiableCollection(lowIndexedRanges.values()).iterator();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lowIndexedRanges);
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
        final SortedRangeSet<?> other = (SortedRangeSet<?>) obj;
        return Objects.equals(this.lowIndexedRanges, other.lowIndexedRanges);
    }

    @Override
    public String toString()
    {
        return lowIndexedRanges.values().toString();
    }

    public static <T extends Comparable<? super T>> Builder<T> builder(Class<T> type)
    {
        return new Builder<>(type);
    }

    public static class Builder<T extends Comparable<? super T>>
    {
        private final Class<T> type;
        private final NavigableMap<Marker<T>, Range<T>> lowIndexedRanges = new TreeMap<>();

        public Builder(Class<T> type)
        {
            this.type = Objects.requireNonNull(type, "type is null");
        }

        public Builder<T> add(Range<?> range)
        {
            Range<T> typedRange = range.asType(type);

            // Merge with any overlapping ranges
            Map.Entry<Marker<T>, Range<T>> lowFloorEntry = lowIndexedRanges.floorEntry(typedRange.getLow());
            if (lowFloorEntry != null && Range.overlaps(lowFloorEntry.getValue(), typedRange)) {
                typedRange = Range.span(lowFloorEntry.getValue(), typedRange);
            }
            Map.Entry<Marker<T>, Range<T>> highFloorEntry = lowIndexedRanges.floorEntry(typedRange.getHigh());
            if (highFloorEntry != null && Range.overlaps(highFloorEntry.getValue(), typedRange)) {
                typedRange = Range.span(highFloorEntry.getValue(), typedRange);
            }

            // Merge with any adjacent ranges
            if (lowFloorEntry != null && lowFloorEntry.getValue().getHigh().isAdjacent(typedRange.getLow())) {
                typedRange = Range.span(lowFloorEntry.getValue(), typedRange);
            }
            Map.Entry<Marker<T>, Range<T>> highHigherEntry = lowIndexedRanges.higherEntry(typedRange.getHigh());
            if (highHigherEntry != null && highHigherEntry.getValue().getLow().isAdjacent(typedRange.getHigh())) {
                typedRange = Range.span(highHigherEntry.getValue(), typedRange);
            }

            // Delete all encompassed ranges
            NavigableMap<Marker<T>, Range<T>> subMap = lowIndexedRanges.subMap(typedRange.getLow(), true, typedRange.getHigh(), true);
            subMap.clear();

            lowIndexedRanges.put(typedRange.getLow(), typedRange);
            return this;
        }

        public Builder<T> addAll(Iterable<? extends Range<?>> ranges)
        {
            for (Range<?> range : ranges) {
                add(range);
            }
            return this;
        }

        public SortedRangeSet<T> build()
        {
            return new SortedRangeSet<>(type, lowIndexedRanges);
        }
    }
}
