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
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import static java.util.Collections.unmodifiableCollection;

public class SortedRangeSet<T extends Comparable<? super T>>
        implements Iterable<Range<T>>
{
    private final NavigableMap<Marker<T>, Range<T>> lowIndexedRanges;

    private SortedRangeSet(NavigableMap<Marker<T>, Range<T>> lowIndexedRanges)
    {
        this.lowIndexedRanges = Objects.requireNonNull(lowIndexedRanges, "lowIndexedRanges is null");
    }

    @SafeVarargs
    public static <T extends Comparable<? super T>> SortedRangeSet<T> of(Range<T>... ranges)
    {
        return copyOf(Arrays.asList(ranges));
    }

    public static <T extends Comparable<? super T>> SortedRangeSet<T> copyOf(Iterable<Range<T>> ranges)
    {
        return new Builder<T>().addAll(ranges).build();
    }

    public SortedRangeSet<T> intersect(SortedRangeSet<T> other)
    {
        Builder<T> builder = new Builder<>();

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
        Builder<T> builder = new Builder<>();

        if (lowIndexedRanges.isEmpty()) {
            // If the Range is empty
            return builder.add(Range.<T>all()).build();
        }

        Iterator<Range<T>> rangeIterator = lowIndexedRanges.values().iterator();

        Range<T> firstRange = rangeIterator.next();
        if (!firstRange.getLow().isLowerUnbounded()) {
            builder.add(new Range<>(Marker.<T>lowerUnbounded(), firstRange.getLow().lesserAdjacent()));
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
            builder.add(new Range<>(lastRange.getHigh().greaterAdjacent(), Marker.<T>upperUnbounded()));
        }

        return builder.build();
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
        Map.Entry<Marker<T>, Range<T>> floorEntry = lowIndexedRanges.floorEntry(marker);
        return floorEntry != null && floorEntry.getValue().contains(marker);
    }

    @Override
    public Iterator<Range<T>> iterator()
    {
        return unmodifiableCollection(lowIndexedRanges.values()).iterator();
    }

    @Override
    public int hashCode() {
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

    public static <T extends Comparable<? super T>> Builder<T> builder()
    {
        return new Builder<>();
    }

    public static class Builder<T extends Comparable<? super T>>
    {
        private final NavigableMap<Marker<T>, Range<T>> lowIndexedRanges = new TreeMap<>();

        public Builder<T> add(Range<T> range)
        {
            // Merge with any overlapping ranges
            Map.Entry<Marker<T>, Range<T>> lowFloorEntry = lowIndexedRanges.floorEntry(range.getLow());
            if (lowFloorEntry != null && Range.overlaps(lowFloorEntry.getValue(), range)) {
                range = Range.span(lowFloorEntry.getValue(), range);
            }
            Map.Entry<Marker<T>, Range<T>> highFloorEntry = lowIndexedRanges.floorEntry(range.getHigh());
            if (highFloorEntry != null && Range.overlaps(highFloorEntry.getValue(), range)) {
                range = Range.span(highFloorEntry.getValue(), range);
            }

            // Merge with any adjacent ranges
            if (lowFloorEntry != null && lowFloorEntry.getValue().getHigh().isAdjacent(range.getLow())) {
                range = Range.span(lowFloorEntry.getValue(), range);
            }
            Map.Entry<Marker<T>, Range<T>> highHigherEntry = lowIndexedRanges.higherEntry(range.getHigh());
            if (highHigherEntry != null && highHigherEntry.getValue().getLow().isAdjacent(range.getHigh())) {
                range = Range.span(highHigherEntry.getValue(), range);
            }

            // Delete all encompassed ranges
            NavigableMap<Marker<T>, Range<T>> subMap = lowIndexedRanges.subMap(range.getLow(), true, range.getHigh(), true);
            subMap.clear();

            lowIndexedRanges.put(range.getLow(), range);
            return this;
        }

        public Builder<T> addAll(Iterable<Range<T>> ranges)
        {
            for (Range<T> range : ranges) {
                add(range);
            }
            return this;
        }

        public SortedRangeSet<T> build()
        {
            return new SortedRangeSet<>(lowIndexedRanges);
        }
    }
}
