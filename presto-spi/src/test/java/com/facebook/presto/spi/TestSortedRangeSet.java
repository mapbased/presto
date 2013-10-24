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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Iterator;

public class TestSortedRangeSet
{
    @Test
    public void testEmptySet()
            throws Exception
    {
        SortedRangeSet<Long> rangeSet = SortedRangeSet.none(Long.class);
        Assert.assertTrue(rangeSet.isEmpty());
        Assert.assertTrue(Iterables.isEmpty(rangeSet));
        Assert.assertEquals(rangeSet.size(), 0);
        Assert.assertEquals(rangeSet.getType(), Long.class);
        Assert.assertEquals(rangeSet.complement(), SortedRangeSet.all(Long.class));
        Assert.assertFalse(rangeSet.includesMarker(Marker.lowerUnbounded(Long.class)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.exactly(0L)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testEntireSet()
            throws Exception
    {
        SortedRangeSet<Long> rangeSet = SortedRangeSet.all(Long.class);
        Assert.assertFalse(rangeSet.isEmpty());
        Assert.assertEquals(rangeSet.size(), 1);
        Assert.assertEquals(rangeSet.getType(), Long.class);
        Assert.assertEquals(rangeSet.complement(), SortedRangeSet.none(Long.class));
        Assert.assertTrue(rangeSet.includesMarker(Marker.lowerUnbounded(Long.class)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.exactly(0L)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testSingleValue()
            throws Exception
    {
        SortedRangeSet<Long> rangeSet = SortedRangeSet.of(Range.equal(10L));

        SortedRangeSet<Long> complement = SortedRangeSet.of(Range.greaterThan(10L), Range.lessThan(10L));

        Assert.assertFalse(rangeSet.isEmpty());
        Assert.assertTrue(Iterables.elementsEqual(rangeSet, ImmutableList.of(Range.equal(10L))));
        Assert.assertEquals(rangeSet.size(), 1);
        Assert.assertEquals(rangeSet.getType(), Long.class);
        Assert.assertEquals(rangeSet.complement(), complement);
        Assert.assertFalse(rangeSet.includesMarker(Marker.lowerUnbounded(Long.class)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.exactly(10L)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.exactly(9L)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testBoundedSet()
            throws Exception
    {
        SortedRangeSet<Long> rangeSet = SortedRangeSet.of(
                Range.equal(10L),
                Range.equal(0L),
                Range.range(9L, true, 11L, false),
                Range.equal(0L),
                Range.range(2L, true, 4L, true),
                Range.range(4L, false, 5L, true));

        ImmutableList<Range<Long>> normalizedResult = ImmutableList.of(
                Range.equal(0L),
                Range.range(2L, true, 5L, true),
                Range.range(9L, true, 11L, false));

        SortedRangeSet<Long> complement = SortedRangeSet.of(
                Range.lessThan(0L),
                Range.range(0L, false, 2L, false),
                Range.range(5L, false, 9L, false),
                Range.greaterThanOrEqual(11L));

        Assert.assertFalse(rangeSet.isEmpty());
        Assert.assertTrue(Iterables.elementsEqual(rangeSet, normalizedResult));
        Assert.assertEquals(rangeSet, SortedRangeSet.copyOf(Long.class, normalizedResult));
        Assert.assertEquals(rangeSet.size(), 3);
        Assert.assertEquals(rangeSet.getType(), Long.class);
        Assert.assertEquals(rangeSet.complement(), complement);
        Assert.assertFalse(rangeSet.includesMarker(Marker.lowerUnbounded(Long.class)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.exactly(0L)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.exactly(1L)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.exactly(7L)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.exactly(9L)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testUnboundedSet()
            throws Exception
    {
        SortedRangeSet<Long> rangeSet = SortedRangeSet.of(
                Range.greaterThan(10L),
                Range.lessThanOrEqual(0L),
                Range.range(2L, true, 4L, false),
                Range.range(4L, true, 6L, false),
                Range.range(1L, false, 2L, false),
                Range.range(9L, false, 11L, false));

        ImmutableList<Range<Long>> normalizedResult = ImmutableList.of(
                Range.lessThanOrEqual(0L),
                Range.range(1L, false, 6L, false),
                Range.greaterThan(9L));

        SortedRangeSet<Long> complement = SortedRangeSet.of(
                Range.range(0L, false, 1L, true),
                Range.range(6L, true, 9L, true));

        Assert.assertFalse(rangeSet.isEmpty());
        Assert.assertTrue(Iterables.elementsEqual(rangeSet, normalizedResult));
        Assert.assertEquals(rangeSet, SortedRangeSet.copyOf(Long.class, normalizedResult));
        Assert.assertEquals(rangeSet.size(), 3);
        Assert.assertEquals(rangeSet.getType(), Long.class);
        Assert.assertEquals(rangeSet.complement(), complement);
        Assert.assertTrue(rangeSet.includesMarker(Marker.lowerUnbounded(Long.class)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.exactly(0L)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.exactly(4L)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.exactly(7L)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testIntersect()
            throws Exception
    {
        Assert.assertEquals(
                SortedRangeSet.none(Long.class).intersect(
                        SortedRangeSet.none(Long.class)),
                SortedRangeSet.none(Long.class));

        Assert.assertEquals(
                SortedRangeSet.all(Long.class).intersect(
                        SortedRangeSet.all(Long.class)),
                SortedRangeSet.all(Long.class));

        Assert.assertEquals(
                SortedRangeSet.none(Long.class).intersect(
                        SortedRangeSet.all(Long.class)),
                SortedRangeSet.none(Long.class));

        Assert.assertEquals(
                SortedRangeSet.of(Range.equal(1L), Range.equal(2L), Range.equal(3L)).intersect(
                        SortedRangeSet.of(Range.equal(2L), Range.equal(4L))),
                SortedRangeSet.of(Range.equal(2L)));

        Assert.assertEquals(
                SortedRangeSet.all(Long.class).intersect(
                        SortedRangeSet.of(Range.equal(2L), Range.equal(4L))),
                SortedRangeSet.of(Range.equal(2L), Range.equal(4L)));

        Assert.assertEquals(
                SortedRangeSet.of(Range.range(0L, true, 4L, false)).intersect(
                        SortedRangeSet.of(Range.equal(2L), Range.greaterThan(3L))),
                SortedRangeSet.of(Range.equal(2L), Range.range(3L, false, 4L, false)));

        Assert.assertEquals(
                SortedRangeSet.of(Range.greaterThanOrEqual(0L)).intersect(
                        SortedRangeSet.of(Range.lessThanOrEqual(0L))),
                SortedRangeSet.of(Range.equal(0L)));

        Assert.assertEquals(
                SortedRangeSet.of(Range.greaterThanOrEqual(-1L)).intersect(
                        SortedRangeSet.of(Range.lessThanOrEqual(1L))),
                SortedRangeSet.of(Range.range(-1L, true, 1L, true)));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testUnmodifiableIterator()
            throws Exception
    {
        Iterator<Range<Long>> iterator = SortedRangeSet.of(Range.equal(1L)).iterator();
        iterator.next();
        iterator.remove();
    }
}
