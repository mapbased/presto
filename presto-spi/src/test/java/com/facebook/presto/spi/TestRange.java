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

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRange
{
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMismatchedTypes()
            throws Exception
    {
        // NEVER DO THIS
        new Range(Marker.exactly(1L), Marker.exactly("a"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvertedBounds()
            throws Exception
    {
        new Range<>(Marker.exactly(1L), Marker.exactly(0L));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLowerUnboundedOnly()
            throws Exception
    {
        new Range<>(Marker.lowerUnbounded(Long.class), Marker.lowerUnbounded(Long.class));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUpperUnboundedOnly()
            throws Exception
    {
        new Range<>(Marker.upperUnbounded(Long.class), Marker.upperUnbounded(Long.class));
    }

    @Test
    public void testAllRange()
            throws Exception
    {
        Range<Long> range = Range.all(Long.class);
        Assert.assertEquals(range.getLow(), Marker.lowerUnbounded(Long.class));
        Assert.assertEquals(range.getHigh(), Marker.upperUnbounded(Long.class));
        Assert.assertFalse(range.isSingleValue());
        Assert.assertTrue(range.isAll());
        Assert.assertEquals(range.getType(), Long.class);
        Assert.assertTrue(range.contains(Marker.lowerUnbounded(Long.class)));
        Assert.assertTrue(range.contains(Marker.below(1L)));
        Assert.assertTrue(range.contains(Marker.exactly(1L)));
        Assert.assertTrue(range.contains(Marker.above(1L)));
        Assert.assertTrue(range.contains(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testGreaterThanRange()
            throws Exception
    {
        Range<Long> range = Range.greaterThan(1L);
        Assert.assertEquals(range.getLow(), Marker.above(1L));
        Assert.assertEquals(range.getHigh(), Marker.upperUnbounded(Long.class));
        Assert.assertFalse(range.isSingleValue());
        Assert.assertFalse(range.isAll());
        Assert.assertEquals(range.getType(), Long.class);
        Assert.assertFalse(range.contains(Marker.lowerUnbounded(Long.class)));
        Assert.assertFalse(range.contains(Marker.exactly(1L)));
        Assert.assertTrue(range.contains(Marker.exactly(2L)));
        Assert.assertTrue(range.contains(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testGreaterThanOrEqualRange()
            throws Exception
    {
        Range<Long> range = Range.greaterThanOrEqual(1L);
        Assert.assertEquals(range.getLow(), Marker.exactly(1L));
        Assert.assertEquals(range.getHigh(), Marker.upperUnbounded(Long.class));
        Assert.assertFalse(range.isSingleValue());
        Assert.assertFalse(range.isAll());
        Assert.assertEquals(range.getType(), Long.class);
        Assert.assertFalse(range.contains(Marker.lowerUnbounded(Long.class)));
        Assert.assertFalse(range.contains(Marker.exactly(0L)));
        Assert.assertTrue(range.contains(Marker.exactly(1L)));
        Assert.assertTrue(range.contains(Marker.exactly(2L)));
        Assert.assertTrue(range.contains(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testLessThanRange()
            throws Exception
    {
        Range<Long> range = Range.lessThan(1L);
        Assert.assertEquals(range.getLow(), Marker.lowerUnbounded(Long.class));
        Assert.assertEquals(range.getHigh(), Marker.below(1L));
        Assert.assertFalse(range.isSingleValue());
        Assert.assertFalse(range.isAll());
        Assert.assertEquals(range.getType(), Long.class);
        Assert.assertTrue(range.contains(Marker.lowerUnbounded(Long.class)));
        Assert.assertFalse(range.contains(Marker.exactly(1L)));
        Assert.assertTrue(range.contains(Marker.exactly(0L)));
        Assert.assertFalse(range.contains(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testLessThanOrEqualRange()
            throws Exception
    {
        Range<Long> range = Range.lessThanOrEqual(1L);
        Assert.assertEquals(range.getLow(), Marker.lowerUnbounded(Long.class));
        Assert.assertEquals(range.getHigh(), Marker.exactly(1L));
        Assert.assertFalse(range.isSingleValue());
        Assert.assertFalse(range.isAll());
        Assert.assertEquals(range.getType(), Long.class);
        Assert.assertTrue(range.contains(Marker.lowerUnbounded(Long.class)));
        Assert.assertFalse(range.contains(Marker.exactly(2L)));
        Assert.assertTrue(range.contains(Marker.exactly(1L)));
        Assert.assertTrue(range.contains(Marker.exactly(0L)));
        Assert.assertFalse(range.contains(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testEqualRange()
            throws Exception
    {
        Range<Long> range = Range.equal(1L);
        Assert.assertEquals(range.getLow(), Marker.exactly(1L));
        Assert.assertEquals(range.getHigh(), Marker.exactly(1L));
        Assert.assertTrue(range.isSingleValue());
        Assert.assertFalse(range.isAll());
        Assert.assertEquals(range.getType(), Long.class);
        Assert.assertFalse(range.contains(Marker.lowerUnbounded(Long.class)));
        Assert.assertFalse(range.contains(Marker.exactly(0L)));
        Assert.assertTrue(range.contains(Marker.exactly(1L)));
        Assert.assertFalse(range.contains(Marker.exactly(2L)));
        Assert.assertFalse(range.contains(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testRange()
            throws Exception
    {
        Range<Long> range = Range.range(0L, false, 2L, true);
        Assert.assertEquals(range.getLow(), Marker.above(0L));
        Assert.assertEquals(range.getHigh(), Marker.exactly(2L));
        Assert.assertFalse(range.isSingleValue());
        Assert.assertFalse(range.isAll());
        Assert.assertEquals(range.getType(), Long.class);
        Assert.assertFalse(range.contains(Marker.lowerUnbounded(Long.class)));
        Assert.assertFalse(range.contains(Marker.exactly(0L)));
        Assert.assertTrue(range.contains(Marker.exactly(1L)));
        Assert.assertTrue(range.contains(Marker.exactly(2L)));
        Assert.assertFalse(range.contains(Marker.exactly(3L)));
        Assert.assertFalse(range.contains(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testSpan()
            throws Exception
    {
        Assert.assertEquals(Range.span(Range.greaterThan(1L), Range.lessThanOrEqual(2L)), Range.all(Long.class));
        Assert.assertEquals(Range.span(Range.greaterThan(2L), Range.lessThanOrEqual(0L)), Range.all(Long.class));
        Assert.assertEquals(Range.span(Range.range(1L, true, 3L, false), Range.equal(2L)), Range.range(1L, true, 3L, false));
        Assert.assertEquals(Range.span(Range.range(1L, true, 3L, false), Range.range(2L, false, 10L, false)), Range.range(1L, true, 10L, false));
        Assert.assertEquals(Range.span(Range.greaterThan(1L), Range.equal(0L)), Range.greaterThanOrEqual(0L));
        Assert.assertEquals(Range.span(Range.greaterThan(1L), Range.greaterThanOrEqual(10L)), Range.greaterThan(1L));
        Assert.assertEquals(Range.span(Range.lessThan(1L), Range.lessThanOrEqual(1L)), Range.lessThanOrEqual(1L));
        Assert.assertEquals(Range.span(Range.all(Long.class), Range.lessThanOrEqual(1L)), Range.all(Long.class));
    }

    @Test
    public void testOverlaps()
            throws Exception
    {
        Assert.assertTrue(Range.overlaps(Range.greaterThan(1L), Range.lessThanOrEqual(2L)));
        Assert.assertFalse(Range.overlaps(Range.greaterThan(2L), Range.lessThan(2L)));
        Assert.assertTrue(Range.overlaps(Range.range(1L, true, 3L, false), Range.equal(2L)));
        Assert.assertTrue(Range.overlaps(Range.range(1L, true, 3L, false), Range.range(2L, false, 10L, false)));
        Assert.assertFalse(Range.overlaps(Range.range(1L, true, 3L, false), Range.range(3L, true, 10L, false)));
        Assert.assertTrue(Range.overlaps(Range.range(1L, true, 3L, true), Range.range(3L, true, 10L, false)));
        Assert.assertTrue(Range.overlaps(Range.all(Long.class), Range.equal(Long.MAX_VALUE)));
    }

    @Test
    public void testIntersect()
            throws Exception
    {
        Assert.assertEquals(Range.intersect(Range.greaterThan(1L), Range.lessThanOrEqual(2L)), Range.range(1L, false, 2L, true));
        Assert.assertNull(Range.intersect(Range.greaterThan(2L), Range.lessThan(2L)));
        Assert.assertEquals(Range.intersect(Range.range(1L, true, 3L, false), Range.equal(2L)), Range.equal(2L));
        Assert.assertEquals(Range.intersect(Range.range(1L, true, 3L, false), Range.range(2L, false, 10L, false)), Range.range(2L, false, 3L, false));
        Assert.assertNull(Range.intersect(Range.range(1L, true, 3L, false), Range.range(3L, true, 10L, false)));
        Assert.assertEquals(Range.intersect(Range.range(1L, true, 3L, true), Range.range(3L, true, 10L, false)), Range.equal(3L));
        Assert.assertEquals(Range.intersect(Range.all(Long.class), Range.equal(Long.MAX_VALUE)), Range.equal(Long.MAX_VALUE));
    }
}
