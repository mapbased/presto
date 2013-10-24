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

public class TestDomain
{
    @Test
    public void testNone()
            throws Exception
    {
        Domain<Long> domain = Domain.none(Long.class);
        Assert.assertFalse(domain.isNullAllowed());
        Assert.assertEquals(domain.getRanges(), SortedRangeSet.none(Long.class));
        Assert.assertEquals(domain.getType(), Long.class);
        Assert.assertFalse(domain.includesValue(Long.MIN_VALUE));
        Assert.assertFalse(domain.includesValue(0L));
        Assert.assertFalse(domain.includesValue(Long.MAX_VALUE));
        Assert.assertEquals(domain.complement(), Domain.all(Long.class));
    }

    @Test
    public void testAll()
            throws Exception
    {
        Domain<Long> domain = Domain.all(Long.class);
        Assert.assertTrue(domain.isNullAllowed());
        Assert.assertEquals(domain.getRanges(), SortedRangeSet.all(Long.class));
        Assert.assertEquals(domain.getType(), Long.class);
        Assert.assertTrue(domain.includesValue(Long.MIN_VALUE));
        Assert.assertTrue(domain.includesValue(0L));
        Assert.assertTrue(domain.includesValue(Long.MAX_VALUE));
        Assert.assertEquals(domain.complement(), Domain.none(Long.class));
    }

    @Test
    public void testNullOnly()
            throws Exception
    {
        Domain<Long> domain = Domain.onlyNull(Long.class);
        Assert.assertTrue(domain.isNullAllowed());
        Assert.assertEquals(domain.getRanges(), SortedRangeSet.none(Long.class));
        Assert.assertEquals(domain.getType(), Long.class);
        Assert.assertFalse(domain.includesValue(Long.MIN_VALUE));
        Assert.assertFalse(domain.includesValue(0L));
        Assert.assertFalse(domain.includesValue(Long.MAX_VALUE));
        Assert.assertEquals(domain.complement(), Domain.notNull(Long.class));
    }

    @Test
    public void testNotNull()
            throws Exception
    {
        Domain<Long> domain = Domain.notNull(Long.class);
        Assert.assertFalse(domain.isNullAllowed());
        Assert.assertEquals(domain.getRanges(), SortedRangeSet.all(Long.class));
        Assert.assertEquals(domain.getType(), Long.class);
        Assert.assertTrue(domain.includesValue(Long.MIN_VALUE));
        Assert.assertTrue(domain.includesValue(0L));
        Assert.assertTrue(domain.includesValue(Long.MAX_VALUE));
        Assert.assertEquals(domain.complement(), Domain.onlyNull(Long.class));
    }

    @Test
    public void testSingle()
            throws Exception
    {
        Domain<Long> domain = Domain.single(0L);
        Assert.assertFalse(domain.isNullAllowed());
        Assert.assertEquals(domain.getRanges(), SortedRangeSet.of(Range.equal(0L)));
        Assert.assertEquals(domain.getType(), Long.class);
        Assert.assertFalse(domain.includesValue(Long.MIN_VALUE));
        Assert.assertTrue(domain.includesValue(0L));
        Assert.assertFalse(domain.includesValue(Long.MAX_VALUE));
        Assert.assertEquals(domain.complement(), Domain.create(SortedRangeSet.of(Range.lessThan(0L), Range.greaterThan(0L)), true));
    }

    @Test
    public void testIntersect()
            throws Exception
    {
        Assert.assertEquals(
                Domain.all(Long.class).intersect(Domain.all(Long.class)),
                Domain.all(Long.class));

        Assert.assertEquals(
                Domain.none(Long.class).intersect(Domain.none(Long.class)),
                Domain.none(Long.class));

        Assert.assertEquals(
                Domain.all(Long.class).intersect(Domain.none(Long.class)),
                Domain.none(Long.class));

        Assert.assertEquals(
                Domain.notNull(Long.class).intersect(Domain.onlyNull(Long.class)),
                Domain.none(Long.class));

        Assert.assertEquals(
                Domain.single(0L).intersect(Domain.all(Long.class)),
                Domain.single(0L));

        Assert.assertEquals(
                Domain.single(0L).intersect(Domain.onlyNull(Long.class)),
                Domain.none(Long.class));

        Assert.assertEquals(
                Domain.create(SortedRangeSet.of(Range.equal(1L)), true).intersect(Domain.create(SortedRangeSet.of(Range.equal(2L)), true)),
                Domain.onlyNull(Long.class));

        Assert.assertEquals(
                Domain.create(SortedRangeSet.of(Range.equal(1L)), true).intersect(Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L)), false)),
                Domain.single(1L));
    }

    @Test
    public void testUnion()
            throws Exception
    {
        Assert.assertEquals(
                Domain.all(Long.class).union(Domain.all(Long.class)),
                Domain.all(Long.class));

        Assert.assertEquals(
                Domain.none(Long.class).union(Domain.none(Long.class)),
                Domain.none(Long.class));

        Assert.assertEquals(
                Domain.all(Long.class).union(Domain.none(Long.class)),
                Domain.all(Long.class));

        Assert.assertEquals(
                Domain.notNull(Long.class).union(Domain.onlyNull(Long.class)),
                Domain.all(Long.class));

        Assert.assertEquals(
                Domain.single(0L).union(Domain.all(Long.class)),
                Domain.all(Long.class));

        Assert.assertEquals(
                  Domain.single(0L).union(Domain.notNull(Long.class)),
                  Domain.notNull(Long.class));

        Assert.assertEquals(
                Domain.single(0L).union(Domain.onlyNull(Long.class)),
                Domain.create(SortedRangeSet.of(Range.equal(0L)), true));

        Assert.assertEquals(
                Domain.create(SortedRangeSet.of(Range.equal(1L)), true).union(Domain.create(SortedRangeSet.of(Range.equal(2L)), true)),
                Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L)), true));

        Assert.assertEquals(
                Domain.create(SortedRangeSet.of(Range.equal(1L)), true).union(Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L)), false)),
                Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L)), true));
    }
}
