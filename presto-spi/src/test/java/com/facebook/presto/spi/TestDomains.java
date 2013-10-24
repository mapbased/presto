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

import com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDomains
{
    @Test
    public void testIntersection()
            throws Exception
    {
        Assert.assertEquals(
                Domains.intersectDomainMaps(
                        ImmutableMap.<String, Domain<?>>builder()
                                .put("a", Domain.none(Long.class))
                                .put("b", Domain.all(String.class))
                                .put("c", Domain.notNull(Double.class))
                                .put("d", Domain.onlyNull(Long.class))
                                .put("e", Domain.single(1L))
                                .put("f", Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(0.0)), true))
                                .build(),
                        ImmutableMap.<String, Domain<?>>builder()
                                .put("a", Domain.all(Long.class))
                                .put("b", Domain.single("value"))
                                .put("c", Domain.single(0.0))
                                .put("d", Domain.notNull(Long.class))
                                .put("e", Domain.single(1L))
                                .put("f", Domain.create(SortedRangeSet.of(Range.lessThan(10.0)), false))
                                .build()),
                ImmutableMap.<String, Domain<?>>builder()
                        .put("a", Domain.none(Long.class))
                        .put("b", Domain.single("value"))
                        .put("c", Domain.single(0.0))
                        .put("d", Domain.none(Long.class))
                        .put("e", Domain.single(1L))
                        .put("f", Domain.create(SortedRangeSet.of(Range.range(0.0, true, 10.0, false)), false))
                        .build());
    }

    @Test
    public void testMultipleIntersection()
            throws Exception
    {
        Assert.assertEquals(
                Domains.intersectDomainMaps(
                        ImmutableMap.<String, Domain<?>>builder()
                                .put("a", Domain.create(SortedRangeSet.of(Range.lessThan(10.0)), false))
                                .build(),
                        ImmutableMap.<String, Domain<?>>builder()
                                .put("a", Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(0.0)), true))
                                .build(),
                        ImmutableMap.<String, Domain<?>>builder()
                                .put("a", Domain.create(SortedRangeSet.of(Range.lessThan(5.0)), false))
                                .build()),
                ImmutableMap.<String, Domain<?>>builder()
                        .put("a", Domain.create(SortedRangeSet.of(Range.range(0.0, true, 5.0, false)), false))
                        .build());
    }

    @Test
    public void testMismatchedKeyIntersection()
            throws Exception
    {
        Assert.assertEquals(
                Domains.intersectDomainMaps(
                        ImmutableMap.<String, Domain<?>>builder()
                                .put("a", Domain.all(Double.class))
                                .put("b", Domain.single("value"))
                                .build(),
                        ImmutableMap.<String, Domain<?>>builder()
                                .put("a", Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(0.0)), true))
                                .put("c", Domain.single(1L))
                                .build()),
                ImmutableMap.<String, Domain<?>>builder()
                        .put("a", Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(0.0)), true))
                        .put("b", Domain.single("value"))
                        .put("c", Domain.single(1L))
                        .build());
    }

    @Test
    public void testUnion()
            throws Exception
    {
        Assert.assertEquals(
                Domains.unionDomainMaps(
                        ImmutableMap.<String, Domain<?>>builder()
                                .put("a", Domain.none(Long.class))
                                .put("b", Domain.all(String.class))
                                .put("c", Domain.notNull(Double.class))
                                .put("d", Domain.onlyNull(Long.class))
                                .put("e", Domain.single(1L))
                                .put("f", Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(0.0)), true))
                                .build(),
                        ImmutableMap.<String, Domain<?>>builder()
                                .put("a", Domain.all(Long.class))
                                .put("b", Domain.single("value"))
                                .put("c", Domain.single(0.0))
                                .put("d", Domain.notNull(Long.class))
                                .put("e", Domain.single(1L))
                                .put("f", Domain.create(SortedRangeSet.of(Range.lessThan(10.0)), false))
                                .build()),
                ImmutableMap.<String, Domain<?>>builder()
                        .put("a", Domain.all(Long.class))
                        .put("b", Domain.all(String.class))
                        .put("c", Domain.notNull(Double.class))
                        .put("d", Domain.all(Long.class))
                        .put("e", Domain.single(1L))
                        .put("f", Domain.all(Double.class))
                        .build());
    }

    @Test
    public void testMultipleUnion()
            throws Exception
    {
        Assert.assertEquals(
                Domains.unionDomainMaps(
                        ImmutableMap.<String, Domain<?>>builder()
                                .put("a", Domain.create(SortedRangeSet.of(Range.range(0L, true, 5L, true)), false))
                                .build(),
                        ImmutableMap.<String, Domain<?>>builder()
                                .put("a", Domain.create(SortedRangeSet.of(Range.range(4L, true, 6L, true)), true))
                                .build(),
                        ImmutableMap.<String, Domain<?>>builder()
                                .put("a", Domain.create(SortedRangeSet.of(Range.equal(10L)), false))
                                .build()),
                ImmutableMap.<String, Domain<?>>builder()
                        .put("a", Domain.create(SortedRangeSet.of(Range.range(0L, true, 6L, true), Range.equal(10L)), true))
                        .build());
    }

    @Test
    public void testMismatchedKeyUnion()
            throws Exception
    {
        Assert.assertEquals(
                Domains.unionDomainMaps(
                        ImmutableMap.<String, Domain<?>>builder()
                                .put("a", Domain.all(Double.class))
                                .put("b", Domain.single("value"))
                                .build(),
                        ImmutableMap.<String, Domain<?>>builder()
                                .put("a", Domain.create(SortedRangeSet.of(Range.greaterThanOrEqual(0.0)), true))
                                .put("c", Domain.single(1L))
                                .build()),
                ImmutableMap.<String, Domain<?>>builder()
                        .put("a", Domain.all(Double.class))
                        .build());
    }

    @Test
    public void testExtractSingleValues()
            throws Exception
    {
        Assert.assertEquals(
                Domains.extractSingleValues(
                        ImmutableMap.<String, Domain<?>>builder()
                                .put("a", Domain.all(Double.class))
                                .put("b", Domain.single("value"))
                                .put("c", Domain.onlyNull(Long.class))
                                .put("d", Domain.create(SortedRangeSet.of(Range.equal(1L)), true))
                                .build()),
                ImmutableMap.<String, Comparable<?>>builder()
                        .put("b", "value")
                        .build());
    }

    @Test
    public void testSingleValuesMapToDomain()
            throws Exception
    {
        Assert.assertEquals(
                Domains.singleValuesMapToDomain(
                        ImmutableMap.<String, Comparable<?>>builder()
                                .put("a", 1L)
                                .put("b", "value")
                                .put("c", 0.01)
                                .put("d", true)
                                .build()),
                ImmutableMap.<String, Domain<?>>builder()
                        .put("a", Domain.single(1L))
                        .put("b", Domain.single("value"))
                        .put("c", Domain.single(0.01))
                        .put("d", Domain.single(true))
                        .build());
    }
}
