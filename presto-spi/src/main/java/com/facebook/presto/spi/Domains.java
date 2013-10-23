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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class Domains
{
    private Domains()
    {
    }

    @SafeVarargs
    public static <K> Map<K, Domain<?>> intersectDomainMaps(Map<K, Domain<?>>... domainMaps)
    {
        Map<K, Domain<?>> intersectedDomains = new HashMap<>();
        for (Map<K, Domain<?>> domainMap : domainMaps) {
            for (Map.Entry<K, Domain<?>> entry : domainMap.entrySet()) {
                Domain<?> domain = entry.getValue();
                Domain<?> existingDomain = intersectedDomains.get(entry.getKey());
                if (existingDomain != null) {
                    domain = domain.intersect(existingDomain);
                }
                intersectedDomains.put(entry.getKey(), domain);
            }
        }
        return intersectedDomains;
    }

    @SafeVarargs
    public static <K> Map<K, Domain<?>> unionDomainMaps(Map<K, Domain<?>>... domainMaps)
    {
        // Only keys common to all domainMaps will be in the final unionized domainMap.
        // This is because a key not found in a single domainMap is equivalent
        // to that key having no constraints, and thus the union result of the domain on
        // that key will also have no bounds (which can be represented by omitting the key from the map).
        Set<K> intersectedKeys = new HashSet<>();
        for (int i = 0; i < domainMaps.length; i++) {
            if (i == 0) {
                intersectedKeys.addAll(domainMaps[i].keySet());
            }
            else {
                intersectedKeys.retainAll(domainMaps[i].keySet());
            }
        }

        Map<K, Domain<?>> unionDomains = new HashMap<>();
        for (Map<K, Domain<?>> domainMap : domainMaps) {
            for (K key : intersectedKeys) {
                Domain<?> domain = domainMap.get(key);
                Domain<?> existingDomain = unionDomains.get(key);
                unionDomains.put(key, (existingDomain == null) ? domain : domain.union(existingDomain));
            }
        }
        return unionDomains;
    }

    /**
     * Extract all Domains that contain exactly one value and return them as a Map of keys
     * to raw Comparable objects.
     */
    public static <K> Map<K, Comparable<?>> extractSingleValues(Map<K, Domain<?>> domainMap)
    {
        Map<K, Comparable<?>> singleValues = new HashMap<>();
        for (Map.Entry<K, Domain<?>> entry : domainMap.entrySet()) {
            Domain<?> domain = entry.getValue();
            if (domain.getRanges().size() == 1 && !domain.isNullAllowed()) {
                Range<? extends Comparable<?>> range = domain.getRanges().iterator().next();
                if (range.isSingleValue()) {
                    singleValues.put(entry.getKey(), range.getLow().getValue());
                }
            }
        }
        return singleValues;
    }

    /**
     * Convert a map of keys to raw comparable objects into the corresponding domainMap.
     */
    public static <K> Map<K, Domain<?>> singleValuesMapToDomain(Map<K, ? extends Comparable<?>> singleValues)
    {
        Map<K, Domain<?>> domainMap = new HashMap<>();
        for (Map.Entry<K, ? extends Comparable<?>> entry : singleValues.entrySet()) {
            domainMap.put(entry.getKey(), singleValueDomainFromComparable(entry.getValue()));
        }
        return domainMap;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Comparable<? super T>> Domain<T> singleValueDomainFromComparable(Comparable<?> value)
    {
        return Domain.single((T) value);
    }

    public static <T extends Comparable<? super T>> T valueAsType(Class<T> type, Object value)
    {
        if (value != null && !type.isInstance(value)) {
            throw new IllegalArgumentException(String.format("Can not cast value (%s) into type %s", value, type));
        }
        return type.cast(value);
    }

    public static <T extends Comparable<? super T>> ColumnValue<T> valueToColumnValue(Class<T> type, Object value)
    {
        return new ColumnValue<>(valueAsType(type, value));
    }

    @SuppressWarnings("unchecked")
    public static <T extends Comparable<? super T>> Class<T> extractType(T value)
    {
        return (Class<T>) value.getClass();
    }
}
