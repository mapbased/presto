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

    @SuppressWarnings("unchecked")
    public static <T extends Comparable<? super T>> Domain<T> uncheckedIntersect(Domain<T> left, Domain<?> right)
    {
        return left.intersect((Domain<T>) right);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Comparable<? super T>> Domain<T> uncheckedUnion(Domain<T> left, Domain<?> right)
    {
        return left.union((Domain<T>) right);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Comparable<? super T>> boolean uncheckedIncludesValue(Domain<T> domain, Comparable<?> value)
    {
        return domain.includesValue((T) value);
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
                     domain = Domains.uncheckedIntersect(domain, existingDomain);
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
         // This is because a key not represented in a single domainMap is equivalent
         // to that key having no bounds, and thus the union result of the domain on
         // that key will also have no bounds, and thereby this key does not need to be
         // considered.
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
                 assert domain != null;

                 Domain<?> existingDomain = unionDomains.get(key);

                 unionDomains.put(key, (existingDomain == null) ? domain : Domains.uncheckedUnion(domain, existingDomain));
             }
         }
         return unionDomains;
     }

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

     public static <K> Map<K, Domain<?>> singleValuesMapToDomain(Map<K, ? extends Comparable<?>> singleValues)
     {
         Map<K, Domain<?>> domainMap = new HashMap<>();
         for (Map.Entry<K, ? extends Comparable <?>> entry : singleValues.entrySet()) {
             Comparable<?> value = entry.getValue();
             domainMap.put(entry.getKey(), uncheckedComparableToDomain(value));
         }
         return domainMap;
     }

    @SuppressWarnings("unchecked")
     private static <T extends Comparable<? super T>> Domain<T> uncheckedComparableToDomain(Comparable<?> value)
     {
         return Domain.singleValue((T) value);
     }
}
