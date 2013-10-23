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

public enum ColumnType
{
    BOOLEAN(Boolean.class),
    LONG(Long.class),
    DOUBLE(Double.class),
    STRING(String.class);

    private final Class<?> nativeType;

    private <T extends Comparable<? super T>> ColumnType(Class<T> nativeType)
    {
        this.nativeType = Objects.requireNonNull(nativeType, "nativeType is null");
    }

    @SuppressWarnings("unchecked")
    public <T extends Comparable<? super T>> Class<T> getNativeType()
    {
        // Since we control the constructor, this cast must be correct
        return (Class<T>) nativeType;
    }
}
