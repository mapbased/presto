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

/**
 * ColumnValue is used to get around some Java generic weirdness in the SPI.
 * All SPI methods that take a type T as an argument should also have a method
 * that takes a ColumnValue<T> that does the exact same thing.
 */
public class ColumnValue<T extends Comparable<? super T>>
{
    private final T value;

    public ColumnValue(T value)
    {
        this.value = value;
    }

    public T get()
    {
        return value;
    }
}
