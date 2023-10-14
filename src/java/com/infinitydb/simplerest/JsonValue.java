// MIT License
//
// Copyright (c) 2023 Roger L. Deran
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package com.infinitydb.simplerest;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A single value such as an IdbClass, IdbAttribute, IdbByteArray or the primitive
 * wrappers like Long, Date and so on. Strings are escaped according to
 * JavaScript rules. This can look like a Map with one entry, so
 * while iterating, you don't need to worry about single values being
 * handled differently. Everything looks like a Map, but these just
 * have no 'contents' so get() returns null.
 */
public class JsonValue extends JsonElement {
    
    // Not a JsonElement such as a JsonValue. Tentatively nullable.
    // An IdbClass, IdbAttribute, Long, Double, String, Date etc.
    private final Object v;

    /**
     * @param v may be a JsonValue or any type of the 12 types.
     */
    JsonValue(Object v) {
        this.v = v instanceof JsonValue ? ((JsonValue)v).value() : v;
        if (this.v instanceof JsonElement) {
            throw new RuntimeException("A JsonValue cannot be constructed from a"
                    + " value that is another JsonElement except for a JsonValue");
        }
    }

    // Make this look like a singleton set. Contains JsonValues.
    @Override
    public Set<JsonValue> keySet() {
        Set<JsonValue> set = new HashSet<>(1);
        set.add(this);
        return set;
    }

    // Iterate a single value as a single-element Map.
    @Override
    public Iterator<JsonValue> iterator() {
        List<JsonValue> list = new ArrayList<>(1);
        list.add(this);
        return list.iterator();
    }
    
    // Iterate as a list with a single value.
    @Override
    public Iterator<JsonElement> listIterator() {
        List<JsonElement> list = new ArrayList<>(1);
        list.add(this);
        return list.iterator();
    }

    // We have nothing 'inside'.
    @Override
    public JsonElement get(Object o) {
        return null;
    }
    
    // Not a JsonElement such as a JsonValue
    @Override
    public Object value() {
        return v;
    }

    @Override
    public boolean isList() {
        return false;
    }
    
    @Override 
    public boolean isObject() {
        return false;
    }

    @Override 
    public boolean isValue() {
        return true;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof JsonValue))
            return false;
        JsonValue otherJson = (JsonValue)other;
        return v == null ? otherJson.v == null : 
            v.equals(otherJson.v);
    }
    
    @Override
    public int hashCode() {
        return v == null ? 42 : v.hashCode();
    }

    @Override
    public Set<Entry<JsonValue, JsonElement>> entrySet() {
        Set<Entry<JsonValue, JsonElement>> set = new HashSet<>(1);
        set.add(new SimpleEntry(this, null));
        return set;
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        return v == null ? key == null : v.equals(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public JsonElement put(JsonValue key, JsonElement value) {
        throw new RuntimeException("Cannot put a key/value into a JsonValue");
    }

    @Override
    public JsonElement remove(Object key) {
        throw new RuntimeException("Cannot remove a key/value from a JsonValue");
    }

    @Override
    public void putAll(Map< ? extends JsonValue, ? extends JsonElement> m) {
        throw new RuntimeException("Cannot putAll into a JsonValue");
    }

    @Override
    public void clear() {
        throw new RuntimeException("Cannot clear a JsonValue");
    }

    // We have no values, considering this in the Map sense.
    @Override
    public Collection<JsonElement> values() {
        Set<JsonElement> set = new HashSet<>(1);
        return set;
    }
}