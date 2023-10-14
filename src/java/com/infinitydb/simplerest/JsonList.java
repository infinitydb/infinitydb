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

/*
 * A JsonList does not implement java.util.List. It is a simulated map. You
 * can't implement both Map and List. You can use the the Maps' get(Object)
 * instead of the List get(int) so you won't even notice. The iterator returns
 * a JsonValue(Index), which can be unwrapped to a long if needed or used directly with
 * get().
 */
public class JsonList extends JsonElement {
    private final List<JsonElement> list = new ArrayList<>();

    JsonList() {
    }

    JsonList(boolean isCompactTips, Object... objects) {
        insert(isCompactTips, objects);
    }

    /*
     * Implement this as if it were a set Note this is a bit expensive: we
     * construct a new HashSet. Hopefully, this is rare, and you use the
     * iterator and get(), which make the key an index into the list.
     * 
     * TODO implement an efficient simulator of a set that is just
     * a range of 0..size() - 1 longs.
     */
    @Override
    public Set<JsonValue> keySet() {
        Set ks = new HashSet<JsonValue>();
        for (int i = 0; i < list.size(); i++) {
            ks.add(new JsonValue(new Long(i)));
        }
        return ks;
    }

    // Iterate it like a pseudo-map
    @Override
    public Iterator<JsonValue> iterator() {
        return new Iterator<JsonValue>() {
            int pos;

            @Override
            public boolean hasNext() {
                return pos < list.size();
            }

            @Override
            public JsonValue next() {
                return new JsonValue(new IdbIndex(pos++));
            }
        };
    }

    // NOTE we round double and float, expecting them to in fact be integer.
    @Override
    public JsonElement get(Object key) {
        if (key instanceof JsonValue)
            key = ((JsonValue)key).value();
        long index = key instanceof IdbIndex  ? ((IdbIndex)key).getIndex() 
                   : key instanceof Number ? ((Number)key).longValue() : -1;
        return index < 0 || index >= list.size() ? null : list.get((int)index);
    }

    /**
     * The element can be a JsonValue with a class like String, Long,
     * IdbClass, Attribute or similar, or else it can be any other
     * JsonElement for a possibly nested arrangement. Also, we
     * wrap a 'bare' object like IdbClass or Long in a JsonValue to add it.
     */
    public JsonList add(Object e) {
        list.add(e instanceof JsonElement ? (JsonElement) e : new JsonValue(e));
        return this;
    }

    @Override
    public Object value() {
        return null;
    }

    @Override
    public boolean isList() {
        return true;
    }

    @Override
    public boolean isObject() {
        return false;
    }

    @Override
    public boolean isValue() {
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof JsonList))
            return false;
        return list.equals(((JsonList)other).list);
    }

    @Override
    public int hashCode() {
        return list.hashCode();
    }

    // We might not even want this.
    @Override
    public Set<Entry<JsonValue, JsonElement>> entrySet() {
        Set<Entry<JsonValue, JsonElement>> set = new HashSet<>();
        for (int i = 0; i < list.size(); i++)
            set.add(new SimpleEntry(new JsonValue(new Long(i)),
                    list.get(i)));
        return set;
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        if (key instanceof JsonValue)
            key = ((JsonValue)key).value();
        if (!(key instanceof Long))
            return false;
        return list.size() > ((Long)key).intValue();
    }

    @Override
    public boolean containsValue(Object value) {
        if (!(value instanceof JsonValue))
            value = new JsonValue(value);
        return list.contains(value);
    }

    /**
     * Put at index determined by key, which must be a JsonValue containing a
     * Long or Index. The index can be within the list or at the end, in which case we do
     * an add. This makes the list look similar to a Map, like a JsonObject.
     * 
     * TODO It might be good to allow extending the list more than one at a
     * time.
     */
    @Override
    public JsonElement put(JsonValue key, JsonElement value) {
        Object k = key.value();
        long i = -1;
        if (k instanceof Long) {
            i = ((Long)k).longValue();
        } else if (k instanceof IdbIndex) {
            i = ((IdbIndex)k).index;
        } else {
            throw new RuntimeException(
                    "Bad index type for put into a JsonList : " + k);
        }
        if (i < 0 || i > list.size())
            throw new RuntimeException(
                "Bad index for put into a JsonList : " + i);
        if (i < list.size()) {
            JsonElement oldContents = list.get((int)i);
            list.set((int)i, value);
            return oldContents;
        } else if (i == list.size()) {
            list.add(value);
            return null;
        } else { 
            throw new RuntimeException(
                    "A JsonList can not use put with indexes beyond the end: " + i);
        }
    }

    @Override
    public JsonElement remove(Object key) {
        throw new RuntimeException(
                "cannot remove a key from a JsonList: " + key);
    }

    @Override
    public void putAll(
            Map< ? extends JsonValue, ? extends JsonElement> m) {
        throw new RuntimeException("cannot use putAll() of a map int a JsonList");
    }

    @Override
    public void clear() {
        list.clear();
    }

    @Override
    public Collection<JsonElement> values() {
        return list;
    }
}