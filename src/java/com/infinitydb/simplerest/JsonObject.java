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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class JsonObject extends JsonElement {
    private final Map<JsonValue, JsonElement> map = new HashMap<>();

    JsonObject() {
    }

    JsonObject(boolean isCompactTips, Object... objects) {
        insert(isCompactTips, objects);
    }

    @Override
    public Set<JsonValue> keySet() {
        return map.keySet();
    }

    @Override
    public Iterator<JsonValue> iterator() {
        return map.keySet().iterator();
    }

    @Override
    public JsonElement get(Object key) {
        if (key instanceof JsonValue)
            return map.get(key);
        return map.get(new JsonValue(key));
    }

    /**
     * The keys are JsonValues holding classes like IdbClass,
     * Attribute, Long, String
     * 
     * @param key
     *            any JsonValue
     * @param value
     *            any JsonElement
     */
    public JsonObject put(JsonValue key, JsonElement value) {
        map.put(key, value);
        return this;
    }
    
    public JsonObject putClass(String key, JsonElement value) {
        map.put(new JsonValue(new IdbClass(key)), value);
        return this;
    }

    public JsonObject putAttribute(String key, JsonElement value) {
        map.put(new JsonValue(new IdbAttribute(key)), value);
        return this;
    }
    
    
//    public static JsonObject fromItems(List<List<Object>> items) {
//        
//    }

    @Override
    public Object value() {
        if (map.size() == 0) {
            return null;
        }
        if (map.size() == 1) {
            for (Object o : map.keySet()) {
                return o;
            }
        }
        throw new RuntimeException("get value of JSON object: " + map);
    }

    @Override
    public boolean isList() {
        return false;
    }

    @Override
    public boolean isObject() {
        return true;
    }

    @Override
    public boolean isValue() {
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof JsonObject))
            return false;
        return map.equals(((JsonObject)other).map);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @Override
    public Set<Entry<JsonValue, JsonElement>> entrySet() {
        return map.entrySet();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public JsonElement remove(Object key) {
        return map.remove(key);
    }

    @Override
    public void putAll(
            Map< ? extends JsonValue, ? extends JsonElement> m) {
        map.putAll(m);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Collection<JsonElement> values() {
        return map.values();
    }
}