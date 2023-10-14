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

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The output of parsing is a tree of these. There are three subclasses, for
 * objects, lists, and values. They are easy to scan and construct because they
 * implement Map, and therefore have many features.
 */
public abstract class JsonElement implements
        Map<JsonValue, JsonElement>, Iterable<JsonValue> {

    public abstract Object value();

    public abstract boolean isList();

    public abstract boolean isObject();

    public abstract boolean isValue();

    /**
     *  Can't iterate as a list by default. You can iterate a JsonList as
     *  if it were a JsonObject though, because the keys are Longs!
     */
    public Iterator<JsonElement> listIterator() {
        return new Iterator<JsonElement>() {

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public JsonElement next() {
                return null;
            }
        };
    }

    // Return underscore-quoted JSON, which is parseable as standard JSON
    @Override
    public String toString() {
        try {
            CharArrayWriter writer = new CharArrayWriter();
            writeJson(writer, true, 0, "");
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Cannot do toString() on JsonElement", e);
        }
    }

    public String toStringIndented() {
        try {
            CharArrayWriter writer = new CharArrayWriter();
            writeJson(writer, true, 4, "\r\n");
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Cannot do toString() on JsonElement", e);
        }
    }

    public String toStringAsExtendedFormat() {
        try {
            CharArrayWriter writer = new CharArrayWriter();
            writeJson(writer, false, 0, "");
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Cannot do toString() on JsonElement", e);
        }
    }

    public String toStringAsExtendedFormatIndented() {
        try {
            CharArrayWriter writer = new CharArrayWriter();
            writeJson(writer, false, 4, "\r\n");
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Cannot do toString() on JsonElement", e);
        }
    }

    // Write as JSON underscore-quoted, indented 4, crlf
    public void writeJson(Writer writer) throws IOException {
        writeJson(writer, true);
    }

    // Write as JSON underscore-quoted or not
    public void writeJson(Writer writer, boolean isUnderscoreQuoting)
            throws IOException {
        writeJson(writer, isUnderscoreQuoting, 4, "\r\n");
    }

    // Write as JSON underscore-quoted or not.
    // indent is 0 for single-spaced, otherwise # spaces per level
    public void writeJson(Writer writer, boolean isUnderscoreQuoting, 
            int indent, String eol) throws IOException {
        writeJson(writer, isUnderscoreQuoting, indent, eol, 0);
    }

    // This loops over both objects and lists the same, because they are
    // both Maps. For the lists, the key is the index.
    void writeJson(Writer writer, boolean useUnderscoreQuoting, 
            int indent, String eolString, int depth) throws IOException {
        if (isValue()) {
            writer.write(underscoreQuote(useUnderscoreQuoting));
            return;
        }
        boolean isFirst = true;
        writer.write((isList() ? "[" : "{") + eolString);
        for (JsonElement v : this) {
            JsonElement e = get(v);
            if (!isFirst) 
                writer.write("," + eolString);
            isFirst = false;
            indent(writer, depth + 1, indent);
            if (isObject())
                writer.write(v.underscoreQuote(useUnderscoreQuoting) + ": ");
            if (e != null)
                e.writeJson(writer, useUnderscoreQuoting, indent, eolString, depth + 1);
            else
                writer.write("null");
            }
        writer.write(eolString);
        indent(writer, depth, indent);
        writer.write((isList() ? "]" : "}"));
    }
    
    public static void indent(Writer writer, int depth, int indent) throws IOException {
        if (indent == 0) {
            writer.write(' ');
            return;
        }
        for (int i = 0; i < depth * indent; i++)
            writer.write(' ');
    }
    
    String underscoreQuote(boolean isUnderscoreQuoting) {
        Object q = value();
        if (q == null)
            return "null";
        q = isValue() ? JsonParser.qValue(value(), isUnderscoreQuoting) 
                : JsonParser.qKey(value(), isUnderscoreQuoting);
        if (isUnderscoreQuoting && q instanceof String)
            q = JsonParser.convertToJsonString((String)q);
        return q.toString();
    }

    /**
     * Insert a new Item suffix here. Of course if we are the root, then an
     * entire Item is inserted. Anywhere an Index component occurs in the item,
     * a new list is created or an existing list is indexed into.
     * 
     * if isCompactingTips is false, Items end with null.
     * 
     * @param item
     *            an array of Objects which may be Components or JsonValues,
     *            including Index components. If they are indexes, we can't
     *            write beyond the list end but we can add one at the end: see
     *            JsonList.put().
     */
    public void insert(boolean isCompactTips, Object... item) {
        JsonElement outer = this;
        for (int i = 0; i < item.length; i++) {
            Object o = item[i];
            JsonValue jsonValue = o instanceof JsonValue
                    ? (JsonValue)o : new JsonValue(o);
            Object value = jsonValue.value();
            JsonElement inner = outer.get(jsonValue);
            if (inner == null) {
                if (isCompactTips && i == item.length - 2) {
                    Object oInner = item[item.length - 1];
                    JsonValue innerJsonValue = oInner instanceof JsonValue
                            ? (JsonValue)oInner : new JsonValue(oInner);
                    outer.put(jsonValue, innerJsonValue);
                    return;
                }
                if (i == item.length - 1) {
                    outer.put(jsonValue, null);
                    return;
                }
                if (i + 1 < item.length) {
                    inner = item[i + 1] instanceof IdbIndex 
                        ? new JsonList() : new JsonObject();
                }
                outer.put(jsonValue, inner);
                outer = inner;
            }
        }
    }
    
    public List<List<Object>> flattenToList() {
        List<List<Object>> suffixes = new ArrayList<>();
        if (isEmpty())
            return suffixes;
        for (JsonValue k : this) {
            JsonElement v = get(k);
            if (v == null) {
                List<Object> suffix = new ArrayList<Object>();
                suffix.add(k);
                suffixes.add(suffix);
                continue;
            }
            List<List<Object>> nestedSuffixes = v.flattenToList();
            for (List<Object> nestedSuffix : nestedSuffixes) {
                List<Object> concatenatedSuffix = new ArrayList<>(nestedSuffix);
                concatenatedSuffix.add(0, k);
                suffixes.add(concatenatedSuffix);
            }
        }
        return suffixes;
    }
    
    public static JsonElement unflattenFromList(boolean isCompactingTips, List<List<Object>> list) {
        JsonElement e = null;
        // This is horrible but no choice. We have to figure out if this
        // list of Items is a list in the JSON at the outer level.
        if (list.size() > 0 && list.get(0) != null && list.get(0).get(0) instanceof IdbIndex)
            e = new JsonList();
        else
            e = new JsonObject();
        for (List<Object> item : list) {
            e.insert(isCompactingTips, item.toArray());
        }
        return e;
    }
    
    
}