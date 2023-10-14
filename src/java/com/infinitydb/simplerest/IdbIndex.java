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

/**
 * These are special Long's that show up in Items at the location
 * where the JSON should have a list. For example, to index into
 * a list with the URL path, you place an Index(...) into the path.
 */
public class IdbIndex extends IdbPrimitive {
    long index;

    IdbIndex(long index) {
        this.index = index;
    }
    
    long getIndex() {
        return index;
    }

    IdbIndex(String s) {
        if (!s.startsWith("[") || !s.endsWith("]"))
            throw new RuntimeException(
                    "Cannot parse index component: " + s);
        s = s.substring(1, s.length() - 1);
        index = Long.parseLong(s);
    }

    // In an Item, list indexes look like this.
    @Override
    public String toString() {
        return "[" + index + "]";
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof IdbIndex))
            return false;
        return ((IdbIndex)other).index == index;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(index);
    }
}