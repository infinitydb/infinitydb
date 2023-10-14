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
 * The 12 data types have wrappers to make them easy to deal with. If you want
 * to work with an attribute you can construct one and give it its name, then
 * refer to it elsewhere.
 * 
 * If you are running in a JVM that has infinitydb.jar available, then there are
 * standard classes like these already available, so these are redundant, and
 * you probably want to use the RestConnection class to parse into ItemSpaces
 * anyway. The primitive wrappers like Long, String etc are used as-is.
 */
public class IdbMeta {
    final String name;

    IdbMeta(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof IdbMeta))
            return false;
        return ((IdbMeta)other).name.equals(name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}