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

import java.util.Arrays;

// Rare. This is a ByteArray that sorts like a string.
public class IdbByteString extends IdbByteArrayBase {

    IdbByteString(byte[] bytes) {
        super(bytes);
    }

    IdbByteString(String s) {
        if (!s.startsWith("ByteString(") || !s.endsWith(")"))
            throw new RuntimeException(
                    "Cannot parse ByteString component: " + s);
        s = s.substring("ByteString(".length(), s.length() - 1);
        bytes = IdbByteArray.fromHexString(s);
    }

    @Override
    public String toString() {
        return "ByteString(" + IdbByteArray.toHexString(bytes) + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof IdbByteString))
            return false;
        return Arrays.equals(((IdbByteString)other).bytes, bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}