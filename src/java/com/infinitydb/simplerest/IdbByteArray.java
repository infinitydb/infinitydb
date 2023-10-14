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

public class IdbByteArray extends IdbByteArrayBase {

    static String HEX = "0123456789ABCDEF";

    IdbByteArray(byte[] bytes) {
        super(bytes);
    }

    // Parse
    IdbByteArray(String s) {
        if (!s.startsWith("Bytes(") || !s.endsWith(")"))
            throw new RuntimeException(
                    "Cannot parse ByteArray component: " + s);
        s = s.substring("Bytes(".length(), s.length() - 1);
        bytes = fromHexString(s);
    }

    @Override
    public String toString() {
        return "Bytes(" + toHexString(bytes) + ")";
    }

    public static byte[] fromHexString(String s) {
        byte[] bytes = new byte[(s.length() + 1) / 3];
        int j = 0;
        for (int i = 0; i < s.length();) {
            char top = s.charAt(i++);
            char bottom = s.charAt(i++);
            // Skip underscore
            i++;
            int iTop = hexDigitParse(top);
            int iBottom = hexDigitParse(bottom);
            byte b = (byte)((iTop << 4) + (iBottom & 0xf));
            bytes[j++] = b;
        }
        return bytes;
    }

    private static int hexDigitParse(char c) {
        return c >= '0' && c <= '9' ? c - '0'
                : c >= 'A' && c <= 'Z' ? c - 'A' : '_';
    }

    public static String toHexString(byte[] bytes) {
        StringBuffer sb = new StringBuffer();
        boolean isFirst = true;
        for (int i = 0; i < bytes.length; i++) {
            if (!isFirst)
                sb.append("_");
            isFirst = false;
            char top = HEX.charAt((bytes[i] >> 4) & 0xf);
            char bottom = HEX.charAt(bytes[i] & 0xf);
            sb.append(top).append(bottom);
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof IdbByteArray))
            return false;
        return Arrays.equals(((IdbByteArray)other).bytes, bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}