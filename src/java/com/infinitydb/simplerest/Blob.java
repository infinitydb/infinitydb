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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

/**
 * Data read and written to the REST API is in blob format, whether it is
 * JSON or images or other binary. 
 */
public class Blob {
    final byte[] data;
    final String contentType;

    Blob(byte[] data, String contentType) {
        this.data = data;
        this.contentType = contentType;
    }

    Blob(int dataLength, String contentType) {
        this.data = new byte[dataLength];
        this.contentType = contentType;
    }
    
    Blob(String data) throws UnsupportedEncodingException {
        this.data = data.getBytes("UTF-8");
        this.contentType = "text/plain";
    }

    Blob(String data, String contentType) throws UnsupportedEncodingException {
        this.data = data.getBytes("UTF-8");
        this.contentType = contentType;
    }

    Blob(String data, String contentType, String charSet) throws UnsupportedEncodingException {
        this.data = data.getBytes(charSet);
        this.contentType = contentType;
    }

    Blob(JsonElement jsonElement) throws IOException {
        this.data = jsonElement.toString().getBytes("UTF-8");
        this.contentType = "application/json";
    }

    int length() {
        return data.length;
    }

    String getContentType() {
        return contentType;
    }

    void readFully(InputStream in) throws IOException {
        int off = 0;
        while (true) {
            int bytesRead = in.read(data, off, data.length - off);
            if (bytesRead <= 0)
                return;
            off += bytesRead;
        }
    }

    @Override
    public String toString() {
        try {
            return new String(data, "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException("Cannot parse UTF-8 string in a Blob buffer ",e);
        }
    }
}