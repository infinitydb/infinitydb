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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class InfinityDBSimpleRestClient {
    // Not URL quoted, unlike the prefix.
    final String host;
    String userName;
    String passWord;

    public InfinityDBSimpleRestClient(String host) {
        if (host.endsWith("/"))
            host = host.substring(0, host.length() - 1);
        this.host = host;
    }

    public void setUserNameAndPassWord(String userName, String passWord) {
        this.userName = userName;
        this.passWord = passWord;
    }

    /*
     * Returns application/json if there is no Blob there, otherwise the Blob.
     * The existence of the Blob is determined by whether there is a
     * com.infinitydb.Blob attribute and its inner parts on that prefix.
     */
    public Blob get(Object... prefix) throws IOException {
        return command("GET", null, null, prefix);
    }

    /*
     * Like get but insists on JSON, so a Blob becomes com.infinitydb.Blob etc.
     * That allows you to get other things with suffixes
     * on that prefix other than just the Blob's suffixes.
     */
    public Blob getAsJson(Object... prefix) throws IOException {
        return command("GET", "as-json", null, prefix);
    }

    public Blob post(Blob Blob, Object... prefix) throws IOException {
        return command("POST", null, Blob, prefix);
    }

    public Blob executeQuery(String interfaceName, String methodName, Blob Blob)
            throws IOException {
        return command("POST", "execute-query", Blob,
                interfaceName, methodName);
    }

    public Blob executeGetBlobQuery(String interfaceName, String methodName)
            throws IOException {
        return command("GET", "execute-get-Blob-query", null,
                interfaceName, methodName);
    }

    public Blob executePutBlobQuery(String interfaceName, String methodName,
            Blob Blob) throws IOException {
        return command("POST", "execute-put-Blob-query", Blob,
                interfaceName, methodName);
    }

    private Blob command(String method, String action, Blob blob,
            Object... prefix) throws IOException {
        if (method.equalsIgnoreCase("GET") && blob != null)
            throw new RuntimeException(
                    "Method GET is not compatible with sending a Blob");
        String urlString = getQuotedUrl(host, prefix);
        if (action != null)
            urlString = urlString + "?action=" + action;
        URL url = new URL(urlString);
        HttpURLConnection urlConnection =
                (HttpURLConnection)url.openConnection();
        urlConnection.setRequestMethod(method);
        urlConnection.setDoInput(true);
        if (userName != null) {
            String credentials = userName + ":" + passWord;
            String encodedCredentials =
                    Base64.getEncoder().encodeToString(credentials.getBytes());
            urlConnection.setRequestProperty("Authorization",
                    "Basic " + encodedCredentials);
        }
        if (method.equalsIgnoreCase("POST") && blob != null) {
            urlConnection.setDoOutput(true);
        }
        urlConnection.connect();
        // System.out.println("Connection: " +
        // urlConnection.getHeaderField("Connection"));
        if (method.equalsIgnoreCase("POST") && blob != null) {
            OutputStream out = urlConnection.getOutputStream();
            out.write(blob.data);
            out.flush();
        }
        InputStream in = urlConnection.getInputStream();
        long contentLength = urlConnection.getContentLengthLong();
        String contentType = urlConnection.getContentType();
        Blob readBlob = new Blob((int)contentLength, contentType);
        readBlob.readFully(in);
        in.close();
        urlConnection.disconnect();
        return readBlob;
    }

    /**
     * This can quote any URL path, even if it has components like new
     * EntityClass("Documentation"),"Basics",new Attribute("description"), new
     * Index(0) which becomes
     * (host)/demo/readonly/Documentation/%22Basics%22/description/%5B0%5D. It
     * does the components separately, then concatenates with / properly.
     * 
     * @param host
     *            can have any slashes which are not quoted, but the final slash
     *            is optional.
     * @param components
     *            a varargs series of either Object or Object[]. Typically these
     *            will be EntityClass, Attribute, Index, numbers, booleans etc.
     *            for the 12 types.
     * @throws UnsupportedEncodingException
     * @throws MalformedURLException
     */
    public static String getQuotedUrl(String host, Object... components)
            throws UnsupportedEncodingException, MalformedURLException {
        StringBuffer sb = new StringBuffer();
        for (Object o : components) {
            if (o instanceof Object[]) {
                for (Object c : ((Object[])o)) {
                    if (c instanceof String)
                        c = JsonParser.convertToJsonString((String)c);
                    String component = c.toString();
                    String quoted = URLEncoder.encode(component, "UTF-8");
                    sb.append("/").append(quoted);
                }
            } else {
                if (o instanceof String)
                    o = JsonParser.convertToJsonString((String)o);
                String quoted = URLEncoder.encode(o.toString(), "UTF-8");
                sb.append("/").append(quoted);
            }
        }
        return host + sb.toString();
    }

}

/**
 * Data read and written to the REST API is in blob format, whether it is
 * JSON or images or other binary. 
 */
class Blob {
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
            if (bytesRead == -1)
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
class Meta {
    final String name;

    Meta(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Meta))
            return false;
        return ((Meta)other).name.equals(name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}

class EntityClass extends Meta {

    static final Pattern CLASS_PATTERN =
            Pattern.compile("^[A-Z][a-zA-Z0-9._-]*$");

    EntityClass(String name) {
        super(name);
        if (!isValidName(name))
            throw new RuntimeException(
                    "EntityClass must be UC, then letters,"
                            + " digits, dot, dash, and underscore.");
    }

    static boolean isValidName(String s) {
        return CLASS_PATTERN.matcher(s).matches();
    }
}

class Attribute extends Meta {

    static final Pattern ATTRIBUTE_PATTERN =
            Pattern.compile("^[a-z][a-zA-Z0-9._-]*$");

    Attribute(String name) {
        super(name);
        if (!isValidName(name))
            throw new RuntimeException("Attribute must be LC, then letters,"
                    + " digits, dot, dash, and underscore.");
    }

    static boolean isValidName(String s) {
        return ATTRIBUTE_PATTERN.matcher(s).matches();
    }
}

/**
 * These are special Long's that show up in Items at the location
 * where the JSON should have a list. For example, to index into
 * a list with the URL path, you place an Index(...) into the path.
 */
class Index {
    long index;

    Index(long index) {
        this.index = index;
    }
    
    long getIndex() {
        return index;
    }

    Index(String s) {
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
        if (!(other instanceof Index))
            return false;
        return ((Index)other).index == index;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(index);
    }
}

class ByteArray {

    static final String HEX = "0123456789ABCDEF";

    byte[] bytes;

    ByteArray(byte[] bytes) {
        this.bytes = bytes;
    }

    // Parse
    ByteArray(String s) {
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
        if (!(other instanceof ByteArray))
            return false;
        return Arrays.equals(((ByteArray)other).bytes, bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}

// Rare. This is a ByteArray that sorts like a string.
class ByteString {
    private byte[] bytes;

    ByteString(byte[] bytes) {
        this.bytes = bytes;
    }

    ByteString(String s) {
        if (!s.startsWith("ByteString(") || !s.endsWith(")"))
            throw new RuntimeException(
                    "Cannot parse ByteString component: " + s);
        s = s.substring("ByteString(".length(), s.length() - 1);
        bytes = ByteArray.fromHexString(s);
    }

    @Override
    public String toString() {
        return "ByteString(" + ByteArray.toHexString(bytes) + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ByteString))
            return false;
        return Arrays.equals(((ByteString)other).bytes, bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}

// Not used for Blobs, but Clobs, which are rare
class CharArray {
    final char[] chars;

    CharArray(char[] chars) {
        this.chars = chars;
    }

    // double-quotes are optional on the ends of the chars string.
    // We will take off the underscore first in the JSONParser.
    CharArray(String chars) {
        if (!chars.startsWith("Chars(") || !chars.endsWith(")"))
            throw new RuntimeException(
                    "Cannot parse Chars component: " + chars);
        chars = chars.substring("Chars(".length(), chars.length() - 1);
        String unescaped = JsonParser.unescapeJsonString(chars);
        this.chars = unescaped.toCharArray();
    }

    @Override
    public String toString() {
        return "Chars(" + JsonParser.convertToJsonString(chars) + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CharArray))
            return false;
        return Arrays.equals(((CharArray)other).chars, chars);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(chars);
    }

}

/**
 * This a special parser for InfinityDB 'underscore quoted' JSON. It creates a
 * tree of JsonElements with JsonObject, JsonList, and JsonValue subclasses,
 * which are easy to work with because they implement java.util.Map.
 * 
 * We need to encapsulate the 12 data types into JSON keys, which are always
 * strings, so we use a trick: an initial underscore says we have a non-string
 * type. If you actually want a string after all, it stays unchanged except that
 * if it already has an underscore, we 'stuff' an additional one at front, and
 * that is removed later during parsing.
 * 
 * To make a nicer-looking format, we can also turn off the underscore quoting
 * and leave all of the data types in plain form as keys and values, but it is
 * of course not parseable as standard JSON.
 */
class JsonParser {
    final List<String> jsonTokens;
    int pos;

    private static final SimpleDateFormat ISO_SIMPLE_DATE_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    private static final Pattern ISO_DATE_PATTERN = Pattern.compile(
            "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?(Z|[+-]\\d{2}:\\d{2})");

    static class JsonTokenizer {
        final String s;
        int pos;
        boolean isUnderscoreQuoting;

        JsonTokenizer(String s) {
            this.s = s;
        }
        // Turn off the default underscoreQuoting to get the
        // special 'extended' JSON format that is simpler to read.
        // It does  not go on the wire though.
        JsonTokenizer(String s, boolean isUnderscoreQuoting) {
            this.s = s;
            this.isUnderscoreQuoting = isUnderscoreQuoting;
        }

        // An initial pass that locates the tokens
        List<String> tokenize() {
            List<String> tokens = new ArrayList<>();
            whiteSpaces();
            int beforePos = 0;
            while (jsonToken()) {
                tokens.add(s.substring(beforePos,  pos));
                whiteSpaces();
                beforePos = pos;
            }
            return tokens;
        }

        /*
         * The isoDate is for extended JSON in InfinityDB as a key. Match it
         * first, because looks like it is separate tokens due to the contained
         * ':'.
         */
        boolean jsonToken() {
            return isoDate() 
                    || stringToken() 
                    || charSet("{}[],:") 
                    || longToken();
        }
        
        boolean any() {
            if (pos < s.length()) {
                pos++;
                return true;
            }
            return false;
        }
        
        boolean longToken() {
            if (!longTokenChar())
                return false;
            while (longTokenChar()) {
            }
            return true;
        }
        
        // The fact that we consider ':' a separator is tricky in the case of
        // keys being dates in the Extended InfinityDB JSON format.
        boolean longTokenChar() {
            return !lookaheadWhiteSpace() && !lookaheadCharSet(",:[]{}") && any();
        }

        // Match at least one 
        boolean whiteSpaces() {
            if (!lookaheadWhiteSpace())
                return false;
            while (lookaheadWhiteSpace()) {
                if (!any())
                    return true;
            }
            return true;
        }
        // Doesn't move
        boolean lookaheadWhiteSpace() {
            return pos < s.length() && s.charAt(pos) <= ' ';
        }

        boolean lookaheadCharSet(String charSet) {
            int beforePos = pos;
            boolean matched = charSet(charSet);
            pos = beforePos;
            return matched;
        }

        boolean charSet(String charSet) {
            if (pos >= s.length())
                return false;
            for (int i = 0; i < charSet.length(); i++) {
                if (charSet.charAt(i) == s.charAt(pos)) {
                    pos++;
                    return true;
                }
            }
            return false;
        }
        
        boolean digit() {
            if (pos < s.length() && s.charAt(pos) >= '0' && s.charAt(pos) <= '9') {
                pos++;
                return true;
            }
            return false;
        }
        
        // Move forwards only if at least n digits are matched
        boolean digits(int n) {
            int beforePos = pos;
            if (!digit())
                return false;
            // start at 1
            for (int i = 1; i < n; i++) {
                if (!digit()) {
                    pos = beforePos;
                    return false;
                }
            }
            return true;
        }
        
        boolean match(char c) {
            if (pos < s.length() && s.charAt(pos) == c) {
                pos++;
                return true;
            }
            return false;
        }
        boolean match(String s) {
            if (this.s.substring(pos, this.s.length()).startsWith(s)) {
                pos += s.length();
                return true;
            }
            return false;
        }
        
        // like match but don't move
        boolean lookahead(char c) {
            return pos < s.length() && s.charAt(pos) == c;
        }
        
        // like match but don't move
        boolean lookahead(String s) {
            return this.s.substring(pos, this.s.length()).startsWith(s);
        }

        // We don't use regex because that is probably slow, but
        // also it wants to have a $ so it matches completely,
        // yet we are not giving it a pre-terminated string.
        boolean isoDate() {
            int beforePos = pos;
            if (digits(4) && match('-') 
                    && digits(2) && match('-') 
                    && digits(2) && match('T')
                    && digits(2) && match(':')
                    && digits(2) && match(':')
                    && digits(2) 
                    && millis() && timeZone())
                return true;
            pos = beforePos;
            return false;
        }
        boolean millis() {
            if (match('.'))
                return digits(3);
            return true;
        }
        
        boolean timeZone() {
            if (match('-') || match('+'))
                return digits(2) && match(':') && digits(2);
            return match('Z');
        }
        
        boolean stringToken() {
            if (!match('"'))
                return false;
            while (!match('"')) {
                if (!quotedChar())
                    return false;
            }
            return true;
        }
        boolean quotedChar() {
            if (match('\\'))
                any();
            return any();
        }
    }

    boolean isUnderscoreQuoting = true;
    
    public JsonParser(String json) {
        this.jsonTokens = new JsonTokenizer(json).tokenize();
    }
    public JsonParser(String json, boolean isUnderscoreQuoting) {
        this.isUnderscoreQuoting = isUnderscoreQuoting;
        this.jsonTokens = new JsonTokenizer(json, isUnderscoreQuoting).tokenize();
    }

    /*
     * Parsing is easy once we have tokenized it. We create a tree of
     * JsonElement, the superclass of JsonObject, JsonList, and JsonValue. They
     * all implement java.util.Map, so they are easy to deal with.
     */
    JsonElement parse() {
        if (pos >= jsonTokens.size())
            throw new RuntimeException(
                    "JSON string terminated without complete parse");
        if (match("{")) {
            JsonObject jsonObject = new JsonObject();
            // Remove the '_' from the start of the keys and convert to the
            // 12 types
            while (true) {
                Object key = jsonTokens.get(pos);
                key = unQuote(key, isUnderscoreQuoting);
                if (key == null)
                    throw new RuntimeException("Unparseable token in JSON: "
                            + jsonTokens.get(pos));
                pos++;
                if (!match(":"))
                    throw new RuntimeException("Expected ':' in JSON");
                JsonElement value = parse();
                jsonObject.put(new JsonValue(key), value);
                if (match(","))
                    continue;
                if (match("}"))
                    break;
                throw new RuntimeException("Missing } in JSON");
            }
            return jsonObject;
        } else if (match("[")) {
            JsonList jsonList = new JsonList();
            while (true) {
                JsonElement o = parse();
                jsonList.add(o);
                if (match(","))
                    continue;
                if (match("]"))
                    break;
                throw new RuntimeException("Missing ] in JSON");
            }
            return jsonList;
        } else {
            String t = jsonTokens.get(pos);
            pos++;
            Object o = unQuote(t, isUnderscoreQuoting);
            return new JsonValue(o);
        }
    }

    boolean match(String s) {
        if (pos < jsonTokens.size() && jsonTokens.get(pos).equals(s)) {
            pos++;
            return true;
        }
        return false;
    }

    public static String unescapeJsonString(String jsonString) {
        // Remove surrounding double quotes
        if (jsonString.startsWith("\"") && jsonString.endsWith("\"")) {
            jsonString = jsonString.substring(1, jsonString.length() - 1);
        }

        StringBuilder unescapedString = new StringBuilder();
        boolean isEscaped = false;

        for (int i = 0; i < jsonString.length(); i++) {
            char c = jsonString.charAt(i);

            if (isEscaped) {
                switch (c) {
                case '\"' :
                    unescapedString.append('\"');
                    break;
                case '\\' :
                    unescapedString.append('\\');
                    break;
                case '/' :
                    unescapedString.append('/');
                    break;
                case 'b' :
                    unescapedString.append('\b');
                    break;
                case 'f' :
                    unescapedString.append('\f');
                    break;
                case 'n' :
                    unescapedString.append('\n');
                    break;
                case 'r' :
                    unescapedString.append('\r');
                    break;
                case 't' :
                    unescapedString.append('\t');
                    break;
                case 'u' :
                    // Unicode escape sequence
                    if (i + 4 < jsonString.length()) {
                        String unicodeHex =
                                jsonString.substring(i + 1, i + 5);
                        try {
                            int unicodeValue =
                                    Integer.parseInt(unicodeHex, 16);
                            unescapedString.append((char)unicodeValue);
                            i += 4; // Skip the 4 hexadecimal digits
                        } catch (NumberFormatException e) {
                            // Invalid Unicode escape sequence; treat as a
                            // literal 'u'
                            unescapedString.append('u');
                        }
                    } else {
                        // Not enough characters for a valid Unicode escape;
                        // treat as a literal 'u'
                        unescapedString.append('u');
                    }
                    break;
                default :
                    // Invalid escape sequence; treat as a literal character
                    unescapedString.append('\\').append(c);
                    break;
                }
                isEscaped = false;
            } else if (c == '\\') {
                isEscaped = true;
            } else {
                unescapedString.append(c);
            }
        }

        return unescapedString.toString();
    }

    public static String convertToJsonString(String s) {
        return convertToJsonString(s.toCharArray());
    }

    public static String convertToJsonString(char[] chars) {
        StringBuilder jsonValue = new StringBuilder();
        for (char c : chars) {
            switch (c) {
            case '\"' :
                jsonValue.append("\\\"");
                break;
            case '\\' :
                jsonValue.append("\\\\");
                break;
            case '\b' :
                jsonValue.append("\\b");
                break;
            case '\f' :
                jsonValue.append("\\f");
                break;
            case '\n' :
                jsonValue.append("\\n");
                break;
            case '\r' :
                jsonValue.append("\\r");
                break;
            case '\t' :
                jsonValue.append("\\t");
                break;
            default :
                if (c < ' ' || c > '~') {
                    // Unicode escape sequence
                    String unicodeHex = String.format("\\u%04x", (int)c);
                    jsonValue.append(unicodeHex);
                } else {
                    jsonValue.append(c);
                }
                break;
            }
        }
        return "\"" + jsonValue.toString() + "\"";
    }


    /*
     * For quoting a JSON object's key. We convert all types into a string with
     * initial '_', stuffing another if needed for strings. However, for
     * the special 'extended' non-standard JSON format, we can omit the
     * quotes around keys.
     */
    public static String qKey(Object o, boolean isUnderscoreQuoting) {
        String q = isUnderscoreQuoting ? qKeyUnderscoreQuoting(o) : qKeyExtended(o);
        return q;
    }
    
    // Standard JSON string keys, but with underlines at front.
    public static String qKeyUnderscoreQuoting(Object o) {
        if (o == null) {
            throw new RuntimeException(
                    "Cannot have a null JSON key in underscore-quoting");
        } else if (o instanceof Long) {
            return "_" + o;
        } else if (o instanceof Boolean) {
            return o == Boolean.TRUE ? "_true" : "_false";
        } else if (o instanceof Double) {
            String n = o.toString();
            return "_" + (n.contains(".") ? n : n + ".0");
        } else if (o instanceof Float) {
            String n = o.toString();
            // We add the 'f' but Java does not.
            return "_" + (n.contains(".") ? n : n + ".0") + "f";
        } else if (o instanceof String) {
            String s = (String)o;
            // 'Stuff' an extra '_' if one is already there
            return s.startsWith("_") ? "_" + s : s;
        } else if (o instanceof Date) {
            return "_" + ISO_SIMPLE_DATE_FORMAT.format((Date)o);
        } else if (o instanceof byte[]) {
            return new ByteArray((byte[])o).toString();
        } else if (o instanceof char[]) {
            return new CharArray((char[])o).toString();
        } else if (o instanceof EntityClass
                || o instanceof Attribute
                || o instanceof Index
                || o instanceof ByteArray
                || o instanceof CharArray
                || o instanceof ByteString) {
            return "_" + o;
        }
        throw new RuntimeException("Unrecognized type: cannot "
                    + "convert to JSON key or value: " + o);
    }

    // Our special 'extended' JSON format which minimizes quotes in keys
    public static String qKeyExtended(Object o) {
        if (o == null) {
            throw new RuntimeException(
                    "Cannot have a null JSON key in underscore-quoting");
        } else if (o instanceof Long) {
            return o.toString();
        } else if (o instanceof Float) {
            String n = o.toString();
            // We use the decimal point and trailing 'f' to indicate floats
            // We add the 'f' but Java does not.
            return "" + (n.contains(".") ? n : n + ".0") + "f";
        } else if (o instanceof Double) {
            // We use the decimal point to indicate doubles
            String n = o.toString();
            return "" + (n.contains(".") ? n : n + ".0");
        } else if (o instanceof String) {
            return JsonParser.convertToJsonString((String)o);
        } else if (o instanceof Date) {
            return ISO_SIMPLE_DATE_FORMAT.format((Date)o);
        } else if (o instanceof byte[]) {
            return new ByteArray((byte[])o).toString();
        } else if (o instanceof char[]) {
            return new CharArray((char[])o).toString();
        } else {
            return o.toString();
        }
    }

    // For quoting a JSON object's value.
    public static Object qValue(Object o, boolean isUnderscoreQuoting) {
        if (o == null || o instanceof Double || o instanceof Boolean) {
            return o;
        } else {
            return qKey(o, isUnderscoreQuoting);
        }
    }

    // This works for key or value.
    public static Object unQuote(Object o, boolean isUnderscoreQuoting) {
        try {
            if (!(o instanceof String))
                return o;
            String s = (String)o;
            if (s.length() == 0)
                return "";
            else if (s.equals("null"))
                return null;
            else if (s.equals("true"))
                return true;
            else if (s.equals("false"))
                return false;
            if (!isUnderscoreQuoting) {
                // The special 'extended' format where keys may not need double quotes.
                if (s.startsWith("\"")) {
                    s = unescapeJsonString(s);
                    return s;
                }
            } else {
                s = unescapeJsonString(s);
                if (s.length() == 0)
                    return "\"\"";
                if (s.charAt(0) != '_') {
                    // NOTE a key should never be null
                    return s.equals("null") ? null : s;
                } else if (s.startsWith("__")) {
                    return s.substring(1);
                }
                // it is a string starting with a single _
                s = s.substring(1);
                if (s.length() == 0) {
                    throw new RuntimeException(
                            "Cannot underscore-unquote a lone underscore");
                }
            }
            if (s.equals("null")) {
                return null;
            } else if (s.equals("true")) {
                return true;
            } else if (s.equals("false")) {
                return false;
            } else if (s.charAt(0) >= '0' && s.charAt(0) <= '9') {
                if (ISO_DATE_PATTERN.matcher(s).matches()) {
                    return ISO_SIMPLE_DATE_FORMAT.parse(s);
                } else if (!s.contains(".")) {
                    return Long.parseLong(s);
                } else if (s.endsWith("f")) {
                    return Float.parseFloat(s.substring(0, s.length() - 1));
                } else {
                    return Double.parseDouble(s);
                }
            } else if (EntityClass.isValidName(s)) {
                return new EntityClass(s);
            } else if (Attribute.isValidName(s)) {
                return new Attribute(s);
            } else if (s.startsWith("Bytes(")) {
                return new ByteArray(s);
            } else if (s.startsWith("ByteString(")) {
                return new ByteString(s);
            } else if (s.startsWith("Chars(")) {
                return new CharArray(s);
            }
            // Can't handle byte[], char[]
            throw new RuntimeException(
                    "Cannot underscore-unquote string: " + s);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}

/**
 * The output of parsing is a tree of these. There are three subclasses, for
 * objects, lists, and values. They are easy to scan and construct because they
 * implement Map, and therefore have many features.
 */
abstract class JsonElement implements
        Map<JsonElement, JsonElement>, Iterable<JsonElement> {

    public abstract Object value();

    public abstract boolean isList();

    public abstract boolean isObject();

    public abstract boolean isValue();

    // Can't iterate as a list by default. You can iterate a JsonList as
    // if
    // it were a JsonObject though, because the keys are Longs!
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
            writeJson(writer, true, 0);
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Cannot do toString() on JsonElement", e);
        }
    }

    public String toStringAsExtendedFormat() {
        try {
            CharArrayWriter writer = new CharArrayWriter();
            writeJson(writer, false, 0);
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Cannot do toString() on JsonElement", e);
        }
    }

    // Write as JSON underscore-quoted or not
    public void writeJson(Writer writer, boolean isUnderscoreQuoting)
            throws IOException {
        writeJson(writer, isUnderscoreQuoting, 0);
    }

    // This loops over both objects and lists the same, because they are
    // both Maps. For the lists, the key is the index.
    void writeJson(Writer writer, boolean useUnderscoreQuoting, int depth) throws IOException {
        if (isValue()) {
            writer.write(underscoreQuote(useUnderscoreQuoting));
            return;
        }
        boolean isFirst = true;
        writer.write(isList() ? "[\r\n" : "{\r\n");
        for (JsonElement v : this) {
            JsonElement e = get(v);
            if (!isFirst)
                writer.write(",\r\n");
            isFirst = false;
            writer.write(indent(depth + 1));
            if (isObject())
                writer.write(v.underscoreQuote(useUnderscoreQuoting) + ": ");
            if (e != null)
                e.writeJson(writer, useUnderscoreQuoting, depth + 1);
        }
        writer.write("\r\n" + indent(depth) + (isList() ? "]" : "}"));
    }
    
    public static String indent(int depth) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < depth; i++)
            sb.append("    ");
        return sb.toString();
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
}

class JsonObject extends JsonElement {
    private final Map<JsonElement, JsonElement> map = new HashMap<>();

    @Override
    public Set<JsonElement> keySet() {
        return map.keySet();
    }

    @Override
    public Iterator<JsonElement> iterator() {
        return map.keySet().iterator();
    }

    @Override
    public JsonElement get(Object key) {
        return map.get(key);
    }

    /**
     * The keys are JsonValues holding classes like EntityClass,
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
        map.put(new JsonValue(new EntityClass(key)), value);
        return this;
    }

    public JsonObject putAttribute(String key, JsonElement value) {
        map.put(new JsonValue(new Attribute(key)), value);
        return this;
    }

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
    public Set<Entry<JsonElement, JsonElement>> entrySet() {
        return map.entrySet();
    }

    @Override
    public int size() {
        // TODO Auto-generated method stub
        return 0;
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
    public JsonElement put(JsonElement key, JsonElement value) {
        return map.put(key, value);
    }

    @Override
    public JsonElement remove(Object key) {
        return map.remove(key);
    }

    @Override
    public void putAll(
            Map< ? extends JsonElement, ? extends JsonElement> m) {
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

/*
 * A JsonList does not implement java.util.List. It is a simulated map. You
 * can't implement both Map and List. You can use the the Maps' get(Object)
 * instead of the List get(int) so you won't even notice. The iterator returns
 * a JsonValue(Index), which can be unwrapped to a long if needed or used directly with
 * get().
 */
class JsonList extends JsonElement {
    private final List<JsonElement> list = new ArrayList<>();

    /*
     * Implement this as if it were a set Note this is a bit expensive: we
     * construct a new HashSet. Hopefully, this is rare, and you use the
     * iterator and get(), which make the key an index into the list.
     */
    @Override
    public Set<JsonElement> keySet() {
        return new HashSet<JsonElement>(list);
    }

    // Iterate it like a pseudo-map
    @Override
    public Iterator<JsonElement> iterator() {
        return new Iterator<JsonElement>() {
            int pos;

            @Override
            public boolean hasNext() {
                return pos < list.size();
            }

            @Override
            public JsonElement next() {
                return new JsonValue(new Index(pos++));
            }
        };
    }

    // NOTE we round double and float, expecting them to in fact be integer.
    @Override
    public JsonElement get(Object key) {
        if (key instanceof JsonValue)
            key = ((JsonValue)key).value();
        if (key instanceof Index) {
            long index = ((Index)key).getIndex();
            return list.get((int)index);
        } else if (key instanceof Number) {
            int index = ((Number)key).intValue();
            return list.get(index);
        }
        throw new RuntimeException("JsonList: bad type for get(): " + key);
    }

    /**
     * The element can be a JsonValue with a class like String, Long,
     * EntityClass, Attribute or similar, or else it can be any other
     * JsonElement for a possibly nested arrangement. Also, we
     * wrap a 'bare' object like EntityClass or Long in a JsonValue to add it.
     */
    public JsonList add(JsonElement e) {
        if (e instanceof JsonElement)
            list.add(e);
        else 
            list.add(new JsonValue(e));
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
    public Set<Entry<JsonElement, JsonElement>> entrySet() {
        Set<Entry<JsonElement, JsonElement>> set = new HashSet<>();
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

    @Override
    public JsonElement put(JsonElement key, JsonElement value) {
        throw new RuntimeException(
                "cannot put a key into a JsonList: " + key);
    }

    @Override
    public JsonElement remove(Object key) {
        throw new RuntimeException(
                "cannot remove a key from a JsonList: " + key);
    }

    @Override
    public void putAll(
            Map< ? extends JsonElement, ? extends JsonElement> m) {
        throw new RuntimeException("cannot user putAll() of a map int a JsonList");
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

/**
 * A single value such as an EntityClass, Attribute, ByteArray or the primitive
 * wrappers like Long, Date and so on. Strings are escaped according to
 * JavaScript rules. This can look like a Map with one entry, so
 * while iterating, you don't need to worry about single values being
 * handled differently. Everything looks like a Map, but these just
 * have no 'contents' so get() returns null.
 */
class JsonValue extends JsonElement {
    
    // Not a JsonElement such as a JsonValue. Tentatively nullable.
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
    public Set<JsonElement> keySet() {
        Set<JsonElement> set = new HashSet<>(1);
        set.add(this);
        return set;
    }

    // Iterate a single value as a single-element Map.
    @Override
    public Iterator<JsonElement> iterator() {
        List<JsonElement> list = new ArrayList<>(1);
        list.add(this);
        return list.iterator();
    }
    
    // Iterate as a list with a single value.
    @Override
    public Iterator<JsonElement> listIterator() {
        return iterator();
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
    public Set<Entry<JsonElement, JsonElement>> entrySet() {
        Set<Entry<JsonElement, JsonElement>> set = new HashSet<>(1);
        set.add(new SimpleEntry(v, null));
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
    public JsonElement put(JsonElement key, JsonElement value) {
        throw new RuntimeException("Cannot put a key/value into a JsonValue");
    }

    @Override
    public JsonElement remove(Object key) {
        throw new RuntimeException("Cannot remove a key/value from a JsonValue");
    }

    @Override
    public void putAll(Map< ? extends JsonElement, ? extends JsonElement> m) {
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
    