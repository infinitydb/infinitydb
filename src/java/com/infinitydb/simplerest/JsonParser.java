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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

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
public class JsonParser {
    final List<String> jsonTokens;
    int pos;

    static final SimpleDateFormat ISO_SIMPLE_DATE_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    private static final Pattern ISO_DATE_PATTERN = Pattern.compile(
            "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?(Z|[+-]\\d{2}:\\d{2})");

    static class JsonTokenizer {
        final String s;
        final boolean isUnderscoreQuoting;
        int pos;

        JsonTokenizer(String s) {
            this.s = s;
            this.isUnderscoreQuoting = true;
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

    /**
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
            return new IdbByteArray((byte[])o).toString();
        } else if (o instanceof char[]) {
            return new IdbCharArray((char[])o).toString();
        } else if (o instanceof IdbClass
                || o instanceof IdbAttribute
                || o instanceof IdbIndex
                || o instanceof IdbByteArray
                || o instanceof IdbCharArray
                || o instanceof IdbByteString) {
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
            return new IdbByteArray((byte[])o).toString();
        } else if (o instanceof char[]) {
            return new IdbCharArray((char[])o).toString();
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
            } else if (IdbClass.isValidName(s)) {
                return new IdbClass(s);
            } else if (IdbAttribute.isValidName(s)) {
                return new IdbAttribute(s);
            } else if (s.startsWith("Bytes(")) {
                return new IdbByteArray(s);
            } else if (s.startsWith("ByteString(")) {
                return new IdbByteString(s);
            } else if (s.startsWith("Chars(")) {
                return new IdbCharArray(s);
            }
            // Can't handle byte[], char[]
            throw new RuntimeException(
                    "Cannot underscore-unquote string: " + s);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}