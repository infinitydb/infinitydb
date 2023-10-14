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

import java.util.Date;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test the InfinityDB REST access code for Java. See infinitydb.com,
 * and boilerbay.com.
 */
public class InfinityDBSimpleRestClientTest {
    static final String INFINITYDB_HOST =
          "https://infinitydb.com:37411/infinitydb/data/demo/writeable";
    static final String LOCAL_HOST =
            "http://localhost:37411/infinitydb/data/demo/writeable";
    static final String HOST = INFINITYDB_HOST;

    static InfinityDBSimpleRestClient idb;
    
    static InfinityDBSimpleRestClient getClient( ) {
        if (idb != null)
            return idb;
        System.out.println("host=" + HOST);
        idb = new InfinityDBSimpleRestClient(HOST);
        idb.setUserNameAndPassWord("testUser", "db");
        return idb;
    }
    
    @Test
    public void testJson() {
        JsonElement e0 = new JsonObject(false, new IdbAttribute("att"), new Long(5), "hi");
        
        JsonElement o0 = new JsonObject();
        o0.put(new JsonValue("hi"), null);
        JsonElement o = new JsonObject();
        o.put(new JsonValue(new Long(5)), o0);
        JsonElement o1 = new JsonObject();
        o1.put(new JsonValue(new IdbAttribute("att")), o);
        Assert.assertEquals(e0, o1);
    }
    @Test
    public void testJsonCompacted() {
        JsonElement e0 = new JsonObject(true, new IdbAttribute("att"), new Long(5), "hi");

        JsonElement o = new JsonObject();
        o.put(new JsonValue(new Long(5)), new JsonValue("hi"));
        JsonElement o1 = new JsonObject();
        o1.put(new JsonValue(new IdbAttribute("att")), o);
        Assert.assertEquals(e0, o1);
    }
    @Test
    public void testJsonList() {
        JsonElement e0 = new JsonObject(false, new IdbAttribute("att"), new IdbIndex(0), "hi");
        
        JsonElement o0 = new JsonObject();
        o0.put(new JsonValue("hi"), null);
        JsonList o = new JsonList();
        o.add(o0);
        JsonElement o1 = new JsonObject();
        o1.put(new JsonValue(new IdbAttribute("att")), o);
        Assert.assertEquals(e0, o1);
    }
    @Test
    public void testJsonListCompacted() {
        JsonElement e0 = new JsonObject(true, new IdbAttribute("att"), new IdbIndex(0), "hi");

        JsonList o = new JsonList();
        o.add(new JsonValue("hi"));
        JsonElement o1 = new JsonObject();
        o1.put(new JsonValue(new IdbAttribute("att")), o);
        Assert.assertEquals(e0, o1);
        
    }
    
    @Test
    public void testParse() {
        JsonElement e0 = new JsonObject(true, new IdbAttribute("att"), "hi");
        e0.insert(true, new IdbClass("Ec"), new Long(5), new Double(5));
        JsonElement e1 = new JsonParser("{ \"_att\" : \"hi\", \"_Ec\" : { \"_5\" : \"_5.0\" }}").parse();
        Assert.assertEquals(e0, e1);
    }
    
    @Test
    public void testFlatten() throws Exception {
        JsonElement e0 = new JsonObject(true, new IdbAttribute("att"), "hi");
        e0.insert(true, new IdbClass("Ec"), new Long(5), new Double(5));
        List<List<Object>> items = e0.flattenToList();
        for (int i = 0; i < items.size(); i++)
            System.out.println("items[i] " + items.get(i));
        Assert.assertEquals(items.size(), 2);
        JsonElement e1 = JsonElement.unflattenFromList(true, items);
        Assert.assertEquals(e0, e1);
    }

    // Dozens of queries return. They have many different component types
    @Test
    public void testGetAllQueries() throws Exception {
        testGet("All queries direct", new IdbClass("Query"));
    }

    // A single string returns
    @Test
    public void testGetAStringFromTheDocumentation() throws Exception {
        testGet("Small direct",
                new IdbClass("Documentation"),"Basics",
                new IdbAttribute("description"), new IdbIndex(0));
    }

    public void testGet(String msg, Object... item) throws Exception {
        PrintTime t = new PrintTime();
        InfinityDBSimpleRestClient idb = getClient();
        Blob response = null;
        for (int i = 0; i < 3; i++) {
            response = idb.get(item);
            t.printTime(msg, response.length());
        }
        String contentType = response.getContentType();
        Assert.assertEquals("application/json", contentType);
        String s = response.toString();
        // Throws if bad
        JsonElement root = new JsonParser(s).parse();
    }

    @Test
    public void testPutBlob() throws Exception {
        PrintTime t = new PrintTime();
        InfinityDBSimpleRestClient idb = getClient();

        Blob request = new Blob("\"hello from Java\"", "application/json");
        idb.putBlob(request, new IdbClass("Trash"), new IdbClass("JavaDemo"),
                new IdbClass("DemoPost"), new Date());
        
        t.printTime("putBlob", 0);
    }

    @Test
    public void testExecute() throws Exception {
        testExecute("exec get image(not getblob)",
                new JsonObject(false, new IdbAttribute("name"), "pic0"),
                "examples", "Display Image", "application/json");
    }
    
    public void testExecute(String msg, JsonObject jsonObject, String ifc, String method,
            String expectedContentType) throws Exception {
        InfinityDBSimpleRestClient idb = getClient();

        PrintTime t = new PrintTime();
        Blob response = null;
        for (int i = 0; i < 3; i++) {
            Blob requestBlob = new Blob(jsonObject);
             response = idb.executeQuery(
                ifc, method, requestBlob, null);
            t.printTime(msg, response.length());
        }
        String contentType = response.getContentType();
        Assert.assertEquals(expectedContentType, contentType);
        checkJsonParsing(response);
    }
        
    @Test
    public void testExecuteGetBlobQueryImage() throws Exception {
        testExecuteGetBlobQuery("exec get blob image ",
                new Blob("{ \"_name\" : \"pic0\" }", "application/json"),
                "examples", "Display Image", "image/jpeg");
    }
    
    public void testExecuteGetBlobQuery(String msg, Blob requestBlob, String ifc, String method,
            String expectedContentType) throws Exception {
        InfinityDBSimpleRestClient idb = getClient();
        PrintTime t = new PrintTime();

        Blob response = null;
        for (int i = 0; i < 3; i++) {
             response = idb.executeGetBlobQuery(
                ifc, method, requestBlob, null);
            t.printTime(msg, response.length());
        }
        String contentType = response.getContentType();
        Assert.assertEquals(expectedContentType, contentType);
    }

    @Test
    public void testExecutePutBlobQuery() throws Exception {
        testExecutePutBlobQuery("exec put blob query ",
                "examples", "Put blob by name",
                new Blob("I am a plain text blob"),
                new Blob("{ \"_name\" : \"my blob\" }", "application/json"),
                "application/json");
    }

    public void testExecutePutBlobQuery(String msg, 
            String ifc, String method,
            Blob requestBlob, 
            Blob paramsBlob,
            String expectedContentType) throws Exception {
        InfinityDBSimpleRestClient idb = getClient();
        PrintTime t = new PrintTime();

        Blob response = null;
        for (int i = 0; i < 3; i++) {
             response = idb.executePutBlobQuery(
                ifc, method, requestBlob, paramsBlob);
            t.printTime(msg, response.length());
        }
        String contentType = response.getContentType();
        System.out.println("content type " + contentType);
        Assert.assertEquals(expectedContentType, contentType);
    }

    void checkJsonParsing(Blob response) {
        PrintTime t = new PrintTime();
        String s = response.toString();
        t.printTime("response.toString()", response.length());
        // System.out.println("Response: "+ s);

        JsonElement root = new JsonParser(s).parse();
        t.printTime("parse", response.length());

        String formatted = root.toString();
        //String formatted = root.toStringAsExtendedFormatIndented();
//        System.out.println("Formatted: " + formatted);
        t.printTime("root.toString()", formatted.length());

        // Do another round to see if it stays the same 
        JsonElement root2 = new JsonParser(formatted).parse();
//        String formatted2 = root2.toString();
//        System.out.println("Formatted2: " + formatted2);
        Assert.assertEquals(root, root2); 

        /*
         * You can print in the special 'extended JSON' format specific to
         * InfinityDB in which we avoid quoting keys as double-quoted strings. 
         * There is a display mode in the browser for it, and it is
         * quite relaxing. 
         */
        String formattedExtended = root.toStringAsExtendedFormat();
        t.printTime("toStringAsExtendedFormat()", formattedExtended.length());
//            System.out.println("Extended Format: " + formattedExtended);
        JsonElement rootExtended = new JsonParser(formattedExtended, false).parse();
        t.printTime("parse ExtendedFormat", formattedExtended.length());
        
        // Do another round to see if it is consistent.
        String formattedExtended2 = rootExtended.toStringAsExtendedFormat();
//            System.out.println("Extended Format re-formatted: " + formattedExtended2);
        t.printTime("toStringAsExtendedFormat() again", formattedExtended2.length());

         Assert.assertEquals(root, rootExtended);
    }

    static class PrintTime {
        long t = System.currentTimeMillis();
        
        void printTime(String msg, int len) {
            long t2 = System.currentTimeMillis();
            double time = (t2 - t) / 1e3;
            System.out.println(msg + " time=" + time
                    + " len=" + len
                    + " speed=" + len / time);
            t = t2;
        }
    }
}

