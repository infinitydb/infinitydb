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
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * The helper code to access InfinityDB Server. This is all
 * intended to make access easier, but it is possible to
 * do it directly too.
 * 
 * See boilerbay,com.
 */
public class InfinityDBSimpleRestClient {
    // Not URL quoted, unlike the prefix.
    final String host;
    String userName;
    String passWord;
    /*
     *  For HTTPS sometimes there is no way to use SSL host name verification,
     *  because the host has not registered a domain name and then
     *  paid for a cert and installed it. See the InfinityDB Server documentation
     *  at boilerbay.com for management, where this is explained.
     *  Installing a certificate requires host root access. See
     *  /data/infinitydb-home/key-store/certificate-tool.py.
     *  
     *  It is a bad idea to leave this true because then the security
     *  may end up permanently disabled when code gets deployed. It is
     *  also somewhat slow, because the keep-alive is interrupted.
     *  
     *  We disable both host name verification and certificate validation.
     */
    boolean isDisableSSLSecurity;

    public InfinityDBSimpleRestClient(String host) {
        if (host.endsWith("/"))
            host = host.substring(0, host.length() - 1);
        this.host = host;
    }

    public void setUserNameAndPassWord(String userName, String passWord) {
        this.userName = userName;
        this.passWord = passWord;
    }
    
    public void setDisableSSLSecurity(boolean isDisableSSLSecurity) {
        this.isDisableSSLSecurity = isDisableSSLSecurity;
    }

    /**
     * Returns a Blob if there is one on that prefix, otherwise the JSON on that
     * prefix. The existence of the Blob is determined by whether there is a
     * com.infinitydb.blob attribute and its inner parts on that prefix.
     * 
     * This is the default you will see in any browser when you visit that URL
     * without any action parameter.
     * 
     * Requires READ permission.If you do not have READ, you may still have
     * QUERY and be able to discover at least some of the database contents.
     * 
     * @throws ConnectionException if the response status was not 200 or 204.
     */
    public Blob get(Object... prefix) throws IOException {
        return command("GET", null, null, null, prefix);
    }

    /**
     * Returns a Blob if there is one on that prefix, or null if not. The
     * existence of the Blob is determined by whether there is a
     * com.infinitydb.blob attribute and its inner parts on that prefix.
     * 
     * Requires READ permission.If you do not have READ, you may still have
     * QUERY and be able to discover at least some of the database contents.
     * 
     * @throws ConnectionException if the response status was not 200 or 204.
     */
    public Blob getBlob(Object... prefix) throws IOException {
        return command("GET", "get-blob", null, null, prefix);
    }

    /**
     * Like get but insists on JSON, so a Blob becomes com.infinitydb.Blob etc.
     * That allows you to get things with suffixes on that prefix other than
     * just the Blob's attribute and suffixes.
     * 
     * Requires READ permission.
     * 
     * Actually, you can read any number of Blobs that may be nested within that
     * JSON all at once. (We are working on a way to get those nested Blobs
     * out. It is not necessarily faster to transfer a batch of blobs
     * this way until we get zlib compression.)
     * 
     * @throws ConnectionException if the response status was not 200 or 204.
     */
    public Blob getAsJson(Object... prefix) throws IOException {
        return command("GET", "as-json", null, null, prefix);
    }

    /**
     * Write a blob to a URL. The prefix is cleared and then the
     * blob is placed there. Be careful that the prefix is only
     * what you want to affect. If you want there to be suffixes
     * on that prefix other than the blob itself, you will have to
     * put them there later in some way.
     * 
     * Requires WRITE permission. If you do not have WRITE, you
     * may still have QUERY and be able to affect the database
     * contents.
     */
    public Blob putBlob(Blob blob, Object... prefix) throws IOException {
        return command("POST", "write", blob, null, prefix);
    }

    /**
     * Like putBlob(), but creates or adds to a list of blobs at
     * the prefix. It looks at the contents of the ItemSpace at the
     * prefix to find the largest Index() there, and stores the blob
     * at that index plus one. If there are already suffixes at that
     * prefix that are not indexes, they are left there. Note that
     * Indexes sort last of all the component types.
     *  
     * @param Blob
     * @param prefix
     * @return
     * @throws IOException
     * @throws ConnectionException if the response status was not 200 or 204.
     */
    public Blob appendBlob(Blob Blob, Object... prefix) throws IOException {
        return command("POST", "append", Blob, null, prefix);
    }

    /**
     * Invoke a query at the server based on the interface name and method name
     * of the PatternQuery. There is one query associated with each
     * interface/method. They are under the Query class.
     * 
     * For each database and group, this requires QUERY permission and specific
     * permissions on the particular interface. It is possible to restrict a
     * group to a set of interfaces, to a set of prefixes of the interface name,
     * to setters, to getters, or to either setters or getters. Queries can
     * declare themselves to be getters and/or setters with query Option setter
     * true and/or query Option getter true.
     * 
     * To send JSON in the params URL parameter do: ...,new JsonObject().put(new
     * IdbClass("MyClass"),new JsonValue(5)),..
     * 
     * @param interfaceName
     *            the name is stored inner to the Query class.
     * @param methodName
     *            in the database under the interface name.
     * @param requestContentBlob
     *            the query can get this in its request content symbol, if it
     *            wants. Nullable. If null, the request content symbol sees empty.
     * @param paramsUrlParameterJsonBlob
     *            some JSON sent in the URL query string parameter 'params'.
     *            Accessible to the query in a symbol of kind 'params url
     *            parameter'. Nullable.
     * @return a Blob containing data from the response content symbol. May be
     *         empty if the query does not choose to define and use a response
     *         content kind symbol. NO_CONTENT is not possible.
     * @throws IOException
     * @throws ConnectionException if the response status was not 200 OK or 204 NO_CONTENT.
     */
    public Blob executeQuery(String interfaceName, String methodName,
            Blob requestContentBlob,
            Blob paramsUrlParameterJsonBlob)
            throws IOException {
        return command("POST", "execute-query",
                requestContentBlob,
                paramsUrlParameterJsonBlob,
                interfaceName, methodName);
    }

    /**
     * Like executeQuery() but the response is a Blob. This is fast because it
     * is a raw transfer.
     * 
     * The query does not flag the data as being a bob in some explicit way, so
     * there are no specific 'get-blob' queries for example. However the query
     * will in fact write to its response content symbol a structure starting
     * with the magic com.infinitydb.blob attribute and its containing data and
     * content type as described in the REST access page at boilerbay.com. The
     * query will normally just get this effect by transferring a stored blob
     * directly into its response content symbol with no extra effort, and in
     * practice the com.infinitydb.blob structure is transparent. Inner to the
     * response content symbol will be a join symbol of type 'item'. This is all
     * very similar to executePutBlobQuery().
     * 
     * @param interfaceName
     *            like "com.mydomain.myinterface" Goes in the URL.
     * @param methodName
     *            a string like "My Query". Goes in the URL.
     * @param requestContentBlob
     *            the query can get this in its request content symbol, if it
     *            wants. Nullable. If null, the request content symbol sees
     *            empty.
     * @param paramsUrlParameterJsonBlob
     *            some JSON sent in the URL query string parameter 'params'.
     *            Accessible to the query in a symbol of kind 'params url
     *            parameter'. Nullable.
     * @return a Blob containing data from the response content symbol. May be
     *         empty if the query does not choose to define and use a response
     *         content kind symbol. NO_CONTENT is not possible.
     * @throws IOException
     * @throws ConnectionException
     *             if the response status was not 200 OK or 204 NO_CONTENT.
     */
    public Blob executeGetBlobQuery(String interfaceName, String methodName,
            Blob requestContentBlob,
            Blob paramsUrlParameterJsonBlob)
            throws IOException {
        return command("POST", "execute-get-blob-query", 
                requestContentBlob,
                paramsUrlParameterJsonBlob,
                interfaceName, methodName);
    }

    /**
     * Like executeQuery() but expects the request to be a Blob with raw bytes
     * and any content type, normally other than application/json. If you do
     * write application/json, it is stored as a blob. This is fast because it
     * is a raw transfer.
     * 
     * The query does not flag the data as being a blob in some explicit way, so
     * there are no specific 'put-blob' queries for example. However the query
     * will in fact read from its request content symbol a structure starting
     * with the magic com.infinitydb.blob attribute and its containing data and
     * content type as described in the REST access page at boilerbay.com. The
     * query will normally just get this effect by transferring to the ItemSpace
     * a blob directly from its request content symbol with no extra effort, and
     * in practice the com.infinitydb.blob structure is transparent. Inner to
     * the request content symbol will be a join symbol of type 'item' which
     * will match all suffixes.This is all very similar to
     * executeGetBlobQuery().
     * 
     * @param interfaceName
     *            like "com.mydomain.myinterface" Goes in the URL.
     * @param methodName
     *            a string like "My Query". Goes in the URL.
     * @param requestContentBlob
     *            the query can get this in its request content symbol, if it
     *            wants. Nullable. If null, the request content symbol sees
     *            empty.
     * @param paramsUrlParameterJsonBlob
     *            some JSON sent in the URL query string parameter 'params'.
     *            Accessible to the query in a symbol of kind 'params url
     *            parameter'. Nullable. This is particularly useful for put blob
     *            query because there is no other way to get input to the query.
     * @return a Blob containing JSON from the response content symbol. May be
     *         empty if the query does not choose to define and use a response
     *         content kind symbol. NO_CONTENT is not possible.
     * @throws IOException
     * @throws ConnectionException
     *             if the response status was not 200 OK or 204 NO_CONTENT.
     */
    public Blob executePutBlobQuery(String interfaceName, String methodName,
            Blob requestContentBlob,
            Blob paramsUrlParameterJsonBlob) throws IOException {
        return command("POST", "execute-put-blob-query", 
                requestContentBlob,
                paramsUrlParameterJsonBlob,
                interfaceName, methodName);
    }

    // These need work for the transaction parameters like wait-for-durable.

    //    public void commit() throws IOException {
//        command("POST", "commit", null,
//                new Blob("", "application/infinitydb"), null);
//    }
//
//    public void rollback() throws IOException {
//        command("POST", "rollback", null,
//                new Blob("", "application/infinitydb"), null);
//    }

    /**
     *  The prefix may be a nested Object[] of components to be URL-quoted.
     */
    private Blob command(String method, String action,
            Blob requestBlob,
            Blob paramsUrlParameterBlob,
            Object... prefix) throws IOException {
        if (method.equalsIgnoreCase("GET") && requestBlob != null)
            throw new RuntimeException(
                    "Method GET is not compatible with sending a Blob");
        String urlString = host + getQuotedUrl(prefix);
        UrlQueryString queryString = new UrlQueryString();
        queryString.add("action", action);
        if (paramsUrlParameterBlob != null) {
            if (!"application/json".equals(paramsUrlParameterBlob.getContentType()))
                    throw new IOException("Expected JSON for params url parameter");
            String paramsUrlParameter = paramsUrlParameterBlob.toString();
            // Check for valid.
             new JsonParser(paramsUrlParameter).parse();
            queryString.add("params", paramsUrlParameter);
        }
        URL url = new URL(urlString + queryString.queryString);
        HttpURLConnection urlConnection =
                (HttpURLConnection)url.openConnection();
        if (isDisableSSLSecurity && urlConnection instanceof HttpsURLConnection) {
            disableSSLSecurity((HttpsURLConnection)urlConnection);
        }
        urlConnection.setRequestMethod(method);
        // We always do input, but often it will be empty.
        urlConnection.setDoInput(true);
        if (userName != null) {
            String credentials = userName + ":" + passWord;
            String encodedCredentials =
                    Base64.getEncoder().encodeToString(credentials.getBytes());
            urlConnection.setRequestProperty("Authorization",
                    "Basic " + encodedCredentials);
        }
        if (method.equalsIgnoreCase("POST") && requestBlob != null) {
            urlConnection.setDoOutput(true);
            urlConnection.setRequestProperty("Content-Type", requestBlob.getContentType());
        }
        urlConnection.connect();
        
        // System.out.println("Connection: " +
        // urlConnection.getHeaderField("Connection"));
        
        if (method.equalsIgnoreCase("POST") && requestBlob != null) {
            OutputStream out = urlConnection.getOutputStream();
            out.write(requestBlob.data);
            out.flush();
        }
        try {
            // Throws 403 etc with IOException("Server returned response code 403 for
            // URL: ...")
            InputStream in = urlConnection.getInputStream();
            // When we get zlib working, the content length will become
            // -1 until the data is all read. Currently, then, there is
            // a limit to the blob size.
            long contentLength = urlConnection.getContentLengthLong();
            String contentType = urlConnection.getContentType();
            int status = urlConnection.getResponseCode();
            String msg = urlConnection.getResponseMessage();
            Blob readBlob = new Blob((int)contentLength, contentType);
            readBlob.readFully(in);
            in.close();
            // does not prevent keep-alive.
            urlConnection.disconnect();
            if (status == HttpURLConnection.HTTP_OK 
                    || status == HttpURLConnection.HTTP_NO_CONTENT)
                // SUCCESS. Blob may still be empty if that's what we want.
                return readBlob;
            throw new ConnectionException(status, msg);
        } catch (IOException e) {
            int status = urlConnection.getResponseCode();
            String msg = urlConnection.getResponseMessage();
            throw new ConnectionException(status, msg);
        }
    }
    
    static class UrlQueryString {
        String queryString = "";
        void add(String param, String value) throws UnsupportedEncodingException {
            if (param == null || value == null)
                return;
            queryString += queryString.length() == 0 ? "?" : "&";
            value = URLEncoder.encode(value, "UTF-8");
            queryString += param + "=" + value;
        }
    }

    /**
     * This can quote any URL path, even if it has components like new
     * IdbClass("Documentation"),"Basics",new IdbAttribute("description"), new
     * IdbIndex(0) which becomes
     * /demo/readonly/Documentation/%22Basics%22/description/%5B0%5D. It does
     * the components separately, then concatenates with / properly. Note there
     * is no underscore quoting as there is in JSON.
     * 
     * @param components
     *            a varargs series Object[], flattened. Typically these will be
     *            IdbClass, IdbAttribute, Index, numbers, booleans etc. for the
     *            12 types. These are the standard 'token' forms.
     * @throws UnsupportedEncodingException
     * @throws MalformedURLException
     */
    public static String getQuotedUrl(Object... components)
            throws UnsupportedEncodingException, MalformedURLException {
        StringBuffer sb = new StringBuffer();
        for (Object o : Flatten.flatten(components)) {
            if (o instanceof String)
                o = JsonParser.convertToJsonString((String)o);
            else if (o instanceof Date)
                o = JsonParser.ISO_SIMPLE_DATE_FORMAT.format((Date)o);
            String quoted = URLEncoder.encode(o.toString(), "UTF-8");
            sb.append("/").append(quoted);
        }
        return sb.toString();
    }
    
    /**
     * Don't use this in production!!!!
     */
    void disableSSLSecurity(HttpsURLConnection httpsURLConnection) throws IOException {
        try {
            httpsURLConnection.setHostnameVerifier(
                    new HostnameVerifier() {
                        @Override
                        public boolean verify(String hostname,
                                SSLSession session) {
                            // Dangerous. Don't do this unless absolutely
                            // necessary!
                            return true;
                        }
                    });
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[] {new TrustAnythingTrustManager()}, 
                    new SecureRandom());
            httpsURLConnection.setSSLSocketFactory(sslContext.getSocketFactory());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}

class Flatten {
    static Object[] flatten(Object... o) {
        return flatten(new ArrayList<>(), o).toArray();
    }
    static List<Object> flatten(List<Object> list, Object o) {
        if (o instanceof Object[]) {
            for (Object e : (Object[])o) {
                flatten(list, e);
            }
        } else {
            list.add(o);
        }
        return list;
    }
}

/**
 * Don't use this in production!!!!
 */
class IgnoreHostNameVerifier implements HostnameVerifier {
    @Override
    public boolean verify(String hostname, SSLSession session) {
        return true;
    }
}

class TrustAnythingTrustManager implements X509TrustManager {

    @Override
    public void checkClientTrusted(X509Certificate[] arg0, String arg1)
            throws CertificateException {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] arg0, String arg1)
            throws CertificateException {
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }
}
