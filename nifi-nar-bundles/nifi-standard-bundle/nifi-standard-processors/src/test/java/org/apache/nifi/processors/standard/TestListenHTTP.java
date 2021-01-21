/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.servlet.http.HttpServletResponse;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardRestrictedSSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.nifi.processors.standard.ListenHTTP.RELATIONSHIP_SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class TestListenHTTP {

    private static final String SSL_CONTEXT_SERVICE_IDENTIFIER = "ssl-context";

    private static final String HTTP_POST_METHOD = "POST";
    private static final String HTTP_BASE_PATH = "basePath";

    private final static String PORT_VARIABLE = "HTTP_PORT";
    private final static String HTTP_SERVER_PORT_EL = "${" + PORT_VARIABLE + "}";

    private final static String BASEPATH_VARIABLE = "HTTP_BASEPATH";
    private final static String HTTP_SERVER_BASEPATH_EL = "${" + BASEPATH_VARIABLE + "}";

    private static final String TLS_1_3 = "TLSv1.3";
    private static final String TLS_1_2 = "TLSv1.2";
    private static final String LOCALHOST = "localhost";

    private static TlsConfiguration clientTlsConfiguration;
    private static TlsConfiguration trustOnlyTlsConfiguration;

    private ListenHTTP proc;
    private TestRunner runner;

    private int availablePort;

    @BeforeClass
    public static void setUpSuite() throws IOException, GeneralSecurityException {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);

        clientTlsConfiguration = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore();

        trustOnlyTlsConfiguration = new StandardTlsConfiguration(
                null, null, null, null,
                clientTlsConfiguration.getTruststorePath(), clientTlsConfiguration.getTruststorePassword(),
                clientTlsConfiguration.getTruststoreType(), TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (clientTlsConfiguration != null) {
            try {
                if (StringUtils.isNotBlank(clientTlsConfiguration.getKeystorePath())) {
                    java.nio.file.Files.deleteIfExists(Paths.get(clientTlsConfiguration.getKeystorePath()));
                }
            } catch (IOException e) {
                throw new IOException("There was an error deleting a keystore: " + e.getMessage());
            }

            try {
                if (StringUtils.isNotBlank(clientTlsConfiguration.getTruststorePath())) {
                    java.nio.file.Files.deleteIfExists(Paths.get(clientTlsConfiguration.getTruststorePath()));
                }
            } catch (IOException e) {
                throw new IOException("There was an error deleting a truststore: " + e.getMessage());
            }
        }
    }

    @Before
    public void setup() throws IOException, GeneralSecurityException {
        proc = new ListenHTTP();
        runner = TestRunners.newTestRunner(proc);
        availablePort = NetworkUtils.availablePort();
        runner.setVariable(PORT_VARIABLE, Integer.toString(availablePort));
        runner.setVariable(BASEPATH_VARIABLE, HTTP_BASE_PATH);
    }

    @After
    public void teardown() {
        proc.shutdownHttpServer();
        new File("my-file-text.txt").delete();
    }

    @Test
    public void testPOSTRequestsReceivedWithoutEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, false, false);
    }

    @Test
    public void testPOSTRequestsReceivedReturnCodeWithoutEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, false, false);
    }

    @Test
    public void testPOSTRequestsReceivedWithEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, false, false);
    }

    @Test
    public void testPOSTRequestsReturnCodeReceivedWithEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, false, false);
    }

    @Test
    public void testSecurePOSTRequestsReceivedWithoutEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(false);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion());
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, false);
    }

    @Test
    public void testSecurePOSTRequestsReturnCodeReceivedWithoutEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(false);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion());
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, false);
    }

    @Test
    public void testSecurePOSTRequestsReceivedWithEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(false);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion());
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, false);
    }

    @Test
    public void testSecurePOSTRequestsReturnCodeReceivedWithEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(false);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion());
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, false);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithoutEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(true);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion());
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReturnCodeReceivedWithoutEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(true);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion());
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(true);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion());
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReturnCodeReceivedWithEL() throws Exception {
        SSLContextService sslContextService = configureProcessorSslContextService(true);
        runner.setProperty(sslContextService, StandardRestrictedSSLContextService.RESTRICTED_SSL_ALGORITHM, TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion());
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, true);
    }

    @Test
    public void testSecureInvalidSSLConfiguration() throws Exception {
        SSLContextService sslContextService = configureInvalidProcessorSslContextService();
        runner.setProperty(sslContextService, StandardSSLContextService.SSL_ALGORITHM, TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion());
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertNotValid();
    }

    @Test
    public void testSecureServerSupportsCurrentTlsProtocolVersion() throws Exception {
        startSecureServer(false);

        final SSLSocketFactory sslSocketFactory = SslContextFactory.createSSLSocketFactory(trustOnlyTlsConfiguration);
        final SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket(LOCALHOST, availablePort);
        final String currentProtocol = TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion();
        sslSocket.setEnabledProtocols(new String[]{currentProtocol});

        sslSocket.startHandshake();
        final SSLSession sslSession = sslSocket.getSession();
        assertEquals("SSL Session Protocol not matched", currentProtocol, sslSession.getProtocol());
    }

    @Test
    public void testSecureServerTrustStoreConfiguredClientAuthenticationRequired() throws Exception {
        startSecureServer(true);
        final HttpsURLConnection connection = getSecureConnection(trustOnlyTlsConfiguration);
        assertThrows(SSLException.class, connection::getResponseCode);

        final HttpsURLConnection clientCertificateConnection = getSecureConnection(clientTlsConfiguration);
        final int responseCode = clientCertificateConnection.getResponseCode();
        assertEquals(HttpServletResponse.SC_METHOD_NOT_ALLOWED, responseCode);
    }

    @Test
    public void testSecureServerTrustStoreNotConfiguredClientAuthenticationNotRequired() throws Exception {
        startSecureServer(false);
        final HttpsURLConnection connection = getSecureConnection(trustOnlyTlsConfiguration);
        final int responseCode = connection.getResponseCode();
        assertEquals(HttpServletResponse.SC_METHOD_NOT_ALLOWED, responseCode);
    }

    @Test
    public void testSecureServerRejectsUnsupportedTlsProtocolVersion() throws Exception {
        final String currentProtocol = TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion();
        final String protocolMessage = String.format("TLS Protocol required [%s] found [%s]", TLS_1_3, currentProtocol);
        Assume.assumeTrue(protocolMessage, TLS_1_3.equals(currentProtocol));

        final SSLContextService sslContextService = configureProcessorSslContextService(false);
        runner.setProperty(sslContextService, StandardSSLContextService.SSL_ALGORITHM, TLS_1_3);
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        startWebServer();
        final SSLSocketFactory sslSocketFactory = SslContextFactory.createSSLSocketFactory(trustOnlyTlsConfiguration);
        final SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket(LOCALHOST, availablePort);
        sslSocket.setEnabledProtocols(new String[]{TLS_1_2});

        assertThrows(SSLHandshakeException.class, sslSocket::startHandshake);
    }

    private void startSecureServer(final boolean setServerTrustStoreProperties) throws InitializationException {
        final SSLContextService sslContextService = configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO, setServerTrustStoreProperties);
        runner.setProperty(sslContextService, StandardSSLContextService.SSL_ALGORITHM, TlsConfiguration.TLS_PROTOCOL);
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();
        startWebServer();
    }

    private HttpsURLConnection getSecureConnection(final TlsConfiguration tlsConfiguration) throws Exception {
        final URL url = new URL(buildUrl(true));
        final HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
        final SSLSocketFactory sslSocketFactory = SslContextFactory.createSSLSocketFactory(tlsConfiguration);
        connection.setSSLSocketFactory(sslSocketFactory);
        return connection;
    }

    private int executePOST(String message, boolean secure, boolean twoWaySsl) throws Exception {
        String endpointUrl = buildUrl(secure);
        final URL url = new URL(endpointUrl);
        HttpURLConnection connection;

        if (secure) {
            connection = buildSecureConnection(twoWaySsl, url);
        } else {
            connection = (HttpURLConnection) url.openConnection();
        }
        connection.setRequestMethod(HTTP_POST_METHOD);
        connection.setDoOutput(true);

        final DataOutputStream wr = new DataOutputStream(connection.getOutputStream());

        if (message != null) {
            wr.writeBytes(message);
        }
        wr.flush();
        wr.close();
        return connection.getResponseCode();
    }

    private static HttpsURLConnection buildSecureConnection(boolean twoWaySsl, URL url) throws IOException, TlsException {
        final HttpsURLConnection sslCon = (HttpsURLConnection) url.openConnection();
        SSLContext clientSslContext;
        if (twoWaySsl) {
            // Use a client certificate, do not reuse the server's keystore
            clientSslContext = SslContextFactory.createSslContext(clientTlsConfiguration);
        } else {
            // With one-way SSL, the client still needs a truststore
            clientSslContext = SslContextFactory.createSslContext(trustOnlyTlsConfiguration);
        }
        sslCon.setSSLSocketFactory(clientSslContext.getSocketFactory());
        return sslCon;
    }

    private String buildUrl(final boolean secure) {
        return String.format("%s://localhost:%s/%s", secure ? "https" : "http", availablePort, HTTP_BASE_PATH);
    }

    private void testPOSTRequestsReceived(int returnCode, boolean secure, boolean twoWaySsl) throws Exception {
        final List<String> messages = new ArrayList<>();
        messages.add("payload 1");
        messages.add("");
        messages.add(null);
        messages.add("payload 2");

        startWebServerAndSendMessages(messages, returnCode, secure, twoWaySsl);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(RELATIONSHIP_SUCCESS);

        runner.assertTransferCount(RELATIONSHIP_SUCCESS, 4);
        mockFlowFiles.get(0).assertContentEquals("payload 1");
        mockFlowFiles.get(1).assertContentEquals("");
        mockFlowFiles.get(2).assertContentEquals("");
        mockFlowFiles.get(3).assertContentEquals("payload 2");
    }

    private void startWebServer() {
        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        final ProcessContext context = runner.getProcessContext();
        proc.onTrigger(context, processSessionFactory);
    }

    private void startWebServerAndSendRequests(Runnable sendRequestToWebserver, int numberOfExpectedFlowFiles) throws Exception {
        startWebServer();
        new Thread(sendRequestToWebserver).start();
        long responseTimeout = 10000;

        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        final ProcessContext context = runner.getProcessContext();
        int numTransferred = 0;
        long startTime = System.currentTimeMillis();
        while (numTransferred < numberOfExpectedFlowFiles && (System.currentTimeMillis() - startTime < responseTimeout)) {
            proc.onTrigger(context, processSessionFactory);
            numTransferred = runner.getFlowFilesForRelationship(RELATIONSHIP_SUCCESS).size();
            Thread.sleep(100);
        }

        runner.assertTransferCount(ListenHTTP.RELATIONSHIP_SUCCESS, numberOfExpectedFlowFiles);
    }

    private void startWebServerAndSendMessages(final List<String> messages, int returnCode, boolean secure, boolean twoWaySsl)
            throws Exception {

        Runnable sendMessagesToWebServer = () -> {
            try {
                for (final String message : messages) {
                    if (executePOST(message, secure, twoWaySsl) != returnCode) {
                        fail("HTTP POST failed.");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                fail("Not expecting error here.");
            }
        };

        startWebServerAndSendRequests(sendMessagesToWebServer, messages.size());
    }

    private SSLContextService configureProcessorSslContextService(boolean setTrustStoreProperties) throws InitializationException {
        ListenHTTP.ClientAuthentication clientAuthentication = ListenHTTP.ClientAuthentication.AUTO;
        if (setTrustStoreProperties) {
            clientAuthentication = ListenHTTP.ClientAuthentication.REQUIRED;
        }
        return configureProcessorSslContextService(clientAuthentication, setTrustStoreProperties);
    }

    private SSLContextService configureProcessorSslContextService(final ListenHTTP.ClientAuthentication clientAuthentication,
                                                                  final boolean setTrustStoreProperties) throws InitializationException {
        final SSLContextService sslContextService = new StandardRestrictedSSLContextService();
        runner.addControllerService(SSL_CONTEXT_SERVICE_IDENTIFIER, sslContextService);

        if (setTrustStoreProperties) {
            runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, clientTlsConfiguration.getTruststorePath());
            runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, clientTlsConfiguration.getTruststorePassword());
            runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, clientTlsConfiguration.getTruststoreType().toString());
        }
        runner.setProperty(ListenHTTP.CLIENT_AUTHENTICATION, clientAuthentication.name());
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, clientTlsConfiguration.getKeystorePath());
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, clientTlsConfiguration.getKeystorePassword());
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, clientTlsConfiguration.getKeystoreType().toString());

        runner.setProperty(ListenHTTP.SSL_CONTEXT_SERVICE, SSL_CONTEXT_SERVICE_IDENTIFIER);

        return sslContextService;
    }

    private SSLContextService configureInvalidProcessorSslContextService() throws InitializationException {
        final SSLContextService sslContextService = new StandardSSLContextService();
        runner.addControllerService(SSL_CONTEXT_SERVICE_IDENTIFIER, sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, clientTlsConfiguration.getTruststorePath());
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, clientTlsConfiguration.getTruststorePassword());
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, clientTlsConfiguration.getTruststoreType().toString());
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, clientTlsConfiguration.getKeystorePath());
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, clientTlsConfiguration.getKeystorePassword());
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, clientTlsConfiguration.getKeystoreType().toString());

        runner.setProperty(ListenHTTP.SSL_CONTEXT_SERVICE, SSL_CONTEXT_SERVICE_IDENTIFIER);
        return sslContextService;
    }


    @Test(/*timeout=10000*/)
    public void testMultipartFormDataRequest() throws Exception {

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_OK));

        final SSLContextService sslContextService = runner.getControllerService(SSL_CONTEXT_SERVICE_IDENTIFIER, SSLContextService.class);
        final boolean isSecure = (sslContextService != null);

        Runnable sendRequestToWebserver = () -> {
            try {
                MultipartBody multipartBody = new MultipartBody.Builder().setType(MultipartBody.FORM)
                        .addFormDataPart("p1", "v1")
                        .addFormDataPart("p2", "v2")
                        .addFormDataPart("file1", "my-file-text.txt", RequestBody.create(MediaType.parse("text/plain"), createTextFile("my-file-text.txt", "Hello", "World")))
                        .addFormDataPart("file2", "my-file-data.json", RequestBody.create(MediaType.parse("application/json"), createTextFile("my-file-text.txt", "{ \"name\":\"John\", \"age\":30 }")))
                        .addFormDataPart("file3", "my-file-binary.bin", RequestBody.create(MediaType.parse("application/octet-stream"), generateRandomBinaryData(100)))
                        .build();

                Request request =
                        new Request.Builder()
                                .url(buildUrl(isSecure))
                                .post(multipartBody)
                                .build();

                int timeout = 3000;
                OkHttpClient client = new OkHttpClient.Builder()
                        .readTimeout(timeout, TimeUnit.MILLISECONDS)
                        .writeTimeout(timeout, TimeUnit.MILLISECONDS)
                        .build();

                try (Response response = client.newCall(request).execute()) {
                    Assert.assertTrue(String.format("Unexpected code: %s, body: %s", response.code(), response.body().string()), response.isSuccessful());
                }
            } catch (final Throwable t) {
                t.printStackTrace();
                Assert.fail(t.toString());
            }
        };


        startWebServerAndSendRequests(sendRequestToWebserver, 5);

        runner.assertAllFlowFilesTransferred(ListenHTTP.RELATIONSHIP_SUCCESS, 5);
        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(ListenHTTP.RELATIONSHIP_SUCCESS);
        // Part fragments are not processed in the order we submitted them.
        // We cannot rely on the order we sent them in.
        MockFlowFile mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "p1");
        mff.assertAttributeEquals("http.multipart.name", "p1");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeEquals("http.multipart.fragments.sequence.number", "1");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");

        mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "p2");
        mff.assertAttributeEquals("http.multipart.name", "p2");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");

        mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "file1");
        mff.assertAttributeEquals("http.multipart.name", "file1");
        mff.assertAttributeEquals("http.multipart.filename", "my-file-text.txt");
        mff.assertAttributeEquals("http.headers.multipart.content-type", "text/plain");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");

        mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "file2");
        mff.assertAttributeEquals("http.multipart.name", "file2");
        mff.assertAttributeEquals("http.multipart.filename", "my-file-data.json");
        mff.assertAttributeEquals("http.headers.multipart.content-type", "application/json");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");

        mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "file3");
        mff.assertAttributeEquals("http.multipart.name", "file3");
        mff.assertAttributeEquals("http.multipart.filename", "my-file-binary.bin");
        mff.assertAttributeEquals("http.headers.multipart.content-type", "application/octet-stream");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");
    }

    private byte[] generateRandomBinaryData(int i) {
        byte[] bytes = new byte[100];
        new Random().nextBytes(bytes);
        return bytes;
    }

    private File createTextFile(String fileName, String... lines) throws IOException {
        File file = new File("target/" + fileName);
        file.deleteOnExit();
        for (String string : lines) {
            Files.append(string, file, Charsets.UTF_8);
        }
        return file;
    }

    protected MockFlowFile findFlowFile(List<MockFlowFile> flowFilesForRelationship, String attributeName, String attributeValue) {
        Optional<MockFlowFile> optional = Iterables.tryFind(flowFilesForRelationship, ff -> ff.getAttribute(attributeName).equals(attributeValue));
        Assert.assertTrue(optional.isPresent());
        return optional.get();
    }
}
