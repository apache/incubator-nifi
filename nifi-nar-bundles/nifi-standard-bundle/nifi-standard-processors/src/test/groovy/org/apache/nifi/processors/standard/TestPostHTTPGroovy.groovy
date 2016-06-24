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
package org.apache.nifi.processors.standard

import groovy.servlet.GroovyServlet
import org.apache.nifi.ssl.SSLContextService
import org.apache.nifi.ssl.StandardSSLContextService
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.ExtendedKeyUsage
import org.bouncycastle.asn1.x509.KeyPurposeId
import org.bouncycastle.asn1.x509.KeyUsage
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo
import org.bouncycastle.asn1.x509.X509Extension
import org.bouncycastle.cert.X509CertificateHolder
import org.bouncycastle.cert.X509v3CertificateBuilder
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.operator.ContentSigner
import org.bouncycastle.operator.OperatorCreationException
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.eclipse.jetty.server.HttpConfiguration
import org.eclipse.jetty.server.HttpConnectionFactory
import org.eclipse.jetty.server.SecureRequestCustomizer
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.server.SslConnectionFactory
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import javax.net.SocketFactory
import javax.net.ssl.HostnameVerifier
import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager
import java.security.InvalidKeyException
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.KeyStore
import java.security.NoSuchAlgorithmException
import java.security.NoSuchProviderException
import java.security.PrivateKey
import java.security.PublicKey
import java.security.Security
import java.security.SignatureException
import java.security.cert.Certificate
import java.security.cert.CertificateException
import java.security.cert.X509Certificate

@RunWith(JUnit4.class)
class TestPostHTTPGroovy extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(TestPostHTTPGroovy.class)

    private static final int KEY_SIZE = 2048;

    private static final long YESTERDAY = System.currentTimeMillis() - 24 * 60 * 60 * 1000;
    private static final long ONE_YEAR_FROM_NOW = System.currentTimeMillis() + 365 * 24 * 60 * 60 * 1000;
    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";
    private static final String PROVIDER = "BC";

    private static final String SUBJECT_DN = "CN=localhost,OU=Security,O=Apache,ST=CA,C=US";

    private static final String DEFAULT_KEYSTORE_PASSWORD = "thisIsABadKeystorePassword"
    private static final String DEFAULT_KEY_PASSWORD = "thisIsABadKeyPassword"
    private static final String DEFAULT_KEY_ALIAS = "jetty-host-private"
    private static final String DEFAULT_CERTIFICATE_ALIAS = "jetty-host"
    static private final String KEYSTORE_TYPE = "JKS"

    private static final String TLSv1 = "TLSv1"
    private static final String TLSv1_1 = "TLSv1.1"
    private static final String TLSv1_2 = "TLSv1.2"
    private static final List DEFAULT_PROTOCOLS = [TLSv1, TLSv1_1, TLSv1_2]

    private static final String DEFAULT_HOSTNAME = "localhost"
    private static final int DEFAULT_TLS_PORT = 8456
    private static final String HTTPS_URL = "https://${DEFAULT_HOSTNAME}:${DEFAULT_TLS_PORT}"

    private static KeyStore keystore
    private static KeyStore truststore
    private static String keystorePath
    private static String truststorePath

    private static String   keystorePassword = DEFAULT_KEYSTORE_PASSWORD
    private static String truststorePassword = DEFAULT_KEYSTORE_PASSWORD

    private static Server server

    private static TestRunner runner

    /**
     * Generates a public/private RSA keypair using the default key size.
     *
     * @param keySize the key size in bits to use (defaults to 2048)
     * @return the keypair
     * @throws java.security.NoSuchAlgorithmException if the RSA algorithm is not available
     */
    private static KeyPair generateKeyPair(int keySize = KEY_SIZE) throws NoSuchAlgorithmException {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(keySize);
        return keyPairGenerator.generateKeyPair();
    }

    /**
     * Generates a signed certificate using an on-demand keypair.
     *
     * @param dn the DN
     * @return the certificate
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws java.security.cert.CertificateException
     * @throws java.security.NoSuchProviderException
     * @throws java.security.SignatureException
     * @throws java.security.InvalidKeyException
     * @throws org.bouncycastle.operator.OperatorCreationException
     */
    private
    static X509Certificate generateCertificate(String dn) throws IOException, NoSuchAlgorithmException, CertificateException, NoSuchProviderException, SignatureException, InvalidKeyException, OperatorCreationException {
        KeyPair keyPair = generateKeyPair();
        return generateCertificate(dn, keyPair);
    }

    /**
     * Generates a signed certificate with a specific keypair.
     *
     * @param dn the DN
     * @param keyPair the public key will be included in the certificate and the the private key is used to sign the certificate
     * @return the certificate
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws CertificateException
     * @throws NoSuchProviderException
     * @throws SignatureException
     * @throws InvalidKeyException
     * @throws OperatorCreationException
     */
    private
    static X509Certificate generateCertificate(String dn, KeyPair keyPair) throws IOException, NoSuchAlgorithmException, CertificateException, NoSuchProviderException, SignatureException, InvalidKeyException, OperatorCreationException {
        PrivateKey privateKey = keyPair.getPrivate();
        ContentSigner sigGen = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).setProvider(PROVIDER).build(privateKey);
        SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());
        Date startDate = new Date(YESTERDAY);
        Date endDate = new Date(ONE_YEAR_FROM_NOW);

        X509v3CertificateBuilder certBuilder = new X509v3CertificateBuilder(
                new X500Name(dn),
                BigInteger.valueOf(System.currentTimeMillis()),
                startDate, endDate,
                new X500Name(dn),
                subPubKeyInfo);

        // Set certificate extensions
        // (1) digitalSignature extension
        certBuilder.addExtension(X509Extension.keyUsage, true,
                new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment | KeyUsage.dataEncipherment | KeyUsage.keyAgreement));

        // (2) extendedKeyUsage extension
        Vector<KeyPurposeId> ekUsages = new Vector<>();
        ekUsages.add(KeyPurposeId.id_kp_clientAuth);
        ekUsages.add(KeyPurposeId.id_kp_serverAuth);
        certBuilder.addExtension(X509Extension.extendedKeyUsage, false, new ExtendedKeyUsage(ekUsages));

        // Sign the certificate
        X509CertificateHolder certificateHolder = certBuilder.build(sigGen);
        return new JcaX509CertificateConverter().setProvider(PROVIDER)
                .getCertificate(certificateHolder);
    }

    /**
     * Generates a certificate signed by the issuer key.
     *
     * @param dn the subject DN
     * @param issuerDn the issuer DN
     * @param issuerKey the issuer private key
     * @return the certificate
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws CertificateException
     * @throws NoSuchProviderException
     * @throws SignatureException
     * @throws InvalidKeyException
     * @throws OperatorCreationException
     */
    private
    static X509Certificate generateIssuedCertificate(String dn, String issuerDn, PrivateKey issuerKey) throws IOException, NoSuchAlgorithmException, CertificateException, NoSuchProviderException, SignatureException, InvalidKeyException, OperatorCreationException {
        KeyPair keyPair = generateKeyPair();
        return generateIssuedCertificate(dn, keyPair.getPublic(), issuerDn, issuerKey);
    }

    /**
     * Generates a certificate with a specific public key signed by the issuer key.
     *
     * @param dn the subject DN
     * @param publicKey the subject public key
     * @param issuerDn the issuer DN
     * @param issuerKey the issuer private key
     * @return the certificate
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws CertificateException
     * @throws NoSuchProviderException
     * @throws SignatureException
     * @throws InvalidKeyException
     * @throws OperatorCreationException
     */
    private
    static X509Certificate generateIssuedCertificate(String dn, PublicKey publicKey, String issuerDn, PrivateKey issuerKey) throws IOException, NoSuchAlgorithmException, CertificateException, NoSuchProviderException, SignatureException, InvalidKeyException, OperatorCreationException {
        ContentSigner sigGen = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).setProvider(PROVIDER).build(issuerKey);
        SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(publicKey.getEncoded());
        Date startDate = new Date(YESTERDAY);
        Date endDate = new Date(ONE_YEAR_FROM_NOW);

        X509v3CertificateBuilder v3CertGen = new X509v3CertificateBuilder(
                new X500Name(issuerDn),
                BigInteger.valueOf(System.currentTimeMillis()),
                startDate, endDate,
                new X500Name(dn),
                subPubKeyInfo);

        X509CertificateHolder certificateHolder = v3CertGen.build(sigGen);
        return new JcaX509CertificateConverter().setProvider(PROVIDER)
                .getCertificate(certificateHolder);
    }

    public
    static KeyStore prepareKeyStoreAndTrustStore(KeyPair keyPair = generateKeyPair(), String dn = SUBJECT_DN, String keystorePassword = DEFAULT_KEYSTORE_PASSWORD) {
        X509Certificate certificate = generateCertificate(dn, keyPair)
        keystore = KeyStore.getInstance(KEYSTORE_TYPE)
        keystore.load(null, null)
        keystore.setKeyEntry(DEFAULT_KEY_ALIAS, keyPair.private, DEFAULT_KEY_PASSWORD.chars, [certificate] as Certificate[])

        keystorePath = "src/test/resources/TestPostHTTP/${System.currentTimeMillis()}.jks"
        keystore.store(new FileOutputStream(keystorePath), keystorePassword.chars)
        
        // Also prepare a truststore for the client
        truststore = KeyStore.getInstance(KEYSTORE_TYPE)
        truststore.load(null, null)
        truststore.setCertificateEntry(DEFAULT_CERTIFICATE_ALIAS, certificate as Certificate)

        truststorePath = "src/test/resources/TestPostHTTP/${System.currentTimeMillis()}-trust.jks"
        truststore.store(new FileOutputStream(truststorePath), DEFAULT_KEYSTORE_PASSWORD.chars)
        
        keystore
    }

    @BeforeClass
    public static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        keystore = prepareKeyStoreAndTrustStore()
        server = createServer()

        // Set the default trust manager for the "default" tests (the outgoing Groovy call) to ignore certificate path verification for localhost

        def nullTrustManager = [
                checkClientTrusted: { chain, authType ->  },
                checkServerTrusted: { chain, authType ->  },
                getAcceptedIssuers: { null }
        ]

        def nullHostnameVerifier = [
                verify: { String hostname, session ->
                    // Will always return true if the hostname is "localhost"
                    hostname.equalsIgnoreCase(DEFAULT_HOSTNAME)
                }
        ]

        SSLContext sc = SSLContext.getInstance(TLSv1_2)
        sc.init(null, [nullTrustManager as X509TrustManager] as TrustManager[], null)
        SocketFactory socketFactory = sc.getSocketFactory()
        logger.info("JCE unlimited strength installed: ${Cipher.getMaxAllowedKeyLength("AES") > 128}")
        logger.info("Supported client cipher suites: ${socketFactory.supportedCipherSuites}")
        HttpsURLConnection.setDefaultSSLSocketFactory(socketFactory)
        HttpsURLConnection.setDefaultHostnameVerifier(nullHostnameVerifier as HostnameVerifier)

        runner = TestRunners.newTestRunner(PostHTTP.class)
    }

    private static Server createServer(List supportedProtocols = DEFAULT_PROTOCOLS) {
        // Create Server
        server = new Server()

        // Add some secure config
        final HttpConfiguration httpsConfiguration = new HttpConfiguration()
        httpsConfiguration.setSecureScheme("https")
        httpsConfiguration.setSecurePort(DEFAULT_TLS_PORT)
        httpsConfiguration.addCustomizer(new SecureRequestCustomizer())

        // Build the TLS connector
        final ServerConnector https = createConnector(httpsConfiguration, supportedProtocols)

        // Add this connector
        server.addConnector(https)

        /** Create a simple Groovlet that responds to the incoming request by reversing the string parameter
         * i.e. localhost:8456/ReverseHandler.groovy?string=Happy%20birthday -> yadhtrib yppaH
         */
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
        context.with {
            contextPath = '/'
            resourceBase = 'src/test/resources/TestPostHTTP'
            addServlet(GroovyServlet, '*.groovy')
        }
        server.setHandler(context)
        server
    }

    private
    static ServerConnector createConnector(HttpConfiguration httpsConfiguration, List supportedProtocols = DEFAULT_PROTOCOLS) {
        ServerConnector https = new ServerConnector(server,
                new SslConnectionFactory(createSslContextFactory(supportedProtocols), "http/1.1"),
                new HttpConnectionFactory(httpsConfiguration))

        // set host and port
        https.setHost(DEFAULT_HOSTNAME)
        https.setPort(DEFAULT_TLS_PORT)
        https
    }

    private static SslContextFactory createSslContextFactory(List supportedProtocols = DEFAULT_PROTOCOLS) {
        final SslContextFactory contextFactory = new SslContextFactory()
        contextFactory.needClientAuth = false
        contextFactory.wantClientAuth = false

        contextFactory.setKeyStorePath(keystorePath)
        contextFactory.setKeyStoreType(KEYSTORE_TYPE)
        contextFactory.setKeyStorePassword(keystorePassword)
//        contextFactory.setKeyManagerPassword(DEFAULT_KEY_PASSWORD)

        contextFactory.setIncludeProtocols(supportedProtocols as String[])
        contextFactory
    }

    @AfterClass
    public static void tearDownOnce() {
//        new File(keystorePath).delete()
//        new File(truststorePath).delete()
    }

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {
        try {
            server.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }

        runner.clearTransferState()
        runner.clearProvenanceEvents()
    }

    @Ignore
    @Test
    public void testDefaultShouldSupportTLSv1() {
        // Arrange
        final String MSG = "This is a test message"
        final String url = "${HTTPS_URL}/ReverseHandler.groovy?string=${URLEncoder.encode(MSG, "UTF-8")}"

        // Configure server with TLSv1 only
        server = createServer([TLSv1])

        // Start server
        server.start()

        // Act
        String response = new URL(url).text
        logger.info("Response from ${HTTPS_URL}: ${response}")

        // Assert
        assert response == MSG.reverse()
    }

    @Ignore
    @Test
    public void testDefaultShouldSupportTLSv1_1() {
        // Arrange
        final String MSG = "This is a test message"
        final String url = "${HTTPS_URL}/ReverseHandler.groovy?string=${URLEncoder.encode(MSG, "UTF-8")}"

        // Configure server with TLSv1.1 only
        server = createServer([TLSv1_1])

        // Start server
        server.start()

        // Act
        String response = new URL(url).text
        logger.info("Response from ${HTTPS_URL}: ${response}")

        // Assert
        assert response == MSG.reverse()
    }

    @Ignore
    @Test
    public void testDefaultShouldSupportTLSv1_2() {
        // Arrange
        final String MSG = "This is a test message"
        final String url = "${HTTPS_URL}/ReverseHandler.groovy?string=${URLEncoder.encode(MSG, "UTF-8")}"

        // Configure server with TLSv1.2 only
        server = createServer([TLSv1_2])

        // Start server
        server.start()

        // Act
        String response = new URL(url).text
        logger.info("Response from ${HTTPS_URL}: ${response}")

        // Assert
        assert response == MSG.reverse()
    }

    @Ignore
    @Test
    public void testDefaultClient() {
        // Arrange
        final String MSG = "This is a test message"
        final String url = "https://www.ssllabs.com/ssltest/viewMyClient.html"

        // Act
        String response = new URL(url).text
        logger.info("Response from ${url}: ${response}")

        // Assert
        assert response
    }

    @Ignore
    @Test
    public void testDefaultShouldPreferTLSv1_2() {
        // Arrange
        final String MSG = "This is a test message"
        final String url = "${HTTPS_URL}/ReverseHandler.groovy?string=${URLEncoder.encode(MSG, "UTF-8")}"

        // Configure server with all TLS protocols
        server = createServer()

        // Start server
        server.start()

        // Act
        String response = new URL(url).text
        logger.info("Response from ${HTTPS_URL}: ${response}")

        // Assert
        assert response == MSG.reverse()
    }

    @Test
    public void testPostHTTPShouldSupportTLSv1() {
        // Arrange
        final String MSG = "This is a test message"
        final String url = "${HTTPS_URL}/PostHandler.groovy"

        // Configure the test runner
        final SSLContextService sslContextService = new StandardSSLContextService()
        runner.addControllerService("ssl-context", sslContextService)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, truststorePath)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, truststorePassword)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, KEYSTORE_TYPE)
        runner.enableControllerService(sslContextService)

        runner.setProperty(PostHTTP.URL, url)
        runner.setProperty(PostHTTP.SSL_CONTEXT_SERVICE, "ssl-context")
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "false")

        // Configure server with TLSv1 only
        keystorePath = "src/test/resources/localhost-ks.jks"
        keystorePassword = "localtest"
        truststorePath = "src/test/resources/localhost-ts.jks"
        truststorePassword = "localtest"
        server = createServer([TLSv1])

        // Start server
        server.start()

        // Act
        runner.enqueue(MSG.getBytes())
        runner.run()

        // Assert
        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS, 1)
    }

    @Test
    public void testPostHTTPShouldSupportTLSv1_1() {
        // Arrange
        final String MSG = "This is a test message"
        final String url = "${HTTPS_URL}/PostHandler.groovy"

        // Configure the test runner
        final SSLContextService sslContextService = new StandardSSLContextService()
        runner.addControllerService("ssl-context", sslContextService)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, truststorePath)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, truststorePassword)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, KEYSTORE_TYPE)
        runner.setProperty(sslContextService, StandardSSLContextService.SSL_ALGORITHM, "TLSv1.1")
        runner.enableControllerService(sslContextService)

        logger.info("PostHTTP supported cipher suites: ${sslContextService.createSSLContext(SSLContextService.ClientAuth.NONE).supportedSSLParameters.cipherSuites}")

        runner.setProperty(PostHTTP.URL, url)
        runner.setProperty(PostHTTP.SSL_CONTEXT_SERVICE, "ssl-context")
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "false")
//        runner.setProperty(PostHTTP.SEND_AS_FLOWFILE, "false")

        // Configure server with TLSv1.1 only
        keystorePath = "src/test/resources/localhost-ks.jks"
        keystorePassword = "localtest"
        truststorePath = "src/test/resources/localhost-ts.jks"
        truststorePassword = "localtest"
        server = createServer([TLSv1_1])

        // Start server
        server.start()

        // Act
        runner.enqueue(MSG.getBytes())
        runner.run()

        // Assert
        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS, 1)
    }

    @Test
    public void testPostHTTPShouldSupportTLSv1_2() {
        // Arrange
        final String MSG = "This is a test message"
        final String url = "${HTTPS_URL}/PostHandler.groovy"

        // Configure the test runner
        final SSLContextService sslContextService = new StandardSSLContextService()
        runner.addControllerService("ssl-context", sslContextService)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, truststorePath)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, truststorePassword)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, KEYSTORE_TYPE)
        runner.enableControllerService(sslContextService)

        logger.info("PostHTTP supported cipher suites: ${sslContextService.createSSLContext(SSLContextService.ClientAuth.NONE).supportedSSLParameters.cipherSuites}")

        runner.setProperty(PostHTTP.URL, url)
        runner.setProperty(PostHTTP.SSL_CONTEXT_SERVICE, "ssl-context")
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "false")

        // Configure server with TLSv1.2 only
        server = createServer([TLSv1_2])

        // Start server
        server.start()

        // Act
        runner.enqueue(MSG.getBytes())
        runner.run()

        // Assert
        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS, 1)
    }

    @Test
    public void testPostHTTPShouldPreferTLSv1_2() {
        // Arrange
        final String MSG = "This is a test message"
        final String url = "${HTTPS_URL}/PostHandler.groovy"

        // Configure the test runner
        final SSLContextService sslContextService = new StandardSSLContextService()
        runner.addControllerService("ssl-context", sslContextService)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, truststorePath)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, truststorePassword)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, KEYSTORE_TYPE)
        runner.enableControllerService(sslContextService)

        logger.info("PostHTTP supported cipher suites: ${sslContextService.createSSLContext(SSLContextService.ClientAuth.NONE).supportedSSLParameters.cipherSuites}")

        runner.setProperty(PostHTTP.URL, url)
        runner.setProperty(PostHTTP.SSL_CONTEXT_SERVICE, "ssl-context")
        runner.setProperty(PostHTTP.CHUNKED_ENCODING, "false")

        // Configure server with all TLS protocols
        keystorePath = "src/test/resources/localhost-ks.jks"
        keystorePassword = "localtest"
        truststorePath = "src/test/resources/localhost-ts.jks"
        truststorePassword = "localtest"
        server = createServer()

        // Start server
        server.start()

        // Act
        runner.enqueue(MSG.getBytes())
        runner.run()

        // Assert
        runner.assertAllFlowFilesTransferred(PostHTTP.REL_SUCCESS, 1)
    }

    @Ignore("Runs a server temporarily until stopped for testing with s_client")
    @Test
    public void runServer() {

        keystorePath = "src/test/resources/localhost-ks.jks"
        keystorePassword = "localtest"
        truststorePath = "src/test/resources/localhost-ts.jks"
        truststorePassword = "localtest"

        // Configure server with TLSv1 only
        server = createServer()

        // Start server
        server.start()

        boolean exit = false

        while (!exit) {
            sleep(5000)
            logger.info("Still running")
        }
    }
}