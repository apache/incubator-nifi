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

import com.burgstaller.okhttp.AuthenticationCacheInterceptor;
import com.burgstaller.okhttp.CachingAuthenticatorDecorator;
import com.burgstaller.okhttp.digest.CachingAuthenticator;
import com.burgstaller.okhttp.digest.DigestAuthenticator;
import okhttp3.Cache;
import okhttp3.ConnectionPool;
import okhttp3.Credentials;
import okhttp3.Handshake;
import okhttp3.Headers;
import okhttp3.JavaNetCookieJar;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.MultipartBody.Builder;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSink;
import okio.GzipSink;
import okio.Okio;
import okio.Source;
import org.apache.commons.io.input.TeeInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.URLValidator;
import org.apache.nifi.processors.standard.http.ContentEncodingStrategy;
import org.apache.nifi.processors.standard.http.CookieStrategy;
import org.apache.nifi.processors.standard.http.FlowFileNamingStrategy;
import org.apache.nifi.processors.standard.http.HttpHeader;
import org.apache.nifi.processors.standard.http.HttpMethod;
import org.apache.nifi.processors.standard.util.ProxyAuthenticator;
import org.apache.nifi.processors.standard.util.SoftLimitBoundedByteArrayOutputStream;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.stream.io.StreamUtils;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.Principal;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;

@SupportsSensitiveDynamicProperties
@SupportsBatching
@Tags({"http", "https", "rest", "client"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("An HTTP client processor which can interact with a configurable HTTP Endpoint. The destination URL and HTTP Method are configurable."
        + " When the HTTP Method is PUT, POST or PATCH, the FlowFile contents are included as the body of the request and FlowFile attributes are converted"
        + " to HTTP headers, optionally, based on configuration properties.")
@WritesAttributes({
        @WritesAttribute(attribute = InvokeHTTP.STATUS_CODE, description = "The status code that is returned"),
        @WritesAttribute(attribute = InvokeHTTP.STATUS_MESSAGE, description = "The status message that is returned"),
        @WritesAttribute(attribute = InvokeHTTP.RESPONSE_BODY, description = "In the instance where the status code received is not a success (2xx) "
                + "then the response body will be put to the 'invokehttp.response.body' attribute of the request FlowFile."),
        @WritesAttribute(attribute = InvokeHTTP.REQUEST_URL, description = "The original request URL"),
        @WritesAttribute(attribute = InvokeHTTP.REQUEST_DURATION, description = "Duration (in milliseconds) of the HTTP call to the external endpoint"),
        @WritesAttribute(attribute = InvokeHTTP.RESPONSE_URL, description = "The URL that was ultimately requested after any redirects were followed"),
        @WritesAttribute(attribute = InvokeHTTP.TRANSACTION_ID, description = "The transaction ID that is returned after reading the response"),
        @WritesAttribute(attribute = InvokeHTTP.REMOTE_DN, description = "The DN of the remote server"),
        @WritesAttribute(attribute = InvokeHTTP.EXCEPTION_CLASS, description = "The Java exception class raised when the processor fails"),
        @WritesAttribute(attribute = InvokeHTTP.EXCEPTION_MESSAGE, description = "The Java exception message raised when the processor fails"),
        @WritesAttribute(attribute = "user-defined", description = "If the 'Put Response Body In Attribute' property is set then whatever it is set to "
                + "will become the attribute key and the value would be the body of the HTTP response.")})
@DynamicProperties({
        @DynamicProperty(name = "Header Name", value = "Attribute Expression Language", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                description =
                        "Send request header with a key matching the Dynamic Property Key and a value created by evaluating "
                                + "the Attribute Expression Language set in the value of the Dynamic Property."),
        @DynamicProperty(name = "post:form:<NAME>", value = "Attribute Expression Language", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                description =
                        "When the HTTP Method is POST, dynamic properties with the property name in the form of post:form:<NAME>,"
                                + " where the <NAME> will be the form data name, will be used to fill out the multipart form parts."
                                + "  If send message body is false, the flowfile will not be sent, but any other form data will be.")
})
public class InvokeHTTP extends AbstractProcessor {
    public final static String STATUS_CODE = "invokehttp.status.code";
    public final static String STATUS_MESSAGE = "invokehttp.status.message";
    public final static String RESPONSE_BODY = "invokehttp.response.body";
    public final static String REQUEST_URL = "invokehttp.request.url";
    public final static String REQUEST_DURATION = "invokehttp.request.duration";
    public final static String RESPONSE_URL = "invokehttp.response.url";
    public final static String TRANSACTION_ID = "invokehttp.tx.id";
    public final static String REMOTE_DN = "invokehttp.remote.dn";
    public final static String EXCEPTION_CLASS = "invokehttp.java.exception.class";
    public final static String EXCEPTION_MESSAGE = "invokehttp.java.exception.message";

    public static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";

    protected static final String FORM_DATA_NAME_BASE = "post:form";
    private static final Pattern FORM_DATA_NAME_PARAMETER_PATTERN = Pattern.compile("post:form:(?<formDataName>.*)$");
    private static final String FORM_DATA_NAME_GROUP = "formDataName";

    private static final Set<String> IGNORED_REQUEST_ATTRIBUTES = Set.of(
            STATUS_CODE,
            STATUS_MESSAGE,
            RESPONSE_BODY,
            REQUEST_URL,
            RESPONSE_URL,
            TRANSACTION_ID,
            REMOTE_DN,
            EXCEPTION_CLASS,
            EXCEPTION_MESSAGE,
            CoreAttributes.UUID.key(),
            CoreAttributes.PATH.key()
    );

    public static final PropertyDescriptor HTTP_METHOD = new PropertyDescriptor.Builder()
            .name("HTTP Method")
            .description("HTTP request method (GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS). Arbitrary methods are also supported. "
                    + "Methods other than POST, PUT and PATCH will be sent without a message body.")
            .required(true)
            .defaultValue(HttpMethod.GET.name())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();

    public static final PropertyDescriptor HTTP_URL = new PropertyDescriptor.Builder()
            .name("HTTP URL")
            .description("HTTP remote URL including a scheme of http or https, as well as a hostname or IP address with optional port and path elements." +
                    " Any encoding of the URL must be done by the user.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor HTTP2_DISABLED = new PropertyDescriptor.Builder()
            .name("HTTP/2 Disabled")
            .description("Disable negotiation of HTTP/2 protocol. HTTP/2 requires TLS. HTTP/1.1 protocol supported is required when HTTP/2 is disabled.")
            .required(true)
            .defaultValue("False")
            .allowableValues("True", "False")
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("SSL Context Service provides trusted certificates and client certificates for TLS communication.")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();

    public static final PropertyDescriptor SOCKET_CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .description("Maximum time to wait for initial socket connection to the HTTP URL.")
            .required(true)
            .defaultValue("5 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOCKET_READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Socket Read Timeout")
            .description("Maximum time to wait for receiving responses from a socket connection to the HTTP URL.")
            .required(true)
            .defaultValue("15 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOCKET_WRITE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Socket Write Timeout")
            .description("Maximum time to wait for write operations while sending requests from a socket connection to the HTTP URL.")
            .required(true)
            .defaultValue("15 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOCKET_IDLE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Socket Idle Timeout")
            .description("Maximum time to wait before closing idle connections to the HTTP URL.")
            .required(true)
            .defaultValue("5 mins")
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.MILLISECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
            .build();

    public static final PropertyDescriptor SOCKET_IDLE_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("Socket Idle Connections")
            .description("Maximum number of idle connections to the HTTP URL.")
            .required(true)
            .defaultValue("5")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor REQUEST_OAUTH2_ACCESS_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("Request OAuth2 Access Token Provider")
            .description("Enables managed retrieval of OAuth2 Bearer Token applied to HTTP requests using the Authorization Header.")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .required(false)
            .build();

    public static final PropertyDescriptor REQUEST_USERNAME = new PropertyDescriptor.Builder()
            .name("Request Username")
            .description("The username provided for authentication of HTTP requests. Encoded using Base64 for HTTP Basic Authentication as described in RFC 7617.")
            .required(false)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x39\\x3b-\\x7e\\x80-\\xff]+$")))
            .build();

    public static final PropertyDescriptor REQUEST_PASSWORD = new PropertyDescriptor.Builder()
            .name("Request Password")
            .description("The password provided for authentication of HTTP requests. Encoded using Base64 for HTTP Basic Authentication as described in RFC 7617.")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x7e\\x80-\\xff]+$")))
            .build();

    public static final PropertyDescriptor REQUEST_DIGEST_AUTHENTICATION_ENABLED = new PropertyDescriptor.Builder()
            .name("Request Digest Authentication Enabled")
            .description("Enable Digest Authentication on HTTP requests with Username and Password credentials as described in RFC 7616.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .dependsOn(REQUEST_USERNAME)
            .build();

    public static final PropertyDescriptor REQUEST_FAILURE_PENALIZATION_ENABLED = new PropertyDescriptor.Builder()
            .name("Request Failure Penalization Enabled")
            .description("Enable penalization of request FlowFiles when receiving HTTP response with a status code between 400 and 499.")
            .required(false)
            .defaultValue(Boolean.FALSE.toString())
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .build();

    public static final PropertyDescriptor REQUEST_BODY_ENABLED = new PropertyDescriptor.Builder()
            .name("Request Body Enabled")
            .description("Enable sending HTTP request body for PATCH, POST, or PUT methods.")
            .defaultValue(Boolean.TRUE.toString())
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .required(false)
            .dependsOn(HTTP_METHOD, HttpMethod.PATCH.name(), HttpMethod.POST.name(), HttpMethod.PUT.name())
            .build();

    public static final PropertyDescriptor REQUEST_FORM_DATA_NAME = new PropertyDescriptor.Builder()
            .name("Request Multipart Form-Data Name")
            .description("Enable sending HTTP request body formatted using multipart/form-data and using the form name configured.")
            .required(false)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(REQUEST_BODY_ENABLED, Boolean.TRUE.toString())
            .build();

    public static final PropertyDescriptor REQUEST_FORM_DATA_FILENAME_ENABLED = new PropertyDescriptor.Builder()
            .name("Request Multipart Form-Data Filename Enabled")
            .description("Enable sending the FlowFile filename attribute as the filename parameter in the Content-Disposition Header for multipart/form-data HTTP requests.")
            .required(false)
            .defaultValue(Boolean.TRUE.toString())
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .dependsOn(REQUEST_FORM_DATA_NAME)
            .build();

    public static final PropertyDescriptor REQUEST_CHUNKED_TRANSFER_ENCODING_ENABLED = new PropertyDescriptor.Builder()
            .name("Request Chunked Transfer-Encoding Enabled")
            .description("Enable sending HTTP requests with the Transfer-Encoding Header set to chunked, and disable sending the Content-Length Header. " +
                    "Transfer-Encoding applies to the body in HTTP/1.1 requests as described in RFC 7230 Section 3.3.1")
            .required(true)
            .defaultValue(Boolean.FALSE.toString())
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .dependsOn(HTTP_METHOD, HttpMethod.PATCH.name(), HttpMethod.POST.name(), HttpMethod.PUT.name())
            .build();

    public static final PropertyDescriptor REQUEST_CONTENT_ENCODING = new PropertyDescriptor.Builder()
            .name("Request Content-Encoding")
            .description("HTTP Content-Encoding applied to request body during transmission. The receiving server must support the selected encoding to avoid request failures.")
            .required(true)
            .defaultValue(ContentEncodingStrategy.DISABLED)
            .allowableValues(ContentEncodingStrategy.class)
            .dependsOn(HTTP_METHOD, HttpMethod.PATCH.name(), HttpMethod.POST.name(), HttpMethod.PUT.name())
            .build();

    public static final PropertyDescriptor REQUEST_CONTENT_TYPE = new PropertyDescriptor.Builder()
            .name("Request Content-Type")
            .description("HTTP Content-Type Header applied to when sending an HTTP request body for PATCH, POST, or PUT methods. " +
                    String.format("The Content-Type defaults to %s when not configured.", DEFAULT_CONTENT_TYPE))
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${" + CoreAttributes.MIME_TYPE.key() + "}")
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .dependsOn(HTTP_METHOD, HttpMethod.PATCH.name(), HttpMethod.POST.name(), HttpMethod.PUT.name())
            .build();

    public static final PropertyDescriptor REQUEST_DATE_HEADER_ENABLED = new PropertyDescriptor.Builder()
            .name("Request Date Header Enabled")
            .description("Enable sending HTTP Date Header on HTTP requests as described in RFC 7231 Section 7.1.1.2.")
            .required(true)
            .defaultValue("True")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor REQUEST_HEADER_ATTRIBUTES_PATTERN = new PropertyDescriptor.Builder()
            .name("Request Header Attributes Pattern")
            .description("Regular expression that defines which FlowFile attributes to send as HTTP headers in the request. "
                    + "If not defined, no attributes are sent as headers. Dynamic properties will be always be sent as headers. "
                    + "The dynamic property name will be the header key and the dynamic property value, interpreted as Expression "
                    + "Language, will be the header value. Attributes and their values are limited to ASCII characters due to "
                    + "the requirement of the HTTP protocol.")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor REQUEST_USER_AGENT = new PropertyDescriptor.Builder()
            .name("Request User-Agent")
            .description("HTTP User-Agent Header applied to requests. RFC 7231 Section 5.5.3 describes recommend formatting.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RESPONSE_BODY_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Response Body Attribute Name")
            .description("FlowFile attribute name used to write an HTTP response body for FlowFiles transferred to the Original relationship.")
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor RESPONSE_BODY_ATTRIBUTE_SIZE = new PropertyDescriptor.Builder()
            .name("Response Body Attribute Size")
            .description("Maximum size in bytes applied when writing an HTTP response body to a FlowFile attribute. Attributes exceeding the maximum will be truncated.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("256")
            .dependsOn(RESPONSE_BODY_ATTRIBUTE_NAME)
            .build();

    public static final PropertyDescriptor RESPONSE_BODY_IGNORED = new PropertyDescriptor.Builder()
            .name("Response Body Ignored")
            .description("Disable writing HTTP response FlowFiles to Response relationship")
            .required(true)
            .defaultValue(Boolean.FALSE.toString())
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .build();

    public static final PropertyDescriptor RESPONSE_CACHE_ENABLED = new PropertyDescriptor.Builder()
            .name("Response Cache Enabled")
            .description("Enable HTTP response caching described in RFC 7234. Caching responses considers ETag and other headers.")
            .required(true)
            .defaultValue(Boolean.FALSE.toString())
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .build();

    public static final PropertyDescriptor RESPONSE_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("Response Cache Size")
            .description("Maximum size of HTTP response cache in bytes. Caching responses considers ETag and other headers.")
            .required(true)
            .defaultValue("10MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .dependsOn(RESPONSE_CACHE_ENABLED, Boolean.TRUE.toString())
            .build();

    public static final PropertyDescriptor RESPONSE_COOKIE_STRATEGY = new PropertyDescriptor.Builder()
            .name("Response Cookie Strategy")
            .description("Strategy for accepting and persisting HTTP cookies. Accepting cookies enables persistence across multiple requests.")
            .required(true)
            .defaultValue(CookieStrategy.DISABLED.name())
            .allowableValues(CookieStrategy.values())
            .build();

    public static final PropertyDescriptor RESPONSE_GENERATION_REQUIRED = new PropertyDescriptor.Builder()
            .name("Response Generation Required")
            .description("Enable generation and transfer of a FlowFile to the Response relationship regardless of HTTP response received.")
            .required(false)
            .defaultValue(Boolean.FALSE.toString())
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .build();

    public static final PropertyDescriptor RESPONSE_FLOW_FILE_NAMING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Response FlowFile Naming Strategy")
            .description("Determines the strategy used for setting the filename attribute of FlowFiles transferred to the Response relationship.")
            .required(true)
            .defaultValue(FlowFileNamingStrategy.RANDOM.name())
            .allowableValues(
                    Arrays.stream(FlowFileNamingStrategy.values()).map(strategy ->
                            new AllowableValue(strategy.name(), strategy.name(), strategy.getDescription())
                    ).toArray(AllowableValue[]::new)
            )
            .build();

    public static final PropertyDescriptor RESPONSE_HEADER_REQUEST_ATTRIBUTES_ENABLED = new PropertyDescriptor.Builder()
            .name("Response Header Request Attributes Enabled")
            .description("Enable adding HTTP response headers as attributes to FlowFiles transferred to the Original, Retry or No Retry relationships.")
            .required(false)
            .defaultValue(Boolean.FALSE.toString())
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .build();

    public static final PropertyDescriptor RESPONSE_HEADER_REQUEST_ATTRIBUTES_PREFIX = new PropertyDescriptor.Builder()
            .name("Response Header Request Attributes Prefix")
            .description("Prefix to HTTP response headers when included as attributes to FlowFiles transferred to the Original, Retry or No Retry relationships.  "
                + "It is recommended to end with a separator character like '.' or '-'.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(RESPONSE_HEADER_REQUEST_ATTRIBUTES_ENABLED, Boolean.TRUE.toString())
            .build();

    public static final PropertyDescriptor RESPONSE_REDIRECTS_ENABLED = new PropertyDescriptor.Builder()
            .name("Response Redirects Enabled")
            .description("Enable following HTTP redirects sent with HTTP 300 series responses as described in RFC 7231 Section 6.4.")
            .required(true)
            .defaultValue("True")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH, ProxySpec.SOCKS};

    private static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = ProxyConfiguration.createProxyConfigPropertyDescriptor(PROXY_SPECS);

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            HTTP_METHOD,
            HTTP_URL,
            HTTP2_DISABLED,
            SSL_CONTEXT_SERVICE,
            SOCKET_CONNECT_TIMEOUT,
            SOCKET_READ_TIMEOUT,
            SOCKET_WRITE_TIMEOUT,
            SOCKET_IDLE_TIMEOUT,
            SOCKET_IDLE_CONNECTIONS,
            PROXY_CONFIGURATION_SERVICE,
            REQUEST_OAUTH2_ACCESS_TOKEN_PROVIDER,
            REQUEST_USERNAME,
            REQUEST_PASSWORD,
            REQUEST_DIGEST_AUTHENTICATION_ENABLED,
            REQUEST_FAILURE_PENALIZATION_ENABLED,
            REQUEST_BODY_ENABLED,
            REQUEST_FORM_DATA_NAME,
            REQUEST_FORM_DATA_FILENAME_ENABLED,
            REQUEST_CHUNKED_TRANSFER_ENCODING_ENABLED,
            REQUEST_CONTENT_ENCODING,
            REQUEST_CONTENT_TYPE,
            REQUEST_DATE_HEADER_ENABLED,
            REQUEST_HEADER_ATTRIBUTES_PATTERN,
            REQUEST_USER_AGENT,
            RESPONSE_BODY_ATTRIBUTE_NAME,
            RESPONSE_BODY_ATTRIBUTE_SIZE,
            RESPONSE_BODY_IGNORED,
            RESPONSE_CACHE_ENABLED,
            RESPONSE_CACHE_SIZE,
            RESPONSE_COOKIE_STRATEGY,
            RESPONSE_GENERATION_REQUIRED,
            RESPONSE_FLOW_FILE_NAMING_STRATEGY,
            RESPONSE_HEADER_REQUEST_ATTRIBUTES_ENABLED,
            RESPONSE_HEADER_REQUEST_ATTRIBUTES_PREFIX,
            RESPONSE_REDIRECTS_ENABLED
    );

    public static final Relationship ORIGINAL = new Relationship.Builder()
            .name("Original")
            .description("Request FlowFiles transferred when receiving HTTP responses with a status code between 200 and 299.")
            .build();

    public static final Relationship RESPONSE = new Relationship.Builder()
            .name("Response")
            .description("Response FlowFiles transferred when receiving HTTP responses with a status code between 200 and 299.")
            .build();

    public static final Relationship RETRY = new Relationship.Builder()
            .name("Retry")
            .description("Request FlowFiles transferred when receiving HTTP responses with a status code between 500 and 599.")
            .build();

    public static final Relationship NO_RETRY = new Relationship.Builder()
            .name("No Retry")
            .description("Request FlowFiles transferred when receiving HTTP responses with a status code between 400 an 499.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Request FlowFiles transferred when receiving socket communication errors.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            ORIGINAL,
            RESPONSE,
            RETRY,
            NO_RETRY,
            FAILURE
    );

    // RFC 2616 Date Time Formatter with hard-coded GMT Zone and US Locale. RFC 2616 Section 3.3 indicates the header should not be localized
    private static final DateTimeFormatter RFC_2616_DATE_TIME = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);

    private static final String MULTIPLE_HEADER_DELIMITER = ", ";

    private volatile Set<String> dynamicPropertyNames = new HashSet<>();

    private volatile Pattern requestHeaderAttributesPattern = null;

    private volatile boolean chunkedTransferEncoding = false;

    private volatile Optional<OAuth2AccessTokenProvider> oauth2AccessTokenProviderOptional;

    private final AtomicReference<OkHttpClient> okHttpClientAtomicReference = new AtomicReference<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        if (propertyDescriptorName.startsWith(FORM_DATA_NAME_BASE)) {

            Matcher matcher = FORM_DATA_NAME_PARAMETER_PATTERN.matcher(propertyDescriptorName);
            if (matcher.matches()) {
                return new PropertyDescriptor.Builder()
                        .required(false)
                        .name(propertyDescriptorName)
                        .description("Form Data " + matcher.group(FORM_DATA_NAME_GROUP))
                        .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                        .dynamic(true)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .build();
            }
            return null;
        }

        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        if (config.isPropertySet("Proxy Host")) {
            final Map<String, String> serviceProperties = new HashMap<>();
            serviceProperties.put("proxy-type", config.getRawPropertyValue("Proxy Type").map(String::toUpperCase).orElse(null));
            serviceProperties.put("proxy-server-host", config.getRawPropertyValue("Proxy Host").orElse(null));
            serviceProperties.put("proxy-server-port", config.getRawPropertyValue("Proxy Port").orElse(null));
            serviceProperties.put("proxy-user-name", config.getRawPropertyValue("invokehttp-proxy-user").orElse(null));
            serviceProperties.put("proxy-user-password", config.getRawPropertyValue("invokehttp-proxy-password").orElse(null));

            final String serviceId = config.createControllerService("org.apache.nifi.proxy.StandardProxyConfigurationService", serviceProperties);
            config.setProperty(PROXY_CONFIGURATION_SERVICE, serviceId);
        } else {
            config.removeProperty("Proxy Type");
        }

        config.renameProperty("Read Timeout", SOCKET_READ_TIMEOUT.getName());
        config.renameProperty("Remote URL", HTTP_URL.getName());
        config.renameProperty("disable-http2", HTTP2_DISABLED.getName());
        config.renameProperty("idle-timeout", SOCKET_IDLE_TIMEOUT.getName());
        config.renameProperty("max-idle-connections", SOCKET_IDLE_CONNECTIONS.getName());
        config.renameProperty("oauth2-access-token-provider", REQUEST_OAUTH2_ACCESS_TOKEN_PROVIDER.getName());
        config.renameProperty("Basic Authentication Username", REQUEST_USERNAME.getName());
        config.renameProperty("Basic Authentication Password", REQUEST_PASSWORD.getName());
        config.renameProperty("Digest Authentication", REQUEST_DIGEST_AUTHENTICATION_ENABLED.getName());
        config.renameProperty("Penalize on \"No Retry\"", REQUEST_FAILURE_PENALIZATION_ENABLED.getName());
        config.renameProperty("send-message-body", REQUEST_BODY_ENABLED.getName());
        config.renameProperty("form-body-form-name", REQUEST_FORM_DATA_NAME.getName());
        config.renameProperty("set-form-filename", REQUEST_FORM_DATA_FILENAME_ENABLED.getName());
        config.renameProperty("Use Chunked Encoding", REQUEST_CHUNKED_TRANSFER_ENCODING_ENABLED.getName());
        config.renameProperty("Content-Encoding", REQUEST_CONTENT_ENCODING.getName());
        config.renameProperty("Content-Type", REQUEST_CONTENT_TYPE.getName());
        config.renameProperty("Include Date Header", REQUEST_DATE_HEADER_ENABLED.getName());
        config.renameProperty("Attributes to Send", REQUEST_HEADER_ATTRIBUTES_PATTERN.getName());
        config.renameProperty("Useragent", REQUEST_USER_AGENT.getName());
        config.renameProperty("Put Response Body In Attribute", RESPONSE_BODY_ATTRIBUTE_NAME.getName());
        config.renameProperty("Max Length To Put In Attribute", RESPONSE_BODY_ATTRIBUTE_SIZE.getName());
        config.renameProperty("ignore-response-content", RESPONSE_BODY_IGNORED.getName());
        config.renameProperty("use-etag", RESPONSE_CACHE_ENABLED.getName());
        config.renameProperty("etag-max-cache-size", RESPONSE_CACHE_SIZE.getName());
        config.renameProperty("cookie-strategy", RESPONSE_COOKIE_STRATEGY.getName());
        config.renameProperty("Always Output Response", RESPONSE_GENERATION_REQUIRED.getName());
        config.renameProperty("flow-file-naming-strategy", RESPONSE_FLOW_FILE_NAMING_STRATEGY.getName());
        config.renameProperty("Add Response Headers to Request", RESPONSE_HEADER_REQUEST_ATTRIBUTES_ENABLED.getName());
        config.renameProperty("Follow Redirects", RESPONSE_REDIRECTS_ENABLED.getName());
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.isDynamic()) {
            final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);
            if (newValue == null) {
                newDynamicPropertyNames.remove(descriptor.getName());
            } else if (oldValue == null) {    // new property
                newDynamicPropertyNames.add(descriptor.getName());
            }
            this.dynamicPropertyNames = Set.copyOf(newDynamicPropertyNames);
        } else {
            // compile the attributes-to-send filter pattern
            if (REQUEST_HEADER_ATTRIBUTES_PATTERN.getName().equalsIgnoreCase(descriptor.getName())) {
                if (newValue == null || newValue.isEmpty()) {
                    requestHeaderAttributesPattern = null;
                } else {
                    final String trimmedValue = StringUtils.trimToEmpty(newValue);
                    requestHeaderAttributesPattern = Pattern.compile(trimmedValue);
                }
            }
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        ProxyConfiguration.validateProxySpec(validationContext, results, PROXY_SPECS);

        // Check for dynamic properties for form components.
        // Even if the flowfile is not sent, we may still send form parameters.
        boolean hasFormData = false;
        for (final PropertyDescriptor descriptor : validationContext.getProperties().keySet()) {
            final Matcher matcher = FORM_DATA_NAME_PARAMETER_PATTERN.matcher(descriptor.getName());
            if (matcher.matches()) {
                hasFormData = true;
                break;
            }
        }

        // if form data exists, and send body is true, Flowfile Form Data Name must be set.
        final boolean requestBodyEnabled = validationContext.getProperty(REQUEST_BODY_ENABLED).asBoolean();
        final boolean contentNameSet = validationContext.getProperty(REQUEST_FORM_DATA_NAME).isSet();
        if (hasFormData) {
            if (requestBodyEnabled && !contentNameSet) {
                final String explanation = String.format("[%s] is required when Form Data properties are configured and [%s] is enabled",
                        REQUEST_FORM_DATA_NAME.getDisplayName(),
                        REQUEST_BODY_ENABLED.getDisplayName());
                results.add(new ValidationResult.Builder()
                        .subject(REQUEST_FORM_DATA_NAME.getDisplayName())
                        .valid(false)
                        .explanation(explanation)
                        .build());
            }
        }
        if (!requestBodyEnabled && contentNameSet) {
            final String explanation = String.format("[%s] must be [true] when Form Data properties are configured and [%s] is configured",
                    REQUEST_BODY_ENABLED.getDisplayName(),
                    REQUEST_FORM_DATA_NAME.getDisplayName());
            results.add(new ValidationResult.Builder()
                    .subject(REQUEST_FORM_DATA_NAME.getDisplayName())
                    .valid(false)
                    .explanation(explanation)
                    .build());
        }

        boolean usingUserNamePasswordAuthorization = validationContext.getProperty(REQUEST_USERNAME).isSet()
            || validationContext.getProperty(REQUEST_PASSWORD).isSet();

        boolean usingOAuth2Authorization = validationContext.getProperty(REQUEST_OAUTH2_ACCESS_TOKEN_PROVIDER).isSet();

        if (usingUserNamePasswordAuthorization && usingOAuth2Authorization) {
            results.add(new ValidationResult.Builder()
                .subject("Authorization properties")
                .valid(false)
                .explanation("OAuth2 Authorization cannot be configured together with Username and Password properties")
                .build());
        }

        return results;
    }

    @OnScheduled
    public void setUpClient(final ProcessContext context) throws IOException {
        okHttpClientAtomicReference.set(null);

        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient().newBuilder();

        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(context);

        final Proxy proxy = proxyConfig.createProxy();
        if (!Type.DIRECT.equals(proxy.type())) {
            okHttpClientBuilder.proxy(proxy);
            if (proxyConfig.hasCredential()) {
                ProxyAuthenticator proxyAuthenticator = new ProxyAuthenticator(proxyConfig.getProxyUserName(), proxyConfig.getProxyUserPassword());
                okHttpClientBuilder.proxyAuthenticator(proxyAuthenticator);
            }
        }

        // Configure caching
        final boolean cachingEnabled = context.getProperty(RESPONSE_CACHE_ENABLED).asBoolean();
        if (cachingEnabled) {
            final int maxCacheSizeBytes = context.getProperty(RESPONSE_CACHE_SIZE).asDataSize(DataUnit.B).intValue();
            okHttpClientBuilder.cache(new Cache(getResponseCacheDirectory(), maxCacheSizeBytes));
        }

        if (context.getProperty(HTTP2_DISABLED).asBoolean()) {
            okHttpClientBuilder.protocols(List.of(Protocol.HTTP_1_1));
        }

        okHttpClientBuilder.followRedirects(context.getProperty(RESPONSE_REDIRECTS_ENABLED).asBoolean());
        okHttpClientBuilder.connectTimeout((context.getProperty(SOCKET_CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue()), TimeUnit.MILLISECONDS);
        okHttpClientBuilder.readTimeout(context.getProperty(SOCKET_READ_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS);
        okHttpClientBuilder.writeTimeout(context.getProperty(SOCKET_WRITE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS);
        okHttpClientBuilder.connectionPool(
                new ConnectionPool(
                        context.getProperty(SOCKET_IDLE_CONNECTIONS).asInteger(),
                        context.getProperty(SOCKET_IDLE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS
                )
        );

        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        if (sslContextProvider != null) {
            final SSLContext sslContext = sslContextProvider.createContext();
            final SSLSocketFactory socketFactory = sslContext.getSocketFactory();
            final X509TrustManager trustManager = sslContextProvider.createTrustManager();
            okHttpClientBuilder.sslSocketFactory(socketFactory, trustManager);
        }

        final CookieStrategy cookieStrategy = CookieStrategy.valueOf(context.getProperty(RESPONSE_COOKIE_STRATEGY).getValue());
        switch (cookieStrategy) {
            case DISABLED:
                break;
            case ACCEPT_ALL:
                final CookieManager cookieManager = new CookieManager();
                cookieManager.setCookiePolicy(CookiePolicy.ACCEPT_ALL);
                okHttpClientBuilder.cookieJar(new JavaNetCookieJar(cookieManager));
                break;
        }

        setAuthenticator(okHttpClientBuilder, context);

        chunkedTransferEncoding = context.getProperty(REQUEST_CHUNKED_TRANSFER_ENCODING_ENABLED).asBoolean();

        okHttpClientAtomicReference.set(okHttpClientBuilder.build());
    }

    @OnScheduled
    public void initOauth2AccessTokenProvider(final ProcessContext context) {
        if (context.getProperty(REQUEST_OAUTH2_ACCESS_TOKEN_PROVIDER).isSet()) {
            OAuth2AccessTokenProvider oauth2AccessTokenProvider = context.getProperty(REQUEST_OAUTH2_ACCESS_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);

            oauth2AccessTokenProvider.getAccessDetails();

            oauth2AccessTokenProviderOptional = Optional.of(oauth2AccessTokenProvider);
        } else {
            oauth2AccessTokenProviderOptional = Optional.empty();
        }
    }

    private void setAuthenticator(OkHttpClient.Builder okHttpClientBuilder, ProcessContext context) {
        final String authUser = trimToEmpty(context.getProperty(REQUEST_USERNAME).getValue());

        // If the username/password properties are set then check if digest auth is being used
        if (!authUser.isEmpty() && "true".equalsIgnoreCase(context.getProperty(REQUEST_DIGEST_AUTHENTICATION_ENABLED).getValue())) {
            final String authPass = trimToEmpty(context.getProperty(REQUEST_PASSWORD).getValue());

            final Map<String, CachingAuthenticator> authCache = new ConcurrentHashMap<>();
            com.burgstaller.okhttp.digest.Credentials credentials = new com.burgstaller.okhttp.digest.Credentials(authUser, authPass);
            final DigestAuthenticator digestAuthenticator = new DigestAuthenticator(credentials);

            okHttpClientBuilder.interceptors().add(new AuthenticationCacheInterceptor(authCache));
            okHttpClientBuilder.authenticator(new CachingAuthenticatorDecorator(digestAuthenticator, authCache));
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        OkHttpClient okHttpClient = okHttpClientAtomicReference.get();

        FlowFile requestFlowFile = session.get();

        // Checking to see if the property to put the body of the response in an attribute was set
        boolean putToAttribute = context.getProperty(RESPONSE_BODY_ATTRIBUTE_NAME).isSet();
        if (requestFlowFile == null) {
            if (context.hasNonLoopConnection()) {
                return;
            }

            final String method = getRequestMethod(context, null);
            final Optional<HttpMethod> requestMethodFound = findRequestMethod(method);
            final HttpMethod requestMethod = requestMethodFound.orElse(HttpMethod.GET);
            if (requestMethod.isRequestBodySupported()) {
                return;
            } else if (putToAttribute) {
                requestFlowFile = session.create();
            }
        }

        final int maxAttributeSize = context.getProperty(RESPONSE_BODY_ATTRIBUTE_SIZE).asInteger();
        final ComponentLog logger = getLogger();
        final UUID txId = UUID.randomUUID();

        FlowFile responseFlowFile = null;
        try {
            final String urlProperty = trimToEmpty(context.getProperty(HTTP_URL).evaluateAttributeExpressions(requestFlowFile).getValue());

            Request httpRequest = configureRequest(context, session, requestFlowFile, urlProperty);
            logRequest(logger, httpRequest);

            if (httpRequest.body() != null) {
                session.getProvenanceReporter().send(requestFlowFile, urlProperty, true);
            }

            final long startNanos = System.nanoTime();

            try (Response responseHttp = okHttpClient.newCall(httpRequest).execute()) {
                logResponse(logger, urlProperty, responseHttp);

                // store the status code and message
                int statusCode = responseHttp.code();
                String statusMessage = responseHttp.message();

                // Create a map of the status attributes that are always written to the request and response FlowFiles
                Map<String, String> statusAttributes = new HashMap<>();
                statusAttributes.put(STATUS_CODE, String.valueOf(statusCode));
                statusAttributes.put(STATUS_MESSAGE, statusMessage);
                statusAttributes.put(REQUEST_URL, urlProperty);
                statusAttributes.put(REQUEST_DURATION, Long.toString(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos)));
                statusAttributes.put(RESPONSE_URL, responseHttp.request().url().toString());
                statusAttributes.put(TRANSACTION_ID, txId.toString());

                if (requestFlowFile != null) {
                    requestFlowFile = session.putAllAttributes(requestFlowFile, statusAttributes);
                }

                boolean outputBodyToRequestAttribute = (!isSuccess(statusCode) || putToAttribute) && requestFlowFile != null;
                boolean outputBodyToResponseContent = (isSuccess(statusCode) && !putToAttribute) || context.getProperty(RESPONSE_GENERATION_REQUIRED).asBoolean();
                ResponseBody responseBody = responseHttp.body();
                boolean bodyExists = responseBody != null && !context.getProperty(RESPONSE_BODY_IGNORED).asBoolean();

                InputStream responseBodyStream = null;
                SoftLimitBoundedByteArrayOutputStream outputStreamToRequestAttribute = null;
                TeeInputStream teeInputStream = null;
                try {
                    responseBodyStream = bodyExists ? responseBody.byteStream() : null;
                    if (responseBodyStream != null && outputBodyToRequestAttribute && outputBodyToResponseContent) {
                        outputStreamToRequestAttribute = new SoftLimitBoundedByteArrayOutputStream(maxAttributeSize);
                        teeInputStream = new TeeInputStream(responseBodyStream, outputStreamToRequestAttribute);
                    }

                    if (outputBodyToResponseContent) {
                        /*
                         * If successful and putting to response flowfile, store the response body as the flowfile payload
                         * we include additional flowfile attributes including the response headers and the status codes.
                         */

                        // clone the flowfile to capture the response
                        if (requestFlowFile != null) {
                            responseFlowFile = session.create(requestFlowFile);
                        } else {
                            responseFlowFile = session.create();
                        }

                        // write attributes to response flowfile
                        responseFlowFile = session.putAllAttributes(responseFlowFile, statusAttributes);

                        // write the response headers as attributes
                        // this will overwrite any existing flowfile attributes
                        responseFlowFile = session.putAllAttributes(responseFlowFile, convertAttributesFromHeaders(responseHttp, ""));

                        // update FlowFile's filename attribute with an extracted value from the remote URL
                        if (FlowFileNamingStrategy.URL_PATH.equals(getFlowFileNamingStrategy(context)) && HttpMethod.GET.name().equals(httpRequest.method())) {
                            final URL url = URLValidator.createURL(urlProperty);
                            String fileName = getFileNameFromUrl(url);
                            if (fileName != null) {
                                responseFlowFile = session.putAttribute(responseFlowFile, CoreAttributes.FILENAME.key(), fileName);
                            }
                        }

                        // transfer the message body to the payload
                        // can potentially be null in edge cases
                        if (bodyExists) {
                            // write content type attribute to response flowfile if it is available
                            final MediaType contentType = responseBody.contentType();
                            if (contentType != null) {
                                responseFlowFile = session.putAttribute(responseFlowFile, CoreAttributes.MIME_TYPE.key(), contentType.toString());
                            }
                            if (teeInputStream != null) {
                                responseFlowFile = session.importFrom(teeInputStream, responseFlowFile);
                            } else {
                                responseFlowFile = session.importFrom(responseBodyStream, responseFlowFile);
                            }

                            // emit provenance event
                            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                            if (requestFlowFile != null) {
                                session.getProvenanceReporter().fetch(responseFlowFile, urlProperty, millis);
                            } else {
                                session.getProvenanceReporter().receive(responseFlowFile, urlProperty, millis);
                            }
                        }
                    }

                    // if not successful and request flowfile is not null, store the response body into a flowfile attribute
                    if (outputBodyToRequestAttribute && bodyExists) {
                        String attributeKey = context.getProperty(RESPONSE_BODY_ATTRIBUTE_NAME).evaluateAttributeExpressions(requestFlowFile).getValue();
                        if (attributeKey == null) {
                            attributeKey = RESPONSE_BODY;
                        }
                        byte[] outputBuffer;
                        int size;

                        if (outputStreamToRequestAttribute != null) {
                            outputBuffer = outputStreamToRequestAttribute.getBuffer();
                            size = outputStreamToRequestAttribute.size();
                        } else {
                            outputBuffer = new byte[maxAttributeSize];
                            size = StreamUtils.fillBuffer(responseBodyStream, outputBuffer, false);
                        }
                        String bodyString = new String(outputBuffer, 0, size, getCharsetFromMediaType(responseBody.contentType()));
                        requestFlowFile = session.putAttribute(requestFlowFile, attributeKey, bodyString);

                        final long processingDuration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                        final String eventDetails = String.format("Response Body Attribute Added [%s] Processing Duration [%d ms]", attributeKey, processingDuration);
                        session.getProvenanceReporter().modifyAttributes(requestFlowFile, eventDetails);
                    }
                } finally {
                    if (outputStreamToRequestAttribute != null) {
                        outputStreamToRequestAttribute.close();
                    }
                    if (teeInputStream != null) {
                        teeInputStream.close();
                    } else if (responseBodyStream != null) {
                        responseBodyStream.close();
                    }
                }

                // This needs to be done after the response flowFile has been created from the request flowFile
                // as the added attribute headers may have a prefix added that doesn't make sense for the response flowFile.
                if (context.getProperty(RESPONSE_HEADER_REQUEST_ATTRIBUTES_ENABLED).asBoolean() && requestFlowFile != null) {
                    final String prefix = context.getProperty(RESPONSE_HEADER_REQUEST_ATTRIBUTES_PREFIX).evaluateAttributeExpressions(requestFlowFile).getValue();

                    // write the response headers as attributes
                    // this will overwrite any existing flowfile attributes
                    requestFlowFile = session.putAllAttributes(requestFlowFile, convertAttributesFromHeaders(responseHttp, prefix));
                }

                route(requestFlowFile, responseFlowFile, session, context, statusCode);

            }
        } catch (final Exception e) {
            if (requestFlowFile == null) {
                logger.error("Request Processing failed", e);
                context.yield();
            } else {
                logger.error("Request Processing failed: {}", requestFlowFile, e);
                requestFlowFile = session.penalize(requestFlowFile);
                requestFlowFile = session.putAttribute(requestFlowFile, EXCEPTION_CLASS, e.getClass().getName());
                requestFlowFile = session.putAttribute(requestFlowFile, EXCEPTION_MESSAGE, e.getMessage());
                session.transfer(requestFlowFile, FAILURE);
            }

            if (responseFlowFile != null) {
                session.remove(responseFlowFile);
            }
        }
    }

    private Request configureRequest(final ProcessContext context, final ProcessSession session, final FlowFile requestFlowFile, String url) {
        final Request.Builder requestBuilder = new Request.Builder();

        requestBuilder.url(url);
        final String authUser = trimToEmpty(context.getProperty(REQUEST_USERNAME).getValue());

        // If the username/password properties are set then check if digest auth is being used
        if ("false".equalsIgnoreCase(context.getProperty(REQUEST_DIGEST_AUTHENTICATION_ENABLED).getValue())) {
            if (!authUser.isEmpty()) {
                final String authPass = trimToEmpty(context.getProperty(REQUEST_PASSWORD).getValue());

                String credential = Credentials.basic(authUser, authPass);
                requestBuilder.header(HttpHeader.AUTHORIZATION.getHeader(), credential);
            } else {
                oauth2AccessTokenProviderOptional.ifPresent(oauth2AccessTokenProvider ->
                    requestBuilder.addHeader(HttpHeader.AUTHORIZATION.getHeader(), "Bearer " + oauth2AccessTokenProvider.getAccessDetails().getAccessToken())
                );
            }
        }

        final ContentEncodingStrategy contentEncodingStrategy =
                context.getProperty(REQUEST_CONTENT_ENCODING).asAllowableValue(ContentEncodingStrategy.class);
        if (ContentEncodingStrategy.GZIP == contentEncodingStrategy) {
            requestBuilder.addHeader(HttpHeader.CONTENT_ENCODING.getHeader(), ContentEncodingStrategy.GZIP.getValue().toLowerCase());
        }

        final String method = getRequestMethod(context, requestFlowFile);
        final Optional<HttpMethod> httpMethodFound = findRequestMethod(method);

        final RequestBody requestBody;
        if (httpMethodFound.isPresent()) {
            final HttpMethod httpMethod = httpMethodFound.get();
            if (httpMethod.isRequestBodySupported()) {
                requestBody = getRequestBodyToSend(session, context, requestFlowFile, contentEncodingStrategy);
            } else {
                requestBody = null;
            }
        } else {
            requestBody = null;
        }
        requestBuilder.method(method, requestBody);

        setHeaderProperties(context, requestBuilder, requestFlowFile);
        return requestBuilder.build();
    }

    private RequestBody getRequestBodyToSend(final ProcessSession session, final ProcessContext context,
                                             final FlowFile requestFlowFile,
                                             final ContentEncodingStrategy contentEncodingStrategy
    ) {
        boolean requestBodyEnabled = context.getProperty(REQUEST_BODY_ENABLED).asBoolean();

        String evalContentType = context.getProperty(REQUEST_CONTENT_TYPE)
                .evaluateAttributeExpressions(requestFlowFile).getValue();
        final String contentType = StringUtils.isBlank(evalContentType) ? DEFAULT_CONTENT_TYPE : evalContentType;
        String formDataName = context.getProperty(REQUEST_FORM_DATA_NAME).evaluateAttributeExpressions(requestFlowFile).getValue();

        // Check for dynamic properties for form components.
        // Even if the flowfile is not sent, we may still send form parameters.
        Map<String, PropertyDescriptor> propertyDescriptors = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            Matcher matcher = FORM_DATA_NAME_PARAMETER_PATTERN.matcher(entry.getKey().getName());
            if (matcher.matches()) {
                propertyDescriptors.put(matcher.group(FORM_DATA_NAME_GROUP), entry.getKey());
            }
        }

        final boolean contentLengthUnknown = chunkedTransferEncoding || ContentEncodingStrategy.GZIP == contentEncodingStrategy;
        RequestBody requestBody = new RequestBody() {
            @Nullable
            @Override
            public MediaType contentType() {
                return MediaType.parse(contentType);
            }

            @Override
            public void writeTo(final BufferedSink sink) throws IOException {
                final BufferedSink outputSink = (ContentEncodingStrategy.GZIP == contentEncodingStrategy)
                        ? Okio.buffer(new GzipSink(sink))
                        : sink;

                session.read(requestFlowFile, inputStream -> {
                    final Source source = Okio.source(inputStream);
                    outputSink.writeAll(source);
                });

                // Close Output Sink for gzip to write trailing bytes
                if (ContentEncodingStrategy.GZIP == contentEncodingStrategy) {
                    outputSink.close();
                }
            }

            @Override
            public long contentLength() {
                return contentLengthUnknown ? -1 : requestFlowFile.getSize();
            }
        };

        if (!propertyDescriptors.isEmpty() || StringUtils.isNotEmpty(formDataName)) {
            // we have form data
            MultipartBody.Builder builder = new Builder().setType(MultipartBody.FORM);
            boolean useFileName = context.getProperty(REQUEST_FORM_DATA_FILENAME_ENABLED).asBoolean();
            String contentFileName = null;
            if (useFileName) {
                contentFileName = requestFlowFile.getAttribute(CoreAttributes.FILENAME.key());
            }
            // loop through the dynamic form parameters
            for (final Map.Entry<String, PropertyDescriptor> entry : propertyDescriptors.entrySet()) {
                final String propValue = context.getProperty(entry.getValue().getName())
                        .evaluateAttributeExpressions(requestFlowFile).getValue();
                builder.addFormDataPart(entry.getKey(), propValue);
            }
            if (requestBodyEnabled) {
                builder.addFormDataPart(formDataName, contentFileName, requestBody);
            }
            return builder.build();
        } else if (requestBodyEnabled) {
            return requestBody;
        }
        return RequestBody.create(new byte[0], null);
    }

    private void setHeaderProperties(final ProcessContext context, final Request.Builder requestBuilder, final FlowFile requestFlowFile) {
        final String userAgent = trimToEmpty(context.getProperty(REQUEST_USER_AGENT).evaluateAttributeExpressions(requestFlowFile).getValue());
        requestBuilder.addHeader(HttpHeader.USER_AGENT.getHeader(), userAgent);

        if (context.getProperty(REQUEST_DATE_HEADER_ENABLED).asBoolean()) {
            final ZonedDateTime universalCoordinatedTimeNow = ZonedDateTime.now(ZoneOffset.UTC);
            requestBuilder.addHeader(HttpHeader.DATE.getHeader(), RFC_2616_DATE_TIME.format(universalCoordinatedTimeNow));
        }

        for (String headerKey : dynamicPropertyNames) {
            String headerValue = context.getProperty(headerKey).evaluateAttributeExpressions(requestFlowFile).getValue();

            // ignore form data name dynamic properties
            if (FORM_DATA_NAME_PARAMETER_PATTERN.matcher(headerKey).matches()) {
                continue;
            }

            requestBuilder.addHeader(headerKey, headerValue);
        }

        // iterate through the flowfile attributes, adding any attribute that
        // matches the attributes-to-send pattern. if the pattern is not set
        // (it's an optional property), ignore that attribute entirely
        if (requestHeaderAttributesPattern != null && requestFlowFile != null) {
            Map<String, String> attributes = requestFlowFile.getAttributes();
            Matcher m = requestHeaderAttributesPattern.matcher("");
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                String headerKey = trimToEmpty(entry.getKey());
                if (IGNORED_REQUEST_ATTRIBUTES.contains(headerKey)) {
                    continue;
                }

                // check if our attribute key matches the pattern
                // if so, include in the request as a header
                m.reset(headerKey);
                if (m.matches()) {
                    String headerVal = trimToEmpty(entry.getValue());
                    requestBuilder.addHeader(headerKey, headerVal);
                }
            }
        }
    }

    private void route(FlowFile request, FlowFile response, ProcessSession session, ProcessContext context, int statusCode) {
        // check if we should yield the processor
        if (!isSuccess(statusCode) && request == null) {
            context.yield();
        }

        // If the property to output the response flowfile regardless of status code is set then transfer it
        boolean responseSent = false;
        if (context.getProperty(RESPONSE_GENERATION_REQUIRED).asBoolean()) {
            session.transfer(response, RESPONSE);
            responseSent = true;
        }

        // transfer to the correct relationship
        // 2xx -> SUCCESS
        if (isSuccess(statusCode)) {
            // we have two flowfiles to transfer
            if (request != null) {
                session.transfer(request, ORIGINAL);
            }
            if (response != null && !responseSent) {
                session.transfer(response, RESPONSE);
            }

            // 5xx -> RETRY
        } else if (statusCode / 100 == 5) {
            if (request != null) {
                request = session.penalize(request);
                session.transfer(request, RETRY);
            }

            // 1xx, 3xx, 4xx -> NO RETRY
        } else {
            if (request != null) {
                if (context.getProperty(REQUEST_FAILURE_PENALIZATION_ENABLED).asBoolean()) {
                    request = session.penalize(request);
                }
                session.transfer(request, NO_RETRY);
            }
        }

    }

    private boolean isSuccess(int statusCode) {
        return statusCode / 100 == 2;
    }

    private void logRequest(ComponentLog logger, Request request) {
        if (logger.isDebugEnabled()) {
            logger.debug("\nRequest to remote service:\n\t{}\n{}",
                    request.url().url().toExternalForm(), getLogString(request.headers().toMultimap()));
        }
    }

    private void logResponse(ComponentLog logger, String url, Response response) {
        if (logger.isDebugEnabled()) {
            logger.debug("\nResponse from remote service:\n\t{}\n{}",
                    url, getLogString(response.headers().toMultimap()));
        }
    }

    private String getLogString(Map<String, List<String>> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            List<String> list = entry.getValue();
            if (!list.isEmpty()) {
                sb.append("\t");
                sb.append(entry.getKey());
                sb.append(": ");
                if (list.size() == 1) {
                    sb.append(list.getFirst());
                } else {
                    sb.append(list);
                }
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    /**
     * Returns a Map of flowfile attributes from the response http headers. Multivalue headers are naively converted to comma separated strings.
     * Prefix is passed in to allow differentiation for these new attributes.
     */
    private Map<String, String> convertAttributesFromHeaders(final Response responseHttp, final String prefix) {
        // create a new hashmap to store the values from the connection
        final Map<String, String> attributes = new HashMap<>();
        final String trimmedPrefix = trimToEmpty(prefix);
        final Headers headers = responseHttp.headers();
        headers.names().forEach((key) -> {
            final List<String> values = headers.values(key);
            // we ignore any headers with no actual values (rare)
            if (!values.isEmpty()) {
                // create a comma separated string from the values, this is stored in the map
                final String value = StringUtils.join(values, MULTIPLE_HEADER_DELIMITER);
                attributes.put(trimmedPrefix + key, value);
            }
        });

        final Handshake handshake = responseHttp.handshake();
        if (handshake != null) {
            final Principal principal = handshake.peerPrincipal();
            if (principal != null) {
                attributes.put(REMOTE_DN, principal.getName());
            }
        }

        return attributes;
    }

    private Charset getCharsetFromMediaType(MediaType contentType) {
        return contentType != null ? contentType.charset(StandardCharsets.UTF_8) : StandardCharsets.UTF_8;
    }

    private static File getResponseCacheDirectory() throws IOException {
        return Files.createTempDirectory(InvokeHTTP.class.getSimpleName()).toFile();
    }

    private FlowFileNamingStrategy getFlowFileNamingStrategy(final ProcessContext context) {
        final String strategy = context.getProperty(RESPONSE_FLOW_FILE_NAMING_STRATEGY).getValue();
        return FlowFileNamingStrategy.valueOf(strategy);
    }

    private String getFileNameFromUrl(URL url) {
        String fileName = null;
        String path = StringUtils.removeEnd(url.getPath(), "/");

        if (!StringUtils.isEmpty(path)) {
            fileName = path.substring(path.lastIndexOf('/') + 1);
        }

        return fileName;
    }

    private Optional<HttpMethod> findRequestMethod(String method) {
        return Arrays.stream(HttpMethod.values())
                .filter(httpMethod -> httpMethod.name().equals(method))
                .findFirst();
    }

    private String getRequestMethod(final PropertyContext context, final FlowFile flowFile) {
        final String method = context.getProperty(HTTP_METHOD).evaluateAttributeExpressions(flowFile).getValue().toUpperCase();
        return trimToEmpty(method);
    }
}
