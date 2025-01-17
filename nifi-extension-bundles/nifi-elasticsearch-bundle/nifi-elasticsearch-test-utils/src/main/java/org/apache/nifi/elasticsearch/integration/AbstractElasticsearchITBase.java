/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.nifi.elasticsearch.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.nifi.util.TestRunner;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.http.auth.AuthScope.ANY;

public abstract class AbstractElasticsearchITBase {
    // default Elasticsearch version should (ideally) match that in the nifi-elasticsearch-bundle#pom.xml for the integration-tests profile
    protected static final DockerImageName IMAGE = DockerImageName
            .parse(System.getProperty("elasticsearch.docker.image", "docker.elastic.co/elasticsearch/elasticsearch:8.17.0"));
    protected static final String ELASTIC_USER_PASSWORD = System.getProperty("elasticsearch.elastic_user.password", RandomStringUtils.randomAlphanumeric(10, 20));
    private static final int PORT = 9200;
    protected static final ElasticsearchContainer ELASTICSEARCH_CONTAINER = new ElasticsearchContainer(IMAGE)
            .withPassword(ELASTIC_USER_PASSWORD)
            .withEnv("xpack.security.enabled", "true")
            // enable API Keys for integration-tests (6.x & 7.x don't enable SSL and therefore API Keys by default, so use a trial license and explicitly enable API Keys)
            .withEnv("xpack.license.self_generated.type", "trial")
            .withEnv("xpack.security.authc.api_key.enabled", "true")
            // use a "special address" to ensure the publish_host is in the bind_host list, otherwise the Sniffer won't work
            // https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-network.html#network-interface-values
            // TestContainers makes Elasticsearch available via localhost/127.0.0.1; Elasticsearch uses the IP Address in publish_host
            .withEnv("network.bind_host", "_local_,_site_")
            .withEnv("network.publish_host", "127.0.0.1")
            // pin the Elasticsearch port (typically 9200 but not guaranteed), also bind that to 9200 on the host so the network.publish_host is accessible
            .withEnv("http.port", String.valueOf(PORT))
            .withExposedPorts(PORT)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                    new HostConfig().withPortBindings(new PortBinding(Ports.Binding.bindPort(PORT), new ExposedPort(PORT)))
            ));
    protected static final String CLIENT_SERVICE_NAME = "Client Service";
    protected static final String INDEX = "messages";

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    protected static final boolean ENABLE_TEST_CONTAINERS = "true".equalsIgnoreCase(System.getProperty("elasticsearch.testcontainers.enabled"));
    protected static String elasticsearchHost;
    protected static void startTestcontainer() {
        if (ENABLE_TEST_CONTAINERS) {
            if (getElasticMajorVersion() == 6) {
                // disable system call filter check to allow Elasticsearch 6 to run on aarch64 machines (e.g. Mac M1/2)
                ELASTICSEARCH_CONTAINER.withEnv("bootstrap.system_call_filter", "false").start();
            } else {
                ELASTICSEARCH_CONTAINER.start();
            }
            elasticsearchHost = String.format("http://%s", ELASTICSEARCH_CONTAINER.getHttpHostAddress());
        } else {
            elasticsearchHost = System.getProperty("elasticsearch.endpoint", "http://localhost:9200");
        }
    }

    protected TestRunner runner;

    protected static String type;

    static RestClient testDataManagementClient;

    protected static void stopTestcontainer() {
        if (ENABLE_TEST_CONTAINERS) {
            ELASTICSEARCH_CONTAINER.stop();
        }
    }

    @BeforeAll
    static void beforeAll() throws IOException {
        startTestcontainer();
        type = getElasticMajorVersion() == 6 ? "_doc" : "";
        System.out.printf("%n%n%n%n%n%n%n%n%n%n%n%n%n%n%nTYPE: %s%nIMAGE: %s:%s%n%n%n%n%n%n%n%n%n%n%n%n%n%n%n%n",
                type, IMAGE.getRepository(), IMAGE.getVersionPart());

        setupTestData();
    }

    private static String[] getElasticVersion() {
        final String fullVersion = IMAGE.getVersionPart();
        final String[] parts = fullVersion.split("\\.");
        if (parts.length == 1) {
            throw new IllegalArgumentException("The elasticsearch version should have at least a major and minor version ex. 7.17");
        }

        return parts;
    }

    protected static int getElasticMajorVersion() {
        return Integer.parseInt(getElasticVersion()[0]);
    }

    protected static int getElasticMinorVersion() {
        return Integer.parseInt(getElasticVersion()[1]);
    }

    protected static void setupTestData() throws IOException {
        final int majorVersion = getElasticMajorVersion();
        final URL url = URI.create(elasticsearchHost).toURL();
        testDataManagementClient = RestClient
                .builder(new HttpHost(url.getHost(), url.getPort(), url.getProtocol()))
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    final UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("elastic", ELASTIC_USER_PASSWORD);
                    final BasicCredentialsProvider provider = new BasicCredentialsProvider();
                    provider.setCredentials(ANY, credentials);
                    httpClientBuilder.setDefaultCredentialsProvider(provider);
                    return httpClientBuilder;
                })
                .build();
        final String script = String.format("src/test/resources/setup-%s.script", majorVersion);

        final List<SetupAction> actions = readSetupActions(script);

        for (final SetupAction action : actions) {
            final String endpoint = String.format("%s/%s", elasticsearchHost, action.path);
            final Request request = new Request(action.verb, endpoint);
            final HttpEntity jsonBody = new NStringEntity(action.json, ContentType.APPLICATION_JSON);
            request.setEntity(jsonBody);

            try {
                testDataManagementClient.performRequest(request);
            } catch (final ResponseException re) {
                throw new IllegalStateException("Error performing action " + action, re);
            }
        }
    }

    protected static void tearDownTestData(final List<String> testIndices) {
        testIndices.forEach(AbstractElasticsearchITBase::deleteIndex);
    }

    protected static void deleteIndex(final String index) {
        final Request request = new Request("DELETE", String.format("%s/%s", elasticsearchHost, index));
        try {
            testDataManagementClient.performRequest(request);
        } catch (final IOException ioe) {
            throw new IllegalStateException("Error deleting index " + index, ioe);
        }
    }

    protected String prettyJson(final Object o) throws JsonProcessingException {
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(o);
    }

    private static List<SetupAction> readSetupActions(final String scriptPath) throws IOException {
        final List<SetupAction> actions = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(scriptPath))))) {
            String line = reader.readLine();
            while (line != null) {
                if (!line.trim().isEmpty() && !line.trim().startsWith("#")) {
                    final String verb = line.substring(0, line.indexOf(":"));
                    final String path = line.substring(verb.length() + 1, line.indexOf(":", verb.length() + 1));
                    final int loc = verb.length() + path.length() + 2;
                    final String json = line.substring(loc);

                    actions.add(new SetupAction(verb, path, json));
                }
                line = reader.readLine();
            }
        }

        return actions;
    }

    private static final class SetupAction {
        private final String verb;
        private final String path;
        private final String json;

        public SetupAction(final String verb, final String path, final String json) {
            this.verb = verb;
            this.path = path;
            this.json = json;
        }

        @Override
        public String toString() {
            return "SetupAction{" +
                    "verb='" + verb + '\'' +
                    ", path='" + path + '\'' +
                    '}';
        }
    }
}
