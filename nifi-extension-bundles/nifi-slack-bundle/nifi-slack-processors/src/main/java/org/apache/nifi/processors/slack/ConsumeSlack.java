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

package org.apache.nifi.processors.slack;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.api.bolt.App;
import com.slack.api.bolt.AppConfig;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.request.conversations.ConversationsHistoryRequest;
import com.slack.api.methods.request.conversations.ConversationsInfoRequest;
import com.slack.api.methods.request.conversations.ConversationsListRequest;
import com.slack.api.methods.request.conversations.ConversationsRepliesRequest;
import com.slack.api.methods.request.users.UsersInfoRequest;
import com.slack.api.methods.response.conversations.ConversationsHistoryResponse;
import com.slack.api.methods.response.conversations.ConversationsInfoResponse;
import com.slack.api.methods.response.conversations.ConversationsListResponse;
import com.slack.api.methods.response.conversations.ConversationsRepliesResponse;
import com.slack.api.methods.response.users.UsersInfoResponse;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.slack.consume.ConsumeChannel;
import org.apache.nifi.processors.slack.consume.ConsumeSlackClient;
import org.apache.nifi.processors.slack.consume.UsernameLookup;
import org.apache.nifi.processors.slack.util.RateLimit;
import org.apache.nifi.processors.slack.util.SlackResponseUtil;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Stateful(scopes = Scope.CLUSTER, description = "Maintains a mapping of Slack Channel IDs to the timestamp of the last message that was retrieved for that channel. " +
    "This allows the processor to only retrieve messages that have been posted since the last time the processor was run. " +
    "This state is stored in the cluster so that if the Primary Node changes, the new node will pick up where the previous node left off.")
@WritesAttributes({
    @WritesAttribute(attribute = "slack.channel.id", description = "The ID of the Slack Channel from which the messages were retrieved"),
    @WritesAttribute(attribute = "slack.message.count", description = "The number of slack messages that are included in the FlowFile"),
    @WritesAttribute(attribute = "mime.type", description = "Set to application/json, as the output will always be in JSON format")
})
@SeeAlso({ListenSlack.class})
@Tags({"slack", "conversation", "conversation.history", "social media", "team", "text", "unstructured"})
@CapabilityDescription("Retrieves messages from one or more configured Slack channels. The messages are written out in JSON format. " +
    "See Usage / Additional Details for more information about how to configure this Processor and enable it to retrieve messages from Slack.")
@DefaultSettings(yieldDuration = "3 sec")
public class ConsumeSlack extends AbstractProcessor implements VerifiableProcessor {

    static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
        .name("Access Token")
        .description("OAuth Access Token used for authenticating/authorizing the Slack request sent by NiFi. This may be either a User Token or a Bot Token. " +
            "It must be granted the channels:history, groups:history, im:history, or mpim:history scope, depending on the type of conversation being used.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .sensitive(true)
        .build();

    static final PropertyDescriptor CHANNEL_IDS = new PropertyDescriptor.Builder()
        .name("Channels")
        .description("A comma-separated list of Slack Channels to Retrieve Messages From. Each element in the list may be either a Channel ID, such as C0L9VCD47, " +
            "or (for public channels only) the name of a channel, prefixed with a # sign, such as #general. If any channel name is provided instead," +
            "instead of an ID, the Access Token provided must be granted the channels:read scope in order to resolve the Channel ID. See the Processor's " +
            "Additional Details for information on how to find a Channel ID.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor REPLY_MONITOR_WINDOW = new PropertyDescriptor.Builder()
        .name("Reply Monitor Window")
        .description("After consuming all messages in a given channel, this Processor will periodically poll all \"threaded messages\", aka Replies, whose timestamp " +
            "is between now and this amount of time in the past in order to check for any new replies. Setting this value to a larger value may result in " +
            "additional resource use and may result in Rate Limiting. However, if a user replies to an old thread that was started outside of this window, " +
            "the reply may not be captured.")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("7 days")
        .build();

    static final PropertyDescriptor REPLY_MONITOR_FREQUENCY = new PropertyDescriptor.Builder()
        .name("Reply Monitor Frequency")
        .description("After consuming all messages in a given channel, this Processor will periodically poll all \"threaded messages\", aka Replies, whose timestamp " +
            "falls between now and the amount of time specified by the <Reply Monitor Window> property. This property determines how frequently those messages are polled. " +
            "Setting the value to a shorter duration may result in replies to messages being captured more quickly, providing a lower latency. However, it will also result in " +
            "additional resource use and could trigger Rate Limiting to occur.")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("5 mins")
        .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Batch Size")
        .description("The maximum number of messages to retrieve in a single request to Slack. The entire response will be parsed into memory, " +
            "so it is important that this be kept in mind when setting this value.")
        .required(true)
        .addValidator(StandardValidators.createLongValidator(0, 1000, false))
        .defaultValue("100")
        .build();

    static final PropertyDescriptor RESOLVE_USERNAMES = new PropertyDescriptor.Builder()
        .name("Resolve Usernames")
        .description("Specifies whether or not User IDs should be resolved to usernames. By default, Slack Messages provide the ID of the user that sends a message, such as U0123456789, " +
            "but not the username, such as NiFiUser. The username may be resolved, but it may require additional calls to the Slack API and requires that the Token used be granted the " +
            "users:read scope. If set to true, usernames will be resolved with a best-effort policy: if a username cannot be obtained, it will be skipped over. Also, note that " +
            "when a username is obtained, the Message's <username> field is populated, and the <text> field is updated such that any mention will be output such as " +
            "\"Hi @user\" instead of \"Hi <@U1234567>\".")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("true")
        .build();

    static final PropertyDescriptor INCLUDE_MESSAGE_BLOCKS = new PropertyDescriptor.Builder()
        .name("Include Message Blocks")
        .description("Specifies whether or not the output JSON should include the value of the 'blocks' field for each Slack Message. This field includes information such as " +
            "individual parts of a message that are formatted using rich text. This may be useful, for instance, for parsing. However, it often accounts for a significant portion of " +
            "the data and as such may be set to null when it is not useful to you.")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();

    static final PropertyDescriptor INCLUDE_NULL_FIELDS = new PropertyDescriptor.Builder()
        .name("Include Null Fields")
        .description("Specifies whether or not fields that have null values should be included in the output JSON. If true, any field in a Slack Message that " +
            "has a null value will be included in the JSON with a value of null. If false, the key omitted from the output JSON entirely. Omitting null values results in " +
            "smaller messages that are generally more efficient to process, but including the values may provide a better understanding of the format, especially for " +
            "schema inference.")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("true")
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Slack messages that are successfully received will be routed to this relationship")
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CHANNEL_IDS,
            ACCESS_TOKEN,
            REPLY_MONITOR_WINDOW,
            REPLY_MONITOR_FREQUENCY,
            BATCH_SIZE,
            RESOLVE_USERNAMES,
            INCLUDE_MESSAGE_BLOCKS,
            INCLUDE_NULL_FIELDS);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private RateLimit rateLimit;
    private final Queue<ConsumeChannel> channels = new LinkedBlockingQueue<>();
    private volatile App slackApp;


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public boolean isStateful(final ProcessContext context) {
        return true;
    }

    @OnScheduled
    public void setup(final ProcessContext context) throws IOException, SlackApiException {
        rateLimit = new RateLimit(getLogger());
        slackApp = createSlackApp(context);

        final List<ConsumeChannel> consumeChannels = createChannels(context, slackApp);
        this.channels.addAll(consumeChannels);
    }

    @OnStopped
    public void shutdown() {
        channels.clear();
        if (slackApp != null) {
            slackApp.stop();
            slackApp = null;
        }
        rateLimit = null;
    }


    public RateLimit getRateLimit() {
        return rateLimit;
    }


    private App createSlackApp(final ProcessContext context) {
        final String botToken = context.getProperty(ACCESS_TOKEN).getValue();
        final AppConfig appConfig = AppConfig.builder()
            .singleTeamBotToken(botToken)
            .build();

        return new App(appConfig);
    }

    private List<ConsumeChannel> createChannels(final ProcessContext context, final App slackApp) throws SlackApiException, IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        if (context.getProperty(INCLUDE_NULL_FIELDS).asBoolean()) {
            objectMapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
        } else {
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        }

        final ConsumeSlackClient client = initializeClient(slackApp);

        // Split channel ID's by commas and trim any white space
        final List<String> channels = new ArrayList<>();
        final String channelsValue = context.getProperty(CHANNEL_IDS).getValue();
        Arrays.stream(channelsValue.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .forEach(channels::add);

        Map<String, String> channelMapping = new HashMap<>();

        if (channelIdsProvidedOnly(channels)) {
            //resolve the channel names by the specified channel IDs
            for (String channelId : channels) {
                String channelName = client.fetchChannelName(channelId);
                getLogger().info("Resolved Channel ID {} to name {}", channelId, channelName);
                channelMapping.put(channelId, channelName);
            }
        } else {
            // Fetch all channel ID's to have a name/ID channel mapping
            Map<String, String> allChannelNameIdMapping = client.fetchChannelIds();

            for (final String channel : channels) {

                String channelName;
                String channelId;

                final String channelIdOrName = channel.replace("#", "");
                channelId = allChannelNameIdMapping.get(channelIdOrName);

                if (channelId != null) {
                    channelName = channelIdOrName;
                    getLogger().info("Resolved Channel {} to ID {}", channelName, channelId);
                } else {
                    channelId = channelIdOrName;
                    channelName = allChannelNameIdMapping
                            .keySet()
                            .stream()
                            .filter(entry -> channelIdOrName.equals(allChannelNameIdMapping.get(entry)))
                            .findFirst()
                            .orElse("");
                    getLogger().info("Resolved Channel ID {} to name {}", channelId, channelName);
                }

                channelMapping.put(channelId, channelName);
            }
        }

        // Create ConsumeChannel objects for each Channel ID
        final UsernameLookup usernameLookup = new UsernameLookup(client, getLogger());

        final List<ConsumeChannel> consumeChannels = new ArrayList<>();

        for (final Map.Entry<String, String> channel : channelMapping.entrySet()) {
            final ConsumeChannel consumeChannel = new ConsumeChannel.Builder()
                .channelId(channel.getKey())
                .channelName(channel.getValue())
                .batchSize(context.getProperty(BATCH_SIZE).asInteger())
                .client(client)
                .includeMessageBlocks(context.getProperty(INCLUDE_MESSAGE_BLOCKS).asBoolean())
                .logger(getLogger())
                .replyMonitorFrequency(context.getProperty(REPLY_MONITOR_FREQUENCY).asTimePeriod(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .replyMonitorWindow(context.getProperty(REPLY_MONITOR_WINDOW).asTimePeriod(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .resolveUsernames(context.getProperty(RESOLVE_USERNAMES).asBoolean())
                .successRelationship(REL_SUCCESS)
                .usernameLookup(usernameLookup)
                .objectMapper(objectMapper)
                .build();

            consumeChannels.add(consumeChannel);
        }

        return consumeChannels;
    }

    protected ConsumeSlackClient initializeClient(final App slackApp) {
        slackApp.start();
        return new DelegatingSlackClient(slackApp.client());
    }


    private ConsumeChannel getChannel() {
        final List<ConsumeChannel> yieldedChannels = new ArrayList<>();

        try {
            while (!channels.isEmpty()) {
                final ConsumeChannel channel = channels.poll();
                if (channel == null) {
                    return null;
                }

                if (channel.isYielded()) {
                    yieldedChannels.add(channel);
                    continue;
                }

                return channel;
            }
        } finally {
            channels.addAll(yieldedChannels);
        }

        return null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Check to see if we are currently in a backoff period due to Slack's Rate Limit
        if (rateLimit.isLimitReached()) {
            getLogger().debug("Will not consume from Slack because rate limit has been reached");
            context.yield();
            return;
        }

        final ConsumeChannel channel = getChannel();
        if (channel == null) {
            getLogger().debug("All Slack Channels are currently yielded; will yield context and return");
            context.yield();
            return;
        }

        try {
            channel.consume(context, session);
        } catch (final Exception e) {
            session.rollback();

            final String channelId = channel.getChannelId();
            yieldOnException(e, channelId, context);
        } finally {
            channels.offer(channel);
        }
    }

    private static boolean channelIdsProvidedOnly(List<String> channels) {
        return channels.stream().noneMatch(channelValue -> channelValue.contains("#"));
    }

    private void yieldOnException(final Throwable t, final String channelId, final ProcessContext context) {
        if (SlackResponseUtil.isRateLimited(t)) {
            getLogger().warn("Slack indicated that the Rate Limit has been exceeded when attempting to retrieve messages for channel {}", channelId);
        } else {
            getLogger().error("Failed to retrieve messages for channel {}", channelId, t);
        }

        final int retryAfterSeconds = SlackResponseUtil.getRetryAfterSeconds(t);
        rateLimit.retryAfter(Duration.ofSeconds(retryAfterSeconds));
        context.yield();
    }


    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        final App slackApp = createSlackApp(context);

        final List<ConsumeChannel> channels;
        try {
            channels = createChannels(context, slackApp);

            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Determine Channel IDs")
                .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                .build());
        } catch (final Exception e) {
            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Determine Channel IDs")
                .outcome(ConfigVerificationResult.Outcome.FAILED)
                .explanation(e.toString())
                .build());

            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Check authorizations")
                .outcome(ConfigVerificationResult.Outcome.SKIPPED)
                .explanation("Skipped because appropriate Channel IDs could not be determined")
                .build());

            return results;
        }

        for (final ConsumeChannel channel : channels) {
            results.add(channel.verify());
        }

        return results;
    }


    private static class DelegatingSlackClient implements ConsumeSlackClient {
        private final MethodsClient delegate;

        public DelegatingSlackClient(final MethodsClient delegate) {
            this.delegate = delegate;
        }

        @Override
        public ConversationsHistoryResponse fetchConversationsHistory(final ConversationsHistoryRequest request) throws SlackApiException, IOException {
            return delegate.conversationsHistory(request);
        }

        @Override
        public ConversationsRepliesResponse fetchConversationsReplies(final ConversationsRepliesRequest request) throws SlackApiException, IOException {
            return delegate.conversationsReplies(request);
        }

        @Override
        public UsersInfoResponse fetchUsername(final String userId) throws SlackApiException, IOException {
            final UsersInfoRequest uiRequest = UsersInfoRequest.builder()
                .user(userId)
                .build();

            return delegate.usersInfo(uiRequest);
        }

        @Override
        public Map<String, String> fetchChannelIds() throws SlackApiException, IOException {
            final Map<String, String> mapping = new HashMap<>();

            String cursor = null;
            while (true) {
                final ConversationsListRequest request = ConversationsListRequest.builder()
                    .cursor(cursor)
                    .limit(1000)
                    .build();

                final ConversationsListResponse response = delegate.conversationsList(request);
                if (response.isOk()) {
                    response.getChannels().forEach(channel -> mapping.put(channel.getName(), channel.getId()));
                    cursor = response.getResponseMetadata().getNextCursor();
                    if (StringUtils.isEmpty(cursor)) {
                        return mapping;
                    }

                    continue;
                }

                final String errorMessage = SlackResponseUtil.getErrorMessage(response.getError(), response.getNeeded(), response.getProvided(), response.getWarning());
                throw new RuntimeException("Failed to determine Channel IDs: " + errorMessage);
            }
        }

        @Override
        public String fetchChannelName(String channelId) throws SlackApiException, IOException {
            final ConversationsInfoRequest request = ConversationsInfoRequest.builder()
                    .channel(channelId)
                    .build();

            final ConversationsInfoResponse response = delegate.conversationsInfo(request);

            if (response.isOk()) {
                return response.getChannel().getName();
            } else {
                final String errorMessage = SlackResponseUtil.getErrorMessage(response.getError(), response.getNeeded(), response.getProvided(), response.getWarning());
                throw new RuntimeException(format("Failed to determine Channel name from ID [%s]: %s", channelId, errorMessage));
            }
        }
    }
}
