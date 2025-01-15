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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.slack.api.app_backend.events.payload.EventsApiPayload;
import com.slack.api.bolt.App;
import com.slack.api.bolt.AppConfig;
import com.slack.api.bolt.context.builtin.EventContext;
import com.slack.api.bolt.context.builtin.SlashCommandContext;
import com.slack.api.bolt.request.builtin.SlashCommandRequest;
import com.slack.api.bolt.response.Response;
import com.slack.api.bolt.socket_mode.SocketModeApp;
import com.slack.api.model.User;
import com.slack.api.model.event.AppMentionEvent;
import com.slack.api.model.event.FileSharedEvent;
import com.slack.api.model.event.MemberJoinedChannelEvent;
import com.slack.api.model.event.MessageChannelJoinEvent;
import com.slack.api.model.event.MessageEvent;
import com.slack.api.model.event.MessageFileShareEvent;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.slack.consume.UserDetailsLookup;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.function.Supplier;
import java.util.regex.Pattern;


@PrimaryNodeOnly
@DefaultSettings(yieldDuration = "250 millis")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Set to application/json, as the output will always be in JSON format")
})
@SeeAlso({ConsumeSlack.class})
@Tags({"slack", "real-time", "event", "message", "command", "listen", "receive", "social media", "team", "text", "unstructured"})
@CapabilityDescription("Retrieves real-time messages or Slack commands from one or more Slack conversations. The messages are written out in JSON format. " +
    "Note that this Processor should be used to obtain real-time messages and commands from Slack and does not provide a mechanism for obtaining historical messages. " +
    "The ConsumeSlack Processor should be used for an initial load of messages from a channel. " +
    "See Usage / Additional Details for more information about how to configure this Processor and enable it to retrieve messages and commands from Slack.")
public class ListenSlack extends AbstractProcessor {

    private static final ObjectMapper OBJECT_MAPPER;
    static {
        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
    }

    static final AllowableValue RECEIVE_MESSAGE_EVENTS = new AllowableValue("Receive Message Events", "Receive Message Events",
        "The Processor is to receive Slack Message Events");
    static final AllowableValue RECEIVE_MENTION_EVENTS = new AllowableValue("Receive App Mention Events", "Receive App Mention Events",
        "The Processor is to receive only slack messages that mention the bot user (App Mention Events)");
    static final AllowableValue RECEIVE_COMMANDS = new AllowableValue("Receive Commands", "Receive Commands",
        "The Processor is to receive Commands from Slack that are specific to your application. The Processor will not receive Message Events.");
    static final AllowableValue RECEIVE_JOINED_CHANNEL_EVENTS = new AllowableValue("Receive Joined Channel Events", "Receive Joined Channel Events",
        "The Processor is to receive only events when a member is joining a channel. The Processor will not receive Message Events.");


    static PropertyDescriptor APP_TOKEN = new PropertyDescriptor.Builder()
        .name("App Token")
        .description("The Application Token that is registered to your Slack application")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(true)
        .sensitive(true)
        .build();

    static PropertyDescriptor BOT_TOKEN = new PropertyDescriptor.Builder()
        .name("Bot Token")
        .description("The Bot Token that is registered to your Slack application")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(true)
        .sensitive(true)
        .build();

    static final PropertyDescriptor EVENT_TYPE = new PropertyDescriptor.Builder()
        .name("Event Type to Receive")
        .description("Specifies the type of Event that the Processor should respond to")
        .required(true)
        .defaultValue(RECEIVE_MENTION_EVENTS.getValue())
        .allowableValues(RECEIVE_MENTION_EVENTS, RECEIVE_MESSAGE_EVENTS, RECEIVE_COMMANDS, RECEIVE_JOINED_CHANNEL_EVENTS)
        .build();

    static final PropertyDescriptor RESOLVE_USER_DETAILS = new PropertyDescriptor.Builder()
        .name("Resolve User Details")
        .description("Specifies whether the Processor should lookup details about the Slack User who sent the received message. " +
            "If true, the output JSON will contain an additional field named 'userDetails'. " +
            "The 'user' field will still contain the ID of the user. In order to enable this capability, the Bot Token must be granted the 'users:read' " +
            "and optionally the 'users.profile:read' Bot Token Scope. " +
            "If the rate limit is exceeded when retrieving this information, the received message will be rejected and must be re-delivered.")
        .required(true)
        .defaultValue("false")
        .allowableValues("true", "false")
        .dependsOn(EVENT_TYPE, RECEIVE_MESSAGE_EVENTS, RECEIVE_MENTION_EVENTS, RECEIVE_JOINED_CHANNEL_EVENTS)
        .build();

    static Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles that are created will be sent to this Relationship.")
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            APP_TOKEN,
            BOT_TOKEN,
            EVENT_TYPE,
            RESOLVE_USER_DETAILS
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private final TransferQueue<EventWrapper> eventTransferQueue = new LinkedTransferQueue<>();
    private volatile SocketModeApp socketModeApp;
    private volatile UserDetailsLookup userDetailsLookup;


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }


    @OnScheduled
    public void establishWebsocketEndpoint(final ProcessContext context) throws Exception {
        final String appToken = context.getProperty(APP_TOKEN).getValue();
        final String botToken = context.getProperty(BOT_TOKEN).getValue();

        final AppConfig appConfig = AppConfig.builder()
            .singleTeamBotToken(botToken)
            .build();

        final App slackApp = new App(appConfig);

        // Register the event types that make sense
        if (context.getProperty(EVENT_TYPE).getValue().equals(RECEIVE_MESSAGE_EVENTS.getValue())) {
            slackApp.event(MessageEvent.class, this::handleEvent);
            slackApp.event(MessageFileShareEvent.class, this::handleEvent);
            slackApp.event(FileSharedEvent.class, this::handleEvent);
        } else if (context.getProperty(EVENT_TYPE).getValue().equals(RECEIVE_MENTION_EVENTS.getValue())) {
            slackApp.event(AppMentionEvent.class, this::handleEvent);
            // When there's an AppMention, we'll also get a MessageEvent. We need to handle this event, or we'll get warnings in the logs
            // that no Event Handler is registered, and it will respond back to Slack with a 404. To avoid this, we just acknowledge the event.
            slackApp.event(MessageEvent.class, (payload, ctx) -> ctx.ack());
        } else if (context.getProperty(EVENT_TYPE).getValue().equals(RECEIVE_JOINED_CHANNEL_EVENTS.getValue())) {
            slackApp.event(MemberJoinedChannelEvent.class, this::handleEvent);
            slackApp.event(MessageChannelJoinEvent.class, this::handleEvent);
            // When there's an MemberJoinedChannel event, we'll also get a MessageEvent. We need to handle this event, or we'll get warnings in the logs
            // that no Event Handler is registered, and it will respond back to Slack with a 404. To avoid this, we just acknowledge the event.
            slackApp.event(MessageEvent.class, (payload, ctx) -> ctx.ack());
        } else {
            slackApp.command(Pattern.compile(".*"), this::handleCommand);
        }

        userDetailsLookup = new UserDetailsLookup(userId -> slackApp.client().usersInfo(r -> r.user(userId)), getLogger());

        socketModeApp = new SocketModeApp(appToken, slackApp);
        socketModeApp.startAsync();
    }

    private Response handleEvent(final EventsApiPayload<?> eventsApiPayload, final EventContext context) {
        return handleNotification(eventsApiPayload.getEvent(), context::ack);
    }

    private Response handleCommand(final SlashCommandRequest request, final SlashCommandContext context) {
        return handleNotification(request.getPayload(), context::ack);
    }

    private Response handleNotification(final Object notification, final Supplier<Response> ackSupplier) {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        eventTransferQueue.add(new EventWrapper(notification, countDownLatch));

        try {
            final boolean acknowledged = countDownLatch.await(5, TimeUnit.SECONDS);
            return acknowledged ? ackSupplier.get() : Response.error(503);
        } catch (final InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting for event to be processed", e);
        }
    }

    @OnStopped
    public void onStopped() throws Exception {
        socketModeApp.stop();
        socketModeApp.close();
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final EventWrapper eventWrapper;
        try {
            eventWrapper = eventTransferQueue.poll(1, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        if (eventWrapper == null) {
            context.yield();
            return;
        }

        final Object messageEvent = eventWrapper.getEvent();

        FlowFile flowFile = session.create();
        try (final OutputStream out = session.write(flowFile);
             final JsonGenerator generator = OBJECT_MAPPER.createGenerator(out)) {

            // If we need to resolve user details, we need a way to inject it into the JSON. Since we have an object model at this point,
            // we serialize it to a string, then deserialize it back into a JsonNode, and then inject it into the JSON.
            if (context.getProperty(RESOLVE_USER_DETAILS).asBoolean()) {
                final String stringRepresentation = OBJECT_MAPPER.writeValueAsString(messageEvent);
                final JsonNode jsonNode = OBJECT_MAPPER.readTree(stringRepresentation);
                if (jsonNode.hasNonNull("user")) {
                    final String userId = jsonNode.get("user").asText();
                    final User userDetails = userDetailsLookup.getUserDetails(userId);
                    if (userDetails != null) {
                        final ObjectNode objectNode = (ObjectNode) jsonNode;
                        final String userDetailsJson = OBJECT_MAPPER.writeValueAsString(userDetails);
                        final JsonNode userDetailsNode = OBJECT_MAPPER.readTree(userDetailsJson);
                        objectNode.set("userDetails", userDetailsNode);
                    }
                }

                generator.writeTree(jsonNode);
            } else {
                generator.writeObject(messageEvent);
            }
        } catch (final IOException e) {
            getLogger().error("Failed to write out Slack message", e);
            session.remove(flowFile);
            return;
        }

        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.getProvenanceReporter().receive(flowFile, socketModeApp.getClient().getWssUri().toString());
        session.transfer(flowFile, REL_SUCCESS);

        // Commit the session asynchronously and upon success allow the message to be acknowledged.
        session.commitAsync(() -> eventWrapper.getCountDownLatch().countDown());
    }


    private static class EventWrapper {
        private final Object event;
        private final CountDownLatch countDownLatch;

        public EventWrapper(final Object event, final CountDownLatch countDownLatch) {
            this.event = event;
            this.countDownLatch = countDownLatch;
        }

        public Object getEvent() {
            return event;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }
    }
}
