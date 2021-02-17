package org.habbcode.nifi.tibcojms.processors;
/*
 *   Alexandr Mikhaylov created on 16.02.2021 inside the package - org.habbcode.nifi.tibcojms.processors
 */

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
//import org.apache.nifi.jms.cf.JMSConnectionFactoryProvider;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
//import org.habbcode.nifi.tibcojms.cf.JMSConnectionFactoryProvider;
import org.habbcode.nifi.tibcojms.cf.JMSConnectionFactoryProvider;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.JmsHeaders;

import javax.jms.Destination;
import javax.jms.Message;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * An implementation of JMS Message publishing {@link Processor} which upon each
 * invocation of {@link #onTrigger(ProcessContext, ProcessSession)} method will
 * construct a {@link Message} from the contents of the {@link FlowFile} sending
 * it to the {@link Destination} identified by the
 * {@link AbstractJMSProcessor#DESTINATION} property while transferring the
 * incoming {@link FlowFile} to 'success' {@link Relationship}. If message can
 * not be constructed and/or sent the incoming {@link FlowFile} will be
 * transitioned to 'failure' {@link Relationship}
 */
@Tags({ "jms", "put", "message", "send", "publish" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Creates a JMS Message from the contents of a FlowFile and sends it to a "
        + "JMS Destination (queue or topic) as JMS BytesMessage or TextMessage. "
        + "FlowFile attributes will be added as JMS headers and/or properties to the outgoing JMS message.")
@ReadsAttributes({
        @ReadsAttribute(attribute = JmsHeaders.DELIVERY_MODE, description = "This attribute becomes the JMSDeliveryMode message header. Must be an integer."),
        @ReadsAttribute(attribute = JmsHeaders.EXPIRATION, description = "This attribute becomes the JMSExpiration message header. Must be an integer."),
        @ReadsAttribute(attribute = JmsHeaders.PRIORITY, description = "This attribute becomes the JMSPriority message header. Must be an integer."),
        @ReadsAttribute(attribute = JmsHeaders.REDELIVERED, description = "This attribute becomes the JMSRedelivered message header."),
        @ReadsAttribute(attribute = JmsHeaders.TIMESTAMP, description = "This attribute becomes the JMSTimestamp message header. Must be a long."),
        @ReadsAttribute(attribute = JmsHeaders.CORRELATION_ID, description = "This attribute becomes the JMSCorrelationID message header."),
        @ReadsAttribute(attribute = JmsHeaders.TYPE, description = "This attribute becomes the JMSType message header. Must be an integer."),
        @ReadsAttribute(attribute = JmsHeaders.REPLY_TO, description = "This attribute becomes the JMSReplyTo message header. Must be an integer."),
        @ReadsAttribute(attribute = JmsHeaders.DESTINATION, description = "This attribute becomes the JMSDestination message header. Must be an integer."),
        @ReadsAttribute(attribute = "other attributes", description = "All other attributes that do not start with " + JmsHeaders.PREFIX + " are added as message properties."),
        @ReadsAttribute(attribute = "other attributes .type", description = "When an attribute will be added as a message property, a second attribute of the same name but with an extra"
                + " `.type` at the end will cause the message property to be sent using that strong type. For example, attribute `delay` with value `12000` and another attribute"
                + " `delay.type` with value `integer` will cause a JMS message property `delay` to be sent as an Integer rather than a String. Supported types are boolean, byte,"
                + " short, integer, long, float, double, and string (which is the default).")
})
@DynamicProperty(name = "The name of a Connection Factory configuration property.", value = "The value of a given Connection Factory configuration property.",
        description = "Additional configuration property for the Connection Factory. It can be used when the Connection Factory is being configured via the 'JNDI *' or the 'JMS *'" +
                "properties of the processor. For more information, see the Additional Details page.",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
@SeeAlso(value = { ConsumeJMS.class, JMSConnectionFactoryProvider.class })
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PublishJMS extends AbstractJMSProcessor<JMSPublisher> {

    static final PropertyDescriptor MESSAGE_BODY = new PropertyDescriptor.Builder()
            .name("message-body-type")
            .displayName("Message Body Type")
            .description("The type of JMS message body to construct.")
            .required(true)
            .defaultValue(BYTES_MESSAGE)
            .allowableValues(BYTES_MESSAGE, TEXT_MESSAGE)
            .build();
    static final PropertyDescriptor ALLOW_ILLEGAL_HEADER_CHARS = new PropertyDescriptor.Builder()
            .name("allow-illegal-chars-in-jms-header-names")
            .displayName("Allow Illegal Characters in Header Names")
            .description("Specifies whether illegal characters in header names should be sent to the JMS broker. " +
                    "Usually hyphens and full-stops.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor ATTRIBUTES_AS_HEADERS_REGEX = new PropertyDescriptor.Builder()
            .name("attributes-to-send-as-jms-headers-regex")
            .displayName("Attributes to Send as JMS Headers (Regex)")
            .description("Specifies the Regular Expression that determines the names of FlowFile attributes that" +
                    " should be sent as JMS Headers")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .defaultValue(".*")
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are sent to the JMS destination are routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be sent to JMS destination are routed to this relationship")
            .build();

    private static final List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is build only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();

        _propertyDescriptors.add(CF_SERVICE);
        _propertyDescriptors.add(DESTINATION);
        _propertyDescriptors.add(DESTINATION_TYPE);
        _propertyDescriptors.add(USER);
        _propertyDescriptors.add(PASSWORD);
        _propertyDescriptors.add(CLIENT_ID);
        _propertyDescriptors.add(SESSION_CACHE_SIZE);

        _propertyDescriptors.add(MESSAGE_BODY);
        _propertyDescriptors.add(CHARSET);
        _propertyDescriptors.add(ALLOW_ILLEGAL_HEADER_CHARS);
        _propertyDescriptors.add(ATTRIBUTES_AS_HEADERS_REGEX);

        _propertyDescriptors.addAll(JNDI_JMS_CF_PROPERTIES);
        _propertyDescriptors.addAll(JMS_CF_PROPERTIES);

        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);


        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    /**
     * Will construct JMS {@link Message} by extracting its body from the
     * incoming {@link FlowFile}. {@link FlowFile} attributes that represent
     * standard JMS headers will be extracted from the {@link FlowFile} and set
     * as JMS headers on the newly constructed message. For the list of
     * available message headers please see {@link JmsHeaders}. <br>
     * <br>
     * Upon success the incoming {@link FlowFile} is transferred to the'success'
     * {@link Relationship} and upon failure FlowFile is penalized and
     * transferred to the 'failure' {@link Relationship}
     */
    @Override
    protected void rendezvousWithJms(ProcessContext context, ProcessSession processSession, JMSPublisher publisher) throws ProcessException {
        FlowFile flowFile = processSession.get();
        if (flowFile != null) {
            try {
                String destinationName = context.getProperty(DESTINATION).evaluateAttributeExpressions(flowFile).getValue();
                String charset = context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue();
                Boolean allowIllegalChars = context.getProperty(ALLOW_ILLEGAL_HEADER_CHARS).asBoolean();
                String attributeHeaderRegex = context.getProperty(ATTRIBUTES_AS_HEADERS_REGEX).getValue();

                Map<String,String> attributesToSend = new HashMap<>();
                // REGEX Attributes
                final Pattern pattern = Pattern.compile(attributeHeaderRegex);
                for (final Map.Entry<String, String> entry : flowFile.getAttributes().entrySet()) {
                    final String key = entry.getKey();
                    if (pattern.matcher(key).matches()) {
                        if (allowIllegalChars || key.endsWith(".type") || (!key.contains("-") && !key.contains("."))) {
                            attributesToSend.put(key, flowFile.getAttribute(key));
                        }
                    }
                }

                switch (context.getProperty(MESSAGE_BODY).getValue()) {
                    case TEXT_MESSAGE:
                        try {
                            publisher.publish(destinationName, this.extractTextMessageBody(flowFile, processSession, charset), attributesToSend);
                        } catch(Exception e) {
                            publisher.setValid(false);
                            throw e;
                        }
                        break;
                    case BYTES_MESSAGE:
                    default:
                        try {
                            publisher.publish(destinationName, this.extractMessageBody(flowFile, processSession), attributesToSend);
                        } catch(Exception e) {
                            publisher.setValid(false);
                            throw e;
                        }
                        break;
                }
                processSession.transfer(flowFile, REL_SUCCESS);
                processSession.getProvenanceReporter().send(flowFile, destinationName);
            } catch (Exception e) {
                processSession.transfer(flowFile, REL_FAILURE);
                this.getLogger().error("Failed while sending message to JMS via " + publisher, e);
                context.yield();
                publisher.setValid(false);
            }
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    /**
     *
     */
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     * Will create an instance of {@link JMSPublisher}
     */
    @Override
    protected JMSPublisher finishBuildingJmsWorker(CachingConnectionFactory connectionFactory, JmsTemplate jmsTemplate, ProcessContext processContext) {
        return new JMSPublisher(connectionFactory, jmsTemplate, this.getLogger());
    }

    /**
     * Extracts contents of the {@link FlowFile} as byte array.
     */
    private byte[] extractMessageBody(FlowFile flowFile, ProcessSession session) {
        final byte[] messageContent = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, messageContent, true));
        return messageContent;
    }

    private String extractTextMessageBody(FlowFile flowFile, ProcessSession session, String charset) {
        final StringWriter writer = new StringWriter();
        session.read(flowFile, in -> IOUtils.copy(in, writer, Charset.forName(charset)));
        return writer.toString();
    }
}