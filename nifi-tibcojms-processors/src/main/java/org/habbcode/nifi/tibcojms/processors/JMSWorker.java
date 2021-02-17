package org.habbcode.nifi.tibcojms.processors;
/*
 *   Alexandr Mikhaylov created on 16.02.2021 inside the package - org.habbcode.nifi.tibcojms.processors
 */

import java.nio.channels.Channel;

import javax.jms.Connection;

import org.apache.nifi.logging.ComponentLog;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;


/**
 * Base class for implementing publishing and consuming JMS workers.
 *
 * @see JMSPublisher
 * @see JMSConsumer
 */
abstract class JMSWorker {

    protected final JmsTemplate jmsTemplate;
    protected final ComponentLog processLog;
    private final CachingConnectionFactory connectionFactory;
    private boolean isValid = true;


    /**
     * Creates an instance of this worker initializing it with JMS
     * {@link Connection} and creating a target {@link Channel} used by
     * sub-classes to interact with JMS systems
     *
     * @param jmsTemplate the instance of {@link JmsTemplate}
     * @param processLog the instance of {@link ComponentLog}
     */
    public JMSWorker(CachingConnectionFactory connectionFactory, JmsTemplate jmsTemplate, ComponentLog processLog) {
        this.connectionFactory = connectionFactory;
        this.jmsTemplate = jmsTemplate;
        this.processLog = processLog;
    }

    public void shutdown() {
        connectionFactory.destroy();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[destination:" + this.jmsTemplate.getDefaultDestinationName()
                + "; pub-sub:" + this.jmsTemplate.isPubSubDomain() + ";]";
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean isValid) {
        this.isValid = isValid;
    }
}
