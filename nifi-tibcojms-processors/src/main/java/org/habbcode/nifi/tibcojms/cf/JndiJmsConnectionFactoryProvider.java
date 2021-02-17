package org.habbcode.nifi.tibcojms.cf;
/*
 *   Alexandr Mikhaylov created on 16.02.2021 inside the package - org.habbcode.nifi.tibcojms.cf
 */

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProviderDefinition;

import javax.jms.ConnectionFactory;
import java.util.List;

@Tags({"jms", "jndi", "messaging", "integration", "queue", "topic", "publish", "subscribe"})
@CapabilityDescription("Provides a service to lookup an existing JMS ConnectionFactory using the Java Naming and Directory Interface (JNDI).")
@DynamicProperty(
        description = "In order to perform a JNDI Lookup, an Initial Context must be established. When this is done, an Environment can be established for the context. Any dynamic/user-defined property" +
                " that is added to this Controller Service will be added as an Environment configuration/variable to this Context.",
        name = "The name of a JNDI Initial Context environment variable.",
        value = "The value of the JNDI Initial Context environment variable.",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
@SeeAlso(classNames = {"org.apache.nifi.jms.processors.ConsumeJMS", "org.apache.nifi.jms.processors.PublishJMS", "org.apache.nifi.jms.cf.JMSConnectionFactoryProvider"})
public class JndiJmsConnectionFactoryProvider extends AbstractControllerService implements JMSConnectionFactoryProviderDefinition {

    private volatile JndiJmsConnectionFactoryHandler delegate;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return JndiJmsConnectionFactoryProperties.getPropertyDescriptors();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return JndiJmsConnectionFactoryProperties.getDynamicPropertyDescriptor(propertyDescriptorName);
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        delegate = new JndiJmsConnectionFactoryHandler(context, getLogger());
    }

    @OnDisabled
    public void onDisabled() {
        delegate = null;
    }

    @Override
    public ConnectionFactory getConnectionFactory() {
        return delegate.getConnectionFactory();
    }

    @Override
    public void resetConnectionFactory(ConnectionFactory cachedFactory) {
        delegate.resetConnectionFactory(cachedFactory);
    }
}
