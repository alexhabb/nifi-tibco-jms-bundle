package org.habbcode.nifi.tibcojms.cf;
/*
 *   Alexandr Mikhaylov created on 16.02.2021 inside the package - org.habbcode.nifi.tibcojms.cf
 */

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import java.util.Arrays;
import java.util.List;

public class TibcoJMSConnectionFactoryProperties {

    private static final String BROKER = "Tibco EMS Server URI";
    private static final String CF_IMPL = "cf";
    private static final String CF_LIB = "cflib";

    public static final PropertyDescriptor JMS_CONNECTION_FACTORY_IMPL = new PropertyDescriptor.Builder()
            .name("CF_IMPL")
            .displayName("JMS Connection Factory Implementation Class")
            .description("The fully qualified name of the JMS ConnectionFactory implementation "
                    + "class (eg. com.tibco.tibjms.TibjmsQueueConnectionFactory).")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor JMS_CLIENT_LIBRARIES = new PropertyDescriptor.Builder()
            .name("JMS Client Libraries")
            .displayName("JMS Client Libraries")
            .description("Path to the directory with additional resources (eg. JARs, configuration files etc.) to be added "
                    + "to the classpath (defined as a comma separated list of values). Such resources typically represent target JMS client libraries "
                    + "for the ConnectionFactory implementation.")
            .required(false)
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.createURLorFileValidator()))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .dynamicallyModifiesClasspath(true)
            .build();

    public static final PropertyDescriptor JMS_BROKER_URI = new PropertyDescriptor.Builder()
            .name("Tibco EMS Server URI")
            .displayName("Tibco JMS Broker URI")
            .description("URI pointing to the network location of the JMS Message broker. Example: "
                    + "'tcp://tibco_host:7222'  or 'ssl://tibco_host:7243'")
            .required(false)
            .addValidator(new NonEmptyBrokerURIValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor JMS_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service for JMS CLient")
            .displayName("JMS SSL Context Servicefor JMS CLient")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor TIBCO_ENABLE_VERIFY_HOST = new PropertyDescriptor.Builder()
            .name("Tibco setSSLEnableVerifyHost")
            .description("Name of SSL property specifying if client should verify server certificate."
                    + "By default the client always verifies server certificate to be issued by one or more certificates specified by TRUSTED_CERTIFICATES parameter."
                    + "If this property is set to false then the client does not perform server certificate verification."
                    + "The value is a Boolean object.")
            .required(false)
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor TIBCO_ENABLE_VERIFY_HOST_NAME = new PropertyDescriptor.Builder()
            .name("Tibco setSSLEnableVerifyHostName")
            .description("Name of SSL property specifying if client should verify the common name in the server certificate."
                    + "It is enabled by default, unless the host verification is disabled."
                    + "By default, if the EXPECTED_HOST_NAME property is not specified, the client verifies the common name to be the same as the name of the connected host."
                    + " If EXPECTED_HOST_NAME is set then it should be the name in the certificate. "
                    + "Alternatively the client can provide custom host name verifier or disable host name verification completely setting this property to false."
                    + "The value is a Boolean object.")
            .required(false)
            .allowableValues("true", "false")
            .build();


    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Arrays.asList(
            TibcoJMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL,
            TibcoJMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES,
            TibcoJMSConnectionFactoryProperties.JMS_BROKER_URI,
            TibcoJMSConnectionFactoryProperties.JMS_SSL_CONTEXT_SERVICE,
            TibcoJMSConnectionFactoryProperties.TIBCO_ENABLE_VERIFY_HOST,
            TibcoJMSConnectionFactoryProperties.TIBCO_ENABLE_VERIFY_HOST_NAME
    );

    public static List<PropertyDescriptor> getPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    public static PropertyDescriptor getDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName
                        + "' property to be set on the provided Connection Factory implementation.")
                .name(propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .build();
    }

    /**
     * {@link Validator} that ensures that brokerURI's length > 0 after EL
     * evaluation
     */
    private static class NonEmptyBrokerURIValidator implements Validator {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }
            return StandardValidators.NON_EMPTY_VALIDATOR.validate(subject, input, context);
        }
    }

}
