package org.spring.integration.dynamic.tcp;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.spring.integration.dynamic.tcp.spec.DynamicTcpOutboundGatewaySpec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.*;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.ip.tcp.TcpReceivingChannelAdapter;
import org.springframework.integration.ip.tcp.TcpSendingMessageHandler;
import org.springframework.integration.ip.tcp.connection.TcpNetServerConnectionFactory;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.assertThat;


@ExtendWith(SpringExtension.class)
public class DynamicTcpOutboundGatewayTest {
    public static final String COMMAND = "test";
    public static final String FLOW_ID_ONE = DynamicTcpOutboundGateway.getFlowId("127.0.0.1", 5678);
    public static final String FLOW_ID_TWO = DynamicTcpOutboundGateway.getFlowId("127.0.0.1", 5679);
    public static final String FLOW_ID_THREE = DynamicTcpOutboundGateway.getFlowId("127.0.0.1", 5677);
    @Autowired
    private IntegrationFlowContext flowContext;
    @Autowired
    private SendEchoGateway echoGateway;
    private DynamicTcpOutboundGatewaySpec gatewaySpec;

    @BeforeEach
    public void setUp() {
        gatewaySpec = new DynamicTcpOutboundGatewaySpec(flowContext);
    }

    @Test
    public void testCacheable() {
        final IntegrationFlow flow = IntegrationFlows.from("sendChannel")
                .route(gatewaySpec
                        .host("headers['host']").port("headers['port']")
                        .cacheable("headers['port']!=5679")
                        .singleConnection("headers['port']!=5677")
                        .remoteTimeout(10_000L)
                        .responseChannelName("serverResponseChannel"))
                .get();
        flowContext.registration(flow).register();

        final String requestReplyOne = echoGateway.send(COMMAND, "127.0.0.1", 5678);
        final String requestReplyOneDup = echoGateway.send(COMMAND, "127.0.0.1", 5678);
        final String requestReplyTwo = echoGateway.send(COMMAND, "127.0.0.1", 5679);
        final String requestReplyThree = echoGateway.send(COMMAND, "127.0.0.1", 5677);
        final String requestReplyThreeDup = echoGateway.send(COMMAND, "127.0.0.1", 5677);
        final String requestReplyThreeDupDup = echoGateway.send(COMMAND, "127.0.0.1", 5677);

        final IntegrationFlowContext.IntegrationFlowRegistration registrationOne = flowContext.getRegistrationById(FLOW_ID_ONE);
        final IntegrationFlowContext.IntegrationFlowRegistration registrationTwo = flowContext.getRegistrationById(FLOW_ID_TWO);
        final IntegrationFlowContext.IntegrationFlowRegistration registrationThree = flowContext.getRegistrationById(FLOW_ID_THREE);

        assertThat(requestReplyOne)
                .isEqualTo(String.format("%s: answer", COMMAND))
                .isEqualTo(requestReplyOneDup);
        assertThat(requestReplyTwo).isEqualTo(String.format("%s: answer", COMMAND));
        assertThat(requestReplyThree)
                .isEqualTo(String.format("%s: answer", COMMAND))
                .isEqualTo(requestReplyThreeDup)
                .isEqualTo(requestReplyThreeDupDup);
        assertThat(registrationOne).isNotNull();
        assertThat(registrationThree).isNotNull();
        assertThat(registrationTwo).isNull();
    }

    @MessagingGateway(defaultRequestChannel = "sendChannel", defaultReplyChannel = "receiveChannel")
    private interface SendEchoGateway {
        String send(@Payload String command, @Header(name = "host") String host, @Header(name = "port") int port);
    }

    @Configuration
    @EnableIntegration
    @IntegrationComponentScan
    public static class ClientContextConfiguration {

        @Bean
        public IntegrationFlow serverResponseFlow() {
            return IntegrationFlows.from("serverResponseChannel")
                    .log()
                    .transform(Transformers.objectToString())
                    .channel("receiveChannel")
                    .get();
        }

        /// Client config
        @Bean
        public QueueChannel sendChannel() {
            return new QueueChannel();
        }

        @Bean(name = PollerMetadata.DEFAULT_POLLER)
        public PollerMetadata poller() {
            return Pollers.fixedRate(500).get();
        }

        @Bean
        public MessageChannel receiveChannel() {
            return MessageChannels.publishSubscribe().get();
        }

        @Bean
        public MessageChannel serverResponseChannel() {
            return MessageChannels.publishSubscribe().get();
        }
    }

    @Configuration
    @EnableIntegration
    @IntegrationComponentScan
    public static class ServerContextConfigurationOne {

        // Server config
        @Bean
        public TcpNetServerConnectionFactory serverConnectionOne() {
            return new TcpNetServerConnectionFactory(5678);
        }

        @Bean
        public TcpReceivingChannelAdapter serverReceiveOne(TcpNetServerConnectionFactory serverConnectionOne) {
            final TcpReceivingChannelAdapter adapter = new TcpReceivingChannelAdapter();
            adapter.setConnectionFactory(serverConnectionOne);
            adapter.setOutputChannel(serverInputChannelOne());
            return adapter;
        }

        @ServiceActivator(inputChannel = "serverOutChannelOne")
        @Bean
        public TcpSendingMessageHandler serverSendOne(TcpNetServerConnectionFactory serverConnectionOne) {
            final TcpSendingMessageHandler adapter = new TcpSendingMessageHandler();
            adapter.setConnectionFactory(serverConnectionOne);
            return adapter;
        }

        @Bean
        public MessageChannel serverInputChannelOne() {
            return MessageChannels.queue().get();
        }

        @Bean
        public QueueChannel serverOutChannelOne() {
            return new QueueChannel();
        }

        @ServiceActivator(inputChannel = "serverInputChannelOne", outputChannel = "serverOutChannelOne")
        public Message<String> processMessage(Message<String> message) {
            return MessageBuilder
                    .withPayload(message.getPayload() + ": answer")
                    .copyHeaders(message.getHeaders())
                    .build();
        }

    }

    @Configuration
    @EnableIntegration
    @IntegrationComponentScan
    public static class ServerContextConfigurationThree {

        // Server config
        @Bean
        public TcpNetServerConnectionFactory serverConnectionThree() {
            return new TcpNetServerConnectionFactory(5677);
        }

        @Bean
        public TcpReceivingChannelAdapter serverReceiveThree(TcpNetServerConnectionFactory serverConnectionThree) {
            final TcpReceivingChannelAdapter adapter = new TcpReceivingChannelAdapter();
            adapter.setConnectionFactory(serverConnectionThree);
            adapter.setOutputChannel(serverInputChannelThree());
            return adapter;
        }

        @ServiceActivator(inputChannel = "serverOutChannelThree")
        @Bean
        public TcpSendingMessageHandler serverSendThree(TcpNetServerConnectionFactory serverConnectionThree) {
            final TcpSendingMessageHandler adapter = new TcpSendingMessageHandler();
            adapter.setConnectionFactory(serverConnectionThree);
            return adapter;
        }

        @Bean
        public MessageChannel serverInputChannelThree() {
            return MessageChannels.queue().get();
        }

        @Bean
        public QueueChannel serverOutChannelThree() {
            return new QueueChannel();
        }

        @ServiceActivator(inputChannel = "serverInputChannelThree", outputChannel = "serverOutChannelThree")
        public Message<String> processMessage(Message<String> message) {
            return MessageBuilder
                    .withPayload(message.getPayload() + ": answer")
                    .copyHeaders(message.getHeaders())
                    .build();
        }

    }

    @Configuration
    @EnableIntegration
    @IntegrationComponentScan
    public static class ServerContextConfigurationTwo {

        // Server config
        @Bean
        public TcpNetServerConnectionFactory serverConnectionTwo() {
            return new TcpNetServerConnectionFactory(5679);
        }

        @Bean
        public TcpReceivingChannelAdapter serverReceiveTwo(TcpNetServerConnectionFactory serverConnectionTwo) {
            final TcpReceivingChannelAdapter adapter = new TcpReceivingChannelAdapter();
            adapter.setConnectionFactory(serverConnectionTwo);
            adapter.setOutputChannel(serverInputChannelTwo());
            return adapter;
        }

        @ServiceActivator(inputChannel = "serverOutChannelTwo")
        @Bean
        public TcpSendingMessageHandler serverSendTwo(TcpNetServerConnectionFactory serverConnectionTwo) {
            final TcpSendingMessageHandler adapter = new TcpSendingMessageHandler();
            adapter.setConnectionFactory(serverConnectionTwo);
            return adapter;
        }

        @Bean
        public MessageChannel serverInputChannelTwo() {
            return MessageChannels.queue().get();
        }

        @Bean
        public QueueChannel serverOutChannelTwo() {
            return new QueueChannel();
        }

        @ServiceActivator(inputChannel = "serverInputChannelTwo", outputChannel = "serverOutChannelTwo")
        public Message<String> processMessage(Message<String> message) {
            return MessageBuilder
                    .withPayload(message.getPayload() + ": answer")
                    .copyHeaders(message.getHeaders())
                    .build();
        }

    }
}

