package org.spring.integration.dynamic.tcp;

import org.springframework.core.serializer.Serializer;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.handler.MessageProcessor;
import org.springframework.integration.ip.dsl.Tcp;
import org.springframework.integration.ip.dsl.TcpClientConnectionFactorySpec;
import org.springframework.integration.ip.dsl.TcpOutboundGatewaySpec;
import org.springframework.integration.ip.tcp.serializer.AbstractByteArraySerializer;
import org.springframework.integration.ip.tcp.serializer.ByteArrayCrLfSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class DynamicTcpOutboundGateway implements MessageProcessor<MessageChannel> {
    public static final String DEFAULT_RESPONSE_CHANNEL_NAME = "serverResponseChannel";
    private final Map<String, IntegrationFlowContext.IntegrationFlowRegistration> flowCache = new ConcurrentHashMap<>();
    private final IntegrationFlowContext flowContext;
    private final EvaluationContext evaluationContext = new StandardEvaluationContext();
    private String responseChannel = DEFAULT_RESPONSE_CHANNEL_NAME;
    private AbstractByteArraySerializer deserializer = new ByteArrayCrLfSerializer();
    private Serializer<?> serializer;
    private Expression hostExpression;
    private Expression portExpression;
    private Expression cacheableExpression = new ValueExpression<>(Boolean.FALSE);

    public DynamicTcpOutboundGateway(IntegrationFlowContext flowContext) {
        this.flowContext = flowContext;
    }

    private static String getFlowId(String host, Integer port) {
        return String.format("flow_%s_%d.chl", host, port);
    }

    @Override
    public MessageChannel processMessage(Message<?> message) {
        final String host = getHost(message);
        final int port = getPort(message);
        final boolean cacheable = isCacheable(message);
        final String flowId = getFlowId(host, port) + (cacheable ? "" : System.nanoTime());

        final TcpClientConnectionFactorySpec connectionFactorySpec = Tcp.netClient(host, port);
        final TcpOutboundGatewaySpec gatewaySpec = getMessageHandlerSpec(connectionFactorySpec);
        IntegrationFlowContext.IntegrationFlowRegistration registration;
        if (!cacheable) {
            registration = createNonCacheableConnectionFlow(flowId, gatewaySpec);
        } else {
            registration = flowCache.computeIfAbsent(flowId, id -> createConnectionFlow(id, gatewaySpec));

        }
        return registration.getInputChannel();
    }

    private IntegrationFlowContext.IntegrationFlowRegistration createConnectionFlow(String flowId, TcpOutboundGatewaySpec gatewaySpec) {
        final IntegrationFlow flow = f -> f
                .handle(gatewaySpec)
                .channel(responseChannel);
        return flowContext
                .registration(flow)
                .id(flowId)
                .register();
    }

    private IntegrationFlowContext.IntegrationFlowRegistration createNonCacheableConnectionFlow(String flowId, TcpOutboundGatewaySpec gatewaySpec) {
        final IntegrationFlow flow = f -> f
                .handle(gatewaySpec)
                .publishSubscribeChannel(Executors.newCachedThreadPool(), s -> s.subscribe(subFlow -> subFlow.channel(responseChannel)))
                .handle(message -> flowContext.remove(flowId));
        return flowContext
                .registration(flow)
                .id(flowId)
                .register();
    }

    private TcpOutboundGatewaySpec getMessageHandlerSpec(TcpClientConnectionFactorySpec connectionFactorySpec) {
        final TcpClientConnectionFactorySpec connectionFactory = connectionFactorySpec
                .singleUseConnections(true);
        if (serializer != null) {
            connectionFactory.serializer(serializer);
        }
        if (deserializer != null) {
            connectionFactory.deserializer(deserializer);
        }
        return Tcp.outboundGateway(connectionFactory);
    }

    private boolean isCacheable(Message<?> requestMessage) {
        if (this.cacheableExpression != null) {
            final Boolean value = this.cacheableExpression.getValue(this.evaluationContext, requestMessage, Boolean.class);
            if (value != null) {
                return value;
            }
        }
        return true;
    }

    private String getHost(Message<?> requestMessage) {
        Assert.state(this.hostExpression != null, "host expression is missing");
        final String host = this.hostExpression.getValue(this.evaluationContext, requestMessage, String.class);
        Assert.state(host != null, "host is missing");
        return host;
    }

    private int getPort(Message<?> requestMessage) {
        Assert.state(this.portExpression != null, "port expression is missing");
        final Integer port = this.portExpression.getValue(this.evaluationContext, requestMessage, Integer.class);
        Assert.state(port != null, "port is missing");
        return port;
    }

    public void setDeserializer(AbstractByteArraySerializer deserializer) {
        this.deserializer = deserializer;
    }

    public void setResponseChannel(String responseChannel) {
        this.responseChannel = responseChannel;
    }

    public void setHostExpression(Expression hostExpression) {
        this.hostExpression = hostExpression;
    }

    public void setPortExpression(Expression portExpression) {
        this.portExpression = portExpression;
    }

    public void setSerializer(Serializer<?> serializer) {
        this.serializer = serializer;
    }

    public void setCacheableExpression(Expression cacheableExpression) {
        this.cacheableExpression = cacheableExpression;
    }

}
