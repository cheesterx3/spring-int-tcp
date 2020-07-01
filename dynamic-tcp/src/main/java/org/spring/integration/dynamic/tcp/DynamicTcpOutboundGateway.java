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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Objects.nonNull;

public class DynamicTcpOutboundGateway implements MessageProcessor<MessageChannel> {
    public static final String DEFAULT_RESPONSE_CHANNEL_NAME = "serverResponseChannel";
    /**
     * {@link org.springframework.integration.ip.tcp.TcpOutboundGateway#DEFAULT_REMOTE_TIMEOUT}
     */
    private static final long DEFAULT_REMOTE_TIMEOUT = 10_000L;
    /**
     * {@link org.springframework.integration.ip.tcp.connection.AbstractClientConnectionFactory#DEFAULT_CONNECT_TIMEOUT}
     */
    private static final int DEFAULT_CONNECT_TIMEOUT = 60;
    private final Map<String, IntegrationFlowContext.IntegrationFlowRegistration> flowCache = new ConcurrentHashMap<>();
    private final IntegrationFlowContext flowContext;
    private final EvaluationContext evaluationContext = new StandardEvaluationContext();
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private String responseChannel = DEFAULT_RESPONSE_CHANNEL_NAME;
    private AbstractByteArraySerializer deserializer = new ByteArrayCrLfSerializer();
    private Expression cacheableExpression = new ValueExpression<>(Boolean.FALSE);
    private Expression remoteTimeoutExpression = new ValueExpression<>(DEFAULT_REMOTE_TIMEOUT);
    private Expression connectTimeoutExpression = new ValueExpression<>(DEFAULT_CONNECT_TIMEOUT);
    private Serializer<?> serializer;
    private Expression hostExpression;
    private Expression portExpression;

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

        final TcpClientConnectionFactorySpec connectionFactorySpec = Tcp.netClient(host, port)
                .connectTimeout(getConnectTimeout(message));
        final TcpOutboundGatewaySpec gatewaySpec = getMessageHandlerSpec(connectionFactorySpec)
                .remoteTimeout(this::getRemoteTimeout);
        final IntegrationFlowContext.IntegrationFlowRegistration registration;
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
                .publishSubscribeChannel(threadPool, s -> s.subscribe(subFlow -> subFlow.channel(responseChannel)))
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

    private int getConnectTimeout(Message<?> requestMessage) {
        if (nonNull(connectTimeoutExpression)) {
            final Integer timeout = this.connectTimeoutExpression.getValue(this.evaluationContext, requestMessage, Integer.class);
            return nonNull(timeout) ? timeout : 0;
        }
        return DEFAULT_CONNECT_TIMEOUT;
    }

    private long getRemoteTimeout(Message<?> requestMessage) {
        if (nonNull(remoteTimeoutExpression)) {
            final Long timeout = this.remoteTimeoutExpression.getValue(this.evaluationContext, requestMessage, Long.class);
            return nonNull(timeout) ? timeout : 0;
        }
        return DEFAULT_REMOTE_TIMEOUT;
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

    public void setConnectTimeout(Expression timeoutExpression) {
        this.connectTimeoutExpression = timeoutExpression;
    }

    public void setRemoteTimeout(Expression timeoutExpression) {
        this.remoteTimeoutExpression = timeoutExpression;
    }
}
