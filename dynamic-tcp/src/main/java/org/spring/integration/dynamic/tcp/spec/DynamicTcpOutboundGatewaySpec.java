package org.spring.integration.dynamic.tcp.spec;

import org.spring.integration.dynamic.tcp.DynamicTcpOutboundGateway;
import org.springframework.core.serializer.Serializer;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.dsl.MessageProcessorSpec;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.ip.tcp.serializer.AbstractByteArraySerializer;
import org.springframework.messaging.Message;

import java.util.function.Function;

public class DynamicTcpOutboundGatewaySpec extends MessageProcessorSpec<DynamicTcpOutboundGatewaySpec> {
    private static final SpelExpressionParser PARSER = new SpelExpressionParser();

    public DynamicTcpOutboundGatewaySpec(IntegrationFlowContext flowContext) {
        this.target = new DynamicTcpOutboundGateway(flowContext);
    }

    /**
     * Setting a deserializer
     *
     * @param deserializer instance of deserializer
     * @return DynamicTcpOutboundGatewaySpec
     */
    public DynamicTcpOutboundGatewaySpec deserializer(AbstractByteArraySerializer deserializer) {
        getRequestFlow().setDeserializer(deserializer);
        return _this();
    }

    /**
     * Setting a serializer
     *
     * @param serializer instance of serializer
     * @return DynamicTcpOutboundGatewaySpec
     */
    public DynamicTcpOutboundGatewaySpec serializer(Serializer<?> serializer) {
        getRequestFlow().setSerializer(serializer);
        return _this();
    }

    /**
     * Setting a name of channel where response will be sended from tcp connection
     *
     * @param responseChannelName channel name
     * @return DynamicTcpOutboundGatewaySpec
     */
    public DynamicTcpOutboundGatewaySpec responseChannelName(String responseChannelName) {
        getRequestFlow().setResponseChannel(responseChannelName);
        return _this();
    }

    /**
     * Setting a host retrieving function from message
     *
     * @param hostExpression expression function
     * @param <P>            payload type
     * @return DynamicTcpOutboundGatewaySpec
     */
    public <P> DynamicTcpOutboundGatewaySpec host(Function<Message<P>, ?> hostExpression) {
        getRequestFlow().setHostExpression(new FunctionExpression<>(hostExpression));
        return _this();
    }

    /**
     * Setting a port retrieving function from message
     *
     * @param portExpression expression function
     * @param <P>            payload type
     * @return DynamicTcpOutboundGatewaySpec
     */
    public <P> DynamicTcpOutboundGatewaySpec port(Function<Message<P>, ?> portExpression) {
        getRequestFlow().setPortExpression(new FunctionExpression<>(portExpression));
        return _this();
    }

    /**
     * Setting a cacheable param retrieving function from message
     *
     * @param cacheableExpression expression function
     * @param <P>                 payload type
     * @return DynamicTcpOutboundGatewaySpec
     */
    public <P> DynamicTcpOutboundGatewaySpec cacheable(Function<Message<P>, ?> cacheableExpression) {
        getRequestFlow().setCacheableExpression(new FunctionExpression<>(cacheableExpression));
        return _this();
    }

    /**
     * Setting a host retrieving expression from message
     *
     * @param expression expression
     * @return DynamicTcpOutboundGatewaySpec
     */
    public DynamicTcpOutboundGatewaySpec host(String expression) {
        getRequestFlow().setHostExpression(PARSER.parseExpression(expression));
        return _this();
    }

    /**
     * Setting a port retrieving expression from message
     *
     * @param expression expression
     * @return DynamicTcpOutboundGatewaySpec
     */
    public DynamicTcpOutboundGatewaySpec port(String expression) {
        getRequestFlow().setPortExpression(PARSER.parseExpression(expression));
        return _this();
    }

    /**
     * Setting a cacheable retrieving expression from message
     *
     * @param expression expression
     * @return DynamicTcpOutboundGatewaySpec
     */
    public DynamicTcpOutboundGatewaySpec cacheable(String expression) {
        getRequestFlow().setCacheableExpression(PARSER.parseExpression(expression));
        return _this();
    }

    /**
     * Setting a connectTimeout value retrieving expression from message
     *
     * @param expression expression
     * @return DynamicTcpOutboundGatewaySpec
     */
    public DynamicTcpOutboundGatewaySpec connectTimeout(String expression) {
        getRequestFlow().setConnectTimeout(PARSER.parseExpression(expression));
        return _this();
    }

    /**
     * Setting a remoteTimeout value retrieving expression from message
     *
     * @param expression expression
     * @return DynamicTcpOutboundGatewaySpec
     */
    public DynamicTcpOutboundGatewaySpec remoteTimeout(String expression) {
        getRequestFlow().setRemoteTimeout(PARSER.parseExpression(expression));
        return _this();
    }

    /**
     * Setting a connectTimeout value retrieving expression from message
     *
     * @param timeout connect timeout value
     * @return DynamicTcpOutboundGatewaySpec
     */
    public DynamicTcpOutboundGatewaySpec connectTimeout(int timeout) {
        getRequestFlow().setConnectTimeout(new ValueExpression<>(timeout));
        return _this();
    }

    /**
     * Setting a remoteTimeout value retrieving expression from message
     *
     * @param timeout remote timeout value
     * @return DynamicTcpOutboundGatewaySpec
     */
    public DynamicTcpOutboundGatewaySpec remoteTimeout(long timeout) {
        getRequestFlow().setRemoteTimeout(new ValueExpression<>(timeout));
        return _this();
    }

    public DynamicTcpOutboundGatewaySpec singleConnection(String expression) {
        getRequestFlow().setSingleConnection(PARSER.parseExpression(expression));
        return _this();
    }

    public DynamicTcpOutboundGatewaySpec singleConnection(boolean value) {
        getRequestFlow().setSingleConnection(new ValueExpression<>(value));
        return _this();
    }

    private DynamicTcpOutboundGateway getRequestFlow() {
        return (DynamicTcpOutboundGateway) this.target;
    }
}
