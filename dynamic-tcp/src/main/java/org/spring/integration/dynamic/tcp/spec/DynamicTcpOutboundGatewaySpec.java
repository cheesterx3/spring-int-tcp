package org.spring.integration.dynamic.tcp.spec;

import org.spring.integration.dynamic.tcp.DynamicTcpOutboundGateway;
import org.springframework.core.serializer.Serializer;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.dsl.MessageProcessorSpec;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.ip.tcp.serializer.AbstractByteArraySerializer;
import org.springframework.messaging.Message;

import java.util.function.Function;

public class DynamicTcpOutboundGatewaySpec extends MessageProcessorSpec<DynamicTcpOutboundGatewaySpec> {
    private static final SpelExpressionParser PARSER = new SpelExpressionParser();

    public DynamicTcpOutboundGatewaySpec(IntegrationFlowContext flowContext) {
        this.target = new DynamicTcpOutboundGateway(flowContext);
    }

    public DynamicTcpOutboundGatewaySpec deserializer(AbstractByteArraySerializer deserializer) {
        getRequestFlow().setDeserializer(deserializer);
        return _this();
    }

    public DynamicTcpOutboundGatewaySpec serializer(Serializer<?> serializer) {
        getRequestFlow().setSerializer(serializer);
        return _this();
    }

    public DynamicTcpOutboundGatewaySpec responseChannelName(String responseChannelName) {
        getRequestFlow().setResponseChannel(responseChannelName);
        return _this();
    }

    public <P> DynamicTcpOutboundGatewaySpec host(Function<Message<P>, ?> hostExpression) {
        getRequestFlow().setHostExpression(new FunctionExpression<>(hostExpression));
        return _this();
    }

    public <P> DynamicTcpOutboundGatewaySpec port(Function<Message<P>, ?> portExpression) {
        getRequestFlow().setPortExpression(new FunctionExpression<>(portExpression));
        return _this();
    }

    public <P> DynamicTcpOutboundGatewaySpec cacheable(Function<Message<P>, ?> cacheableExpression) {
        getRequestFlow().setCacheableExpression(new FunctionExpression<>(cacheableExpression));
        return _this();
    }

    public DynamicTcpOutboundGatewaySpec host(String expression) {
        getRequestFlow().setHostExpression(PARSER.parseExpression(expression));
        return _this();
    }

    public DynamicTcpOutboundGatewaySpec port(String expression) {
        getRequestFlow().setPortExpression(PARSER.parseExpression(expression));
        return _this();
    }

    public DynamicTcpOutboundGatewaySpec cacheable(String expression) {
        getRequestFlow().setCacheableExpression(PARSER.parseExpression(expression));
        return _this();
    }

    private DynamicTcpOutboundGateway getRequestFlow() {
        return (DynamicTcpOutboundGateway) this.target;
    }
}
