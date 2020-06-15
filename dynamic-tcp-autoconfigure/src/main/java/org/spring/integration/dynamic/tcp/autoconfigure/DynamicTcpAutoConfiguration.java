package org.spring.integration.dynamic.tcp.autoconfigure;

import org.spring.integration.dynamic.tcp.spec.DynamicTcpOutboundGatewaySpec;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.context.IntegrationFlowContext;

@Configuration
@ConditionalOnClass(DynamicTcpOutboundGatewaySpec.class)
public class DynamicTcpAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public DynamicTcpOutboundGatewaySpec tcpOutboundGatewaySpec(IntegrationFlowContext flowContext){
        return new DynamicTcpOutboundGatewaySpec(flowContext);
    }
}
