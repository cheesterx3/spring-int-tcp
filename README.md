# Dynamic tcp outbound gateway implementation
### Description
Allows to create tcp outbound gateways using host/port according some expressions(for example from headers).

Also allows to cache connections to prevent register/unregister flows and beans in runtime.

This is just a simple implementation of single request/reply using dynamic registration integration flows
### Includes
* Dynamic tcp outbound gateway library
* Dynamic tcp outbound autoconfigure

### Usage

```
    private final DynamicTcpOutboundGatewaySpec outboundGatewaySpec;

    @Bean
    public IntegrationFlow serverRequestFlow() {
        return IntegrationFlows.from("serverInfoRequestChannel")
                .route(outboundGatewaySpec
                        .host("headers['host']").port("headers['port']")
                        .deserializer(deserializer)
                        .serializer(serializer)
                        .cacheable("!headers['host'].equals(\"127.0.0.1\")")
                        .responseChannelName("serverResponseChannel"))
                .get();
    }

    @Bean
    public IntegrationFlow serverResponseFlow() {
        return IntegrationFlows.from("serverResponseChannel")
                .transform(Transformers.objectToString())
                .transform(dataTransformer)
                .channel("serverInfoResponseChannel")
                .get();
    }
```