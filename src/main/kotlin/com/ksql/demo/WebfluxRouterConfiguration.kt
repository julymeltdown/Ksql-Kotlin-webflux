package com.ksql.demo

import io.confluent.ksql.api.client.Client
import io.confluent.ksql.api.client.ClientOptions
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.config.EnableWebFlux
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.RouterFunctions
import org.springframework.web.reactive.function.server.ServerResponse
import java.util.*


@EnableWebFlux
@Configuration
class WebfluxRouterConfiguration {
    @Bean
    fun infoRouterFunction(ksqlDbRequestHandler: KsqlDbRequestHandler): RouterFunction<ServerResponse> {
        return RouterFunctions.route()
            .GET("/info", ksqlDbRequestHandler::info)
            .POST("/stream", ksqlDbRequestHandler::createStream)
            .build()
    }

    @Bean
    fun ksqlDbRequestHandler(ksqlClient: Client): KsqlDbRequestHandler {
        return KsqlDbRequestHandler(ksqlClient)
    }

    @Bean
    fun ksqlRestClient(configurationProperties: KsqlDbDemoConfigurationProperties): Client {
        val options: ClientOptions = ClientOptions.create()
            .setHost(configurationProperties.ksqlDbServer)
            .setUseTls(false)
            .setBasicAuthCredentials("admin", "admin")
        return Client.create(
            options
        )
    }

}

