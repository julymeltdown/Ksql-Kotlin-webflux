package com.ksql.demo

import io.confluent.ksql.api.client.Client
import io.confluent.ksql.api.client.ExecuteStatementResult
import io.confluent.ksql.api.client.ServerInfo
import org.springframework.http.HttpStatus
import org.springframework.util.StringUtils
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Mono
import java.lang.String.format
import java.util.concurrent.CompletableFuture
import java.util.stream.Collectors


class KsqlDbRequestHandler(
    private val ksqlClient: Client
) {

    fun info(serverRequest: ServerRequest?): Mono<ServerResponse> {
        val serverInfoFuture: CompletableFuture<ServerInfo> = ksqlClient.serverInfo()

        return Mono.fromFuture(serverInfoFuture)
            .flatMap { serverInfo ->
                ServerResponse.ok()
                    .bodyValue(
                        String.format(
                            "timestamp: %d, cluster_id: %s, ksql_service_id: %s, version: %s",
                            System.currentTimeMillis(),
                            serverInfo.kafkaClusterId,
                            serverInfo.ksqlServiceId,
                            serverInfo.serverVersion,
                        )
                    )
            }
            .onErrorResume { e ->
                val body = String.format("Error: %s", e.message)
                ServerResponse.status(HttpStatus.INTERNAL_SERVER_ERROR).bodyValue(body)
            }
    }


    fun createStream(serverRequest: ServerRequest): Mono<ServerResponse> {
        return serverRequest.bodyToMono(CreateStreamRequest::class.java)
            .flatMap { createStreamRequest ->
                val ksql = formatRequest(createStreamRequest)
                val executeStatementResultFuture: CompletableFuture<ExecuteStatementResult> = ksqlClient.executeStatement(ksql)

                Mono.fromFuture(executeStatementResultFuture)
                    .flatMap { executeStatementResult ->
                        executeStatementResult.queryId()?.let { Mono.just(it) } ?: Mono.empty()
                    }
            }
            .flatMap { requestStatus ->
                ServerResponse.ok().bodyValue(requestStatus)
            }
            .onErrorResume { e ->
                e.message.let { ServerResponse.badRequest().build() }
            }
    }

    private fun formatRequest(createStreamRequest: CreateStreamRequest): String {
        return if (createStreamRequest.createTopic) format(
            CREATE_STREAM_AND_TOPIC_STATEMENT,
            createStreamRequest.streamName,
            generateColumns(createStreamRequest, true),
            generateTopicProperties(createStreamRequest)
        ) else format(
            CREATE_STREAM_STATEMENT,
            createStreamRequest.streamName,
            generateColumns(createStreamRequest, false),
            createStreamRequest.sourceTopicName
        )
    }



    private fun generateTopicProperties(createStreamRequest: CreateStreamRequest): String {
        return format(
            "kafka_topic = '%s', partitions = %d, value_format = '%s'",
            createStreamRequest.sourceTopicName,
            createStreamRequest.partitions,
            createStreamRequest.valueFormat
        )
    }

    private fun generateColumns(createStreamRequest: CreateStreamRequest, withTypes: Boolean): String {
        val columns: Map<String, String> = createStreamRequest.columns!!
        return StringUtils.collectionToCommaDelimitedString(
            if (withTypes) columnsWithTypes(
                columns,
                createStreamRequest.keyColumn!!
            ) else columns.keys
        )
    }

    private fun columnsWithTypes(columns: Map<String, String>, keyColumn: String): Collection<String?> {
        return columns.entries.stream()
            .map { (key, value): Map.Entry<String, String> ->
                "$key $value" + if (key.equals(
                        keyColumn,
                        ignoreCase = true
                    )
                ) " KEY" else ""
            }
            .collect(Collectors.toUnmodifiableList())
    }

    companion object {
        const val REQUEST_SUCCESS_MESSAGE = "request processed successfully"

        // see https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/create-stream/
        const val CREATE_STREAM_STATEMENT = "CREATE STREAM %s AS SELECT %s FROM %s EMIT CHANGES;"
        const val CREATE_STREAM_AND_TOPIC_STATEMENT = "CREATE STREAM %s (%s) WITH (%s);"
    }
}

