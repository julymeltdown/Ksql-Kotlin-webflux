package com.ksql.demo


data class CreateStreamRequest(
    var streamName: String? = null,
    var columns: Map<String, String>? = null,
    var sourceTopicName: String? = null,
    var createTopic:Boolean = false,
    var partitions: Int = 0,
    var replicas: Short = 0,
    var keyColumn: String? = null,
    var valueFormat: String? = null,
)

