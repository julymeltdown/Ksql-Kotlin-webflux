package com.ksql.demo

import io.confluent.ksql.rest.entity.KsqlEntityList

data class RequestStatus(
    val message: String,
    val ksqlEntities: KsqlEntityList
)