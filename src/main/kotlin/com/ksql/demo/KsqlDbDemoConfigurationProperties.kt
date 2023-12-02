package com.ksql.demo

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "org.kidoni.ksqldb")
class KsqlDbDemoConfigurationProperties {
    /**
     * the URL of the ksqlDb server
     */
    var ksqlDbServer = DEFAULT_SERVER_ADDRESS

    companion object {
        const val DEFAULT_SERVER_ADDRESS = "http://localhost:8088"
    }
}

