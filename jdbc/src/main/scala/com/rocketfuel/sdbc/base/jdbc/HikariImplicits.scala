package com.rocketfuel.sdbc.base.jdbc

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigException, ConfigValueType}
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.pool.HikariPool

import scala.collection.convert.wrapAsScala._

private[jdbc] trait HikariImplicits {

  implicit class ConfigOps(config: Config) {

    private def toHikariProperties: Properties = {
      val properties = new java.util.Properties()

      for (entry <- config.entrySet) {
        val key = entry.getKey
        val value = entry.getValue

        if (hikariDurationProperties.contains(key)) {
          val duration = config.getDuration(key, TimeUnit.MILLISECONDS)
          properties.setProperty(key, duration.toString)
        } else if (hikariOtherProperties.contains(key)) {
          value.valueType() match {
            case ConfigValueType.STRING =>
              properties.setProperty(key, config.getString(key))
            case ConfigValueType.BOOLEAN =>
              properties.setProperty(key, config.getBoolean(key).toString)
            case ConfigValueType.NUMBER =>
              properties.setProperty(key, config.getNumber(key).toString)
            case ConfigValueType.NULL =>
              properties.setProperty(key, null)
            case _ =>
            //ignore objects and lists
          }
        }
      }

      properties
    }

    def toHikariPool: HikariPool = {
      new HikariPool(toHikariConfig)
    }

    def toHikariConfig: HikariConfig = {
      val poolProperties = toHikariProperties

      val hikariConfig = new HikariConfig(poolProperties)

      //Set data source properties if they exist.
      try {
        val dataSourceConfig = config.getConfig("dataSource")
        val dataSourceProperties = dataSourceConfig.toProperties
        hikariConfig.setDataSourceProperties(dataSourceProperties)
      } catch {
        case e: ConfigException.Missing =>
      }

      hikariConfig
    }

    def toProperties: Properties = {
      val properties = new java.util.Properties()
      for (entry <- config.entrySet) {
        val key = entry.getKey
        properties.setProperty(key, config.getString(key))
      }
      properties
    }
  }

  private val hikariOtherProperties =
    Set(
      "dataSourceClassName",
      "jdbcUrl",
      "username",
      "password",
      "autoCommit",
      "connectionTimeout",
      "idleTimeout",
      "maxLifetime",
      "connectionTestQuery",
      "minimumIdle",
      "maximumPoolSize",
      "metricRegistry",
      "healthCheckRegistry",
      "poolName",
      "initializationFailFast",
      "isolateInternalQueries",
      "allowPoolSuspension",
      "readOnly",
      "registerMbeans",
      "catalog",
      "connectionInitSql",
      "driverClassName",
      "transactionIsolation",
      "validationTimeout",
      "leakDetectionThreshold",
      "dataSource",
      "threadFactory"
    )

  private val hikariDurationProperties =
    Set(
      "connectionTimeout",
      "validationTimeout",
      "idleTimeout",
      "maxLifetime",
      "leakDetectionThreshold"
    )
}
