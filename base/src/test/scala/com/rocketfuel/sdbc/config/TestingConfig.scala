package com.rocketfuel.sdbc.config

import com.rocketfuel.sdbc.base.HasConfig

import scala.util.Random

trait TestingConfig extends HasConfig {
  def testCatalogPrefix: String = config.getString("testCatalogPrefix")

  lazy val testCatalogSuffix: String = Random.nextInt(Int.MaxValue).toString

  def testCatalogName: String = testCatalogPrefix + testCatalogSuffix
}
