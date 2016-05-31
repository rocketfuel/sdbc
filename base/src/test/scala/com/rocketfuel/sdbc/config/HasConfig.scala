package com.rocketfuel.sdbc.config

import com.typesafe.config.Config

trait HasConfig {
  def config: Config
}
