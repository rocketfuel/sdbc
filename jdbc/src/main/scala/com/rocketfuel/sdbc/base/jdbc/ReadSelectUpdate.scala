package com.rocketfuel.sdbc.base.jdbc

trait ReadSelect {
  self: com.rocketfuel.Resources =>

  def readSelect[T](name: String, hasParameters: Boolean = true)(implicit getter: MutableRow => T): Select[T] = {
    val queryText = readResource("queries", name + ".sql")
    Select[T](queryText, hasParameters)
  }
}

trait ReadBatch {
  self: com.rocketfuel.Resources =>

  def readUpdate(name: String, hasParameters: Boolean = true): Batch = {
    val queryText = readResource("queries", name + ".sql")
    Batch(queryText, hasParameters)
  }
}

trait ReadUpdate {
  self: com.rocketfuel.Resources =>

  def readUpdate(name: String, hasParameters: Boolean = true): Update = {
    val queryText = readResource("queries", name + ".sql")
    Update(queryText, hasParameters)
  }
}

trait ReadSelectForUpdate {
  self: com.rocketfuel.Resources =>

  def readSelectForUpdate(name: String, hasParameters: Boolean = true): SelectForUpdate = {
    val queryText = readResource("queries", name + ".sql")
    SelectForUpdate(queryText, hasParameters)
  }
}
