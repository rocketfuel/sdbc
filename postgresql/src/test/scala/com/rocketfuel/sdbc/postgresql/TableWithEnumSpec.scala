package com.rocketfuel.sdbc.postgresql

class TableWithEnumSpec extends PostgreSqlSuite.Base {

  import postgresql._

  case class TableHasEnum(
    id: Int,
    mood: String,
    value: String
  )

  object TableHasEnum {
    object All {
      implicit val selectable: Selectable[All.type, TableHasEnum] =
        Select[TableHasEnum]("select * from person").selectable.constant
    }
  }

  test("case class for table with enum") { implicit connection: Connection =>
    connection.execSQLUpdate("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")
    connection.execSQLUpdate("CREATE TABLE person (id serial primary key, mood mood not null, value text not null)")
    connection.execSQLUpdate("INSERT INTO person (mood, value) values ('sad', 'panda')")

    import syntax._

    val values = TableHasEnum.All.vector()
    assertResult(1)(values.size)
    assertResult("sad")(values.head.mood)
    assertResult("panda")(values.head.value)
  }

}
