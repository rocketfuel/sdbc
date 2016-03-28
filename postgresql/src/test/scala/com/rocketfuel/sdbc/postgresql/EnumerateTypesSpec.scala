package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.PostgreSql._

class EnumerateTypesSpec extends PostgreSqlSuite {

  test("list type map") {implicit connection =>

    Query[Unit](
      """CREATE TABLE tbl (
        | s bigserial,
        | i int,
        | bo boolean,
        | si smallint,
        | big bigint,
        | de numeric(3,1),
        | dou double precision,
        | re real,
        | t time,
        | tz timetz,
        | da date,
        | ts timestamp,
        | tstz timestamptz,
        | inter interval,
        | by bytea,
        | ine inet,
        | vc varchar(3),
        | c char(5),
        | te text,
        | u uuid,
        | x xml,
        | j json,
        | jb jsonb,
        | array0 int[],
        | array1 bigint[],
        | h hstore,
        | l ltree
        |)
      """.stripMargin
    ).run()

    val rs = connection.prepareStatement("SELECT * FROM tbl").executeQuery()

    val metadata = rs.getMetaData

    println("Map(")
    for (i <- 1 to metadata.getColumnCount) {
      println(s"${metadata.getColumnName(i)} -> ${metadata.getColumnTypeName(i)},")
    }
    println(")")

  }

}
