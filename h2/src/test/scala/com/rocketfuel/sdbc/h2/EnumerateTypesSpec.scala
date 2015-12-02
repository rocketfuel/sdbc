package com.rocketfuel.sdbc.h2

class EnumerateTypesSpec extends H2Suite {

  ignore("list type map") {implicit connection =>

    Update(
      """CREATE TABLE tbl (
        | i int,
        | bo boolean,
        | ti tinyint,
        | si smallint,
        | big bigint,
        | de decimal(3,1),
        | do double,
        | re real,
        | t time,
        | da date,
        | ts timestamp,
        | bin binary,
        | by bytea(5),
        | r raw(5),
        | o other,
        | vc varchar(3),
        | vcic varchar_ignorecase,
        | c char,
        | bl blob,
        | u uuid,
        | a array,
        |)
      """.stripMargin
    ).update()

    val rs = connection.prepareStatement("SELECT * FROM tbl").executeQuery()

    val metadata = rs.getMetaData

    println("Map(")
    for (i <- 1 to metadata.getColumnCount) {
      println(s"${metadata.getColumnName(i)} -> ${metadata.getColumnTypeName(i)},")
    }
    println(")")

  }

}
