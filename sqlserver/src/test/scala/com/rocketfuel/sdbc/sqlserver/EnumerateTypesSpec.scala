package com.rocketfuel.sdbc.sqlserver

import com.rocketfuel.sdbc.SqlServer._

/**
 * Output of this test:
 * int: int
 * bit: bit
 * tinyint: tinyint
 * smallint: smallint
 * bigint: bigint
 * decimal: decimal
 * float: float
 * real: real
 * time: nvarchar
 * date: nvarchar
 * smalldatetime: smalldatetime
 * datetime: datetime
 * datetime2: nvarchar
 * datetimeoffset: nvarchar
 * binary: binary
 * varbinary: image
 * image: image
 * char: char
 * nchar: nchar
 * varchar: varchar
 * nvarchar: nvarchar
 * text: text
 * uniqueidentifier: uniqueidentifier
 * hierarchy: varbinary
 * money: money
 * smallmoney: smallmoney
 * xml: ntext
 */
class EnumerateTypesSpec extends SqlServerSuite {

  ignore("list type map") {implicit connection =>

    Select[Unit](
      """CREATE TABLE tbl (
        | i int,
        | bo bit,
        | ti tinyint,
        | si smallint,
        | bi bigint,
        | de decimal(3,1),
        | fl float,
        | re real,
        | t time,
        | da date,
        | sts smalldatetime,
        | ts datetime,
        | ts2 datetime2,
        | ts3 datetimeoffset,
        | bin binary(1),
        | varb varbinary(max),
        | im image,
        | c char(1),
        | nc nchar(1),
        | varc varchar(1),
        | nvarc nvarchar(1),
        | te text,
        | u uniqueidentifier,
        | h hierarchyid,
        | mo money,
        | smm smallmoney,
        | x xml
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
