package com.rocketfuel.sdbc.postgresql

import scalaz.Scalaz._
import com.rocketfuel.sdbc.PostgreSql._

class SeqGetterSpec
  extends PostgreSqlSuite {

  testSelect[Seq[Option[Boolean]]]("SELECT '{}'::boolean[]", Seq.empty[Option[Boolean]].some)

  testSelect[Seq[Option[Int]]]("SELECT '{}'::int[]", Seq.empty[Option[Int]].some)

  testSelect[Seq[Boolean]]("SELECT '{t}'::boolean[]", Seq(true).some)

  testSelect[Seq[LTree]]("SELECT ARRAY['l.tree']::ltree[]", Seq(LTree("l", "tree")).some)

  testSelect[Seq[Int]]("SELECT '{1,2,3}'::int[]", SeqGetterSpec.oneTwoThree.some)

  testSelect[Seq[Seq[Int]]]("SELECT '{{1},{2},{3}}'::int[]", SeqGetterSpec.oneTwoThree.map(x => Seq(x)).some)

  testSelect[Seq[Seq[Int]]]("SELECT '{{1,2,3},{4,5,6}}'::int[][]", Seq(SeqGetterSpec.oneTwoThree, SeqGetterSpec.fourFiveSix).some)

  testSelect[Seq[Option[Seq[Int]]]]("SELECT '{{1,2,3},{4,5,6}}'::int[][] --optional Seq", Seq(Some(SeqGetterSpec.oneTwoThree), Some(SeqGetterSpec.fourFiveSix)).some)

  testSelect[Seq[Seq[Option[Int]]]]("SELECT '{{1,2,3},{4,5,6}}'::int[][] --optional Int", Seq(SeqGetterSpec.oneTwoThreeOption, SeqGetterSpec.fourFiveSixOption).some)

  testSelect[Seq[Option[Seq[Option[Int]]]]]("SELECT '{{1,2,3},{4,5,6}}'::int[][] --optional Seq and Int", Seq(Some(SeqGetterSpec.oneTwoThreeOption), Some(SeqGetterSpec.fourFiveSixOption)).some)

  testSelect[Seq[Byte]]("SELECT E'\\\\x010203'::bytea", SeqGetterSpec.oneTwoThree.map(_.toByte).some)

}

object SeqGetterSpec {
  val oneTwoThree = Seq(1, 2, 3)

  val oneTwoThreeOption = oneTwoThree.map(Some.apply)

  val fourFiveSix = Seq(4, 5, 6)

  val fourFiveSixOption = fourFiveSix.map(Some.apply)
}
