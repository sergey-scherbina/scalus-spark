package org.scalus.spark

import org.apache.spark.sql.*
import org.junit.Test

class ScalusSparkTest extends Serializable {
  val path = "src/test/resources/blocks"

  @Test def test1(): Unit = {
    val spark = SparkSession.builder
      .master("local[*]").getOrCreate()
    import spark.implicits.*
    try {
      spark.readCardanoCbor(path)
        .map(_.block.hash.toHex)
        .show(false)
    } finally spark.stop
  }

}