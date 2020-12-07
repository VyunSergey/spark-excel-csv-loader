package com.vyunsergey.sparkexcelcsvloader.data

import scala.util.Random

case class IntegersTestRow(a: Short, b: Int, c: Long, d: Short, e: Int, f: Long)
case class NumbersTestRow(a: Byte, b: Short, c: Int, d: Long, e: Float, f: Double)
case class DecimalTestRow(a: java.math.BigDecimal, b: java.math.BigDecimal, c: java.math.BigDecimal, d: java.math.BigDecimal, e: java.math.BigDecimal, f: java.math.BigDecimal)
case class StringsTestRow(a: String, b: String, c: String, d: String, e: String, f: String)
case class DateTimeTestRow(a: java.sql.Date, b: java.sql.Timestamp, c: java.sql.Date, d: java.sql.Timestamp, e: java.sql.Date, f: java.sql.Timestamp)
case class RandomTestRow(a: Byte, b: Short, c: Int, d: Long, e: Float, f: Double, g: Boolean, h: java.math.BigDecimal, i: String, j: String, k: java.sql.Date, l: java.sql.Timestamp)

object TestRow {
  def genIntegersSeqRows(n: Int): Seq[IntegersTestRow] =
    Seq.fill(n)(genIntegersTestRow)

  def genNumbersSeqRows(n: Int): Seq[NumbersTestRow] =
    Seq.fill(n)(genNumbersTestRow)

  def genDecimalSeqRows(n: Int): Seq[DecimalTestRow] =
    Seq.fill(n)(genDecimalTestRow)

  def genStringsSeqRows(n: Int, stringLength: Int): Seq[StringsTestRow] =
    Seq.fill(n)(genStringsTestRow(stringLength))

  def genDateTimeSeqRow(n: Int): Seq[DateTimeTestRow] =
    Seq.fill(n)(genDateTimeTestRow)

  def genRandomSeqRows(n: Int, stringLength: Int): Seq[RandomTestRow] =
    Seq.fill(n)(genRandomTestRow(stringLength))

  def genRandomBigDecimal: java.math.BigDecimal =
    java.math.BigDecimal.valueOf(Random.nextDouble())

  def genRandomDate: java.sql.Date =
    java.sql.Date.valueOf(java.time.LocalDate.ofEpochDay(Math.abs(Random.nextInt(10000 * 365)).toLong))

  def genRandomTimestamp: java.sql.Timestamp =
    java.sql.Timestamp.from(java.time.Instant.ofEpochSecond(Math.abs(Random.nextInt(10000 * 365 * 24 * 60 * 60)).toLong))

  def genIntegersTestRow: IntegersTestRow =
    IntegersTestRow(
      a = Random.nextInt(Short.MaxValue).toShort,
      b = Random.nextInt(),
      c = Random.nextLong(),
      d = Random.nextInt(Short.MaxValue).toShort,
      e = Random.nextInt(),
      f = Random.nextLong()
    )

  def genNumbersTestRow: NumbersTestRow = {
    val bytes: Array[Byte] = Array.empty[Byte]
    Random.nextBytes(bytes)

    NumbersTestRow(
      a = bytes.headOption.getOrElse(0.toByte),
      b = Random.nextInt(Short.MaxValue).toShort,
      c = Random.nextInt(),
      d = Random.nextLong(),
      e = Random.nextFloat(),
      f = Random.nextDouble()
    )
  }

  def genDecimalTestRow: DecimalTestRow =
    DecimalTestRow(
      a = genRandomBigDecimal,
      b = genRandomBigDecimal,
      c = genRandomBigDecimal,
      d = genRandomBigDecimal,
      e = genRandomBigDecimal,
      f = genRandomBigDecimal
    )

  def genStringsTestRow(stringLength: Int): StringsTestRow =
    StringsTestRow(
      a = Random.nextString(stringLength),
      b = Random.nextString(stringLength),
      c = Random.nextString(stringLength),
      d = Random.nextString(stringLength),
      e = Random.nextString(stringLength),
      f = Random.nextString(stringLength)
    )

  def genDateTimeTestRow: DateTimeTestRow =
    DateTimeTestRow(
      a = genRandomDate,
      b = genRandomTimestamp,
      c = genRandomDate,
      d = genRandomTimestamp,
      e = genRandomDate,
      f = genRandomTimestamp
    )

  def genRandomTestRow(stringLength: Int): RandomTestRow = {
    val bytes: Array[Byte] = Array.empty[Byte]
    Random.nextBytes(bytes)

    RandomTestRow(
      a = bytes.headOption.getOrElse(0.toByte),
      b = Random.nextInt(Short.MaxValue).toShort,
      c = Random.nextInt(),
      d = Random.nextLong(),
      e = Random.nextFloat(),
      f = Random.nextDouble(),
      g = Random.nextBoolean(),
      h = genRandomBigDecimal,
      i = Random.nextString(1).toCharArray.headOption.getOrElse('0').toString,
      j = Random.nextString(stringLength),
      k = genRandomDate,
      l = genRandomTimestamp
    )
  }
}
