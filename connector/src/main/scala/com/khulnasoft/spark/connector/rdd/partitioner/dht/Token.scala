package com.khulnasoft.spark.connector.rdd.partitioner.dht

import com.khulnasoft.spark.connector.rdd.partitioner.MonotonicBucketing
import com.khulnasoft.spark.connector.rdd.partitioner.MonotonicBucketing.LongBucketing
import com.khulnasoft.oss.driver.api.core.metadata.token.{Token => NativeToken}
import com.khulnasoft.oss.driver.internal.core.metadata.token.{Murmur3Token, RandomToken}

trait Token[T] extends Ordered[Token[T]] {
  def ord: Ordering[T]
  def value: T
  def nativeToken: NativeToken
}


case class LongToken(value: Long) extends Token[Long] {
  override def compare(that: Token[Long]) = value.compareTo(that.value)
  override def toString = value.toString
  override def ord: Ordering[Long] = implicitly[Ordering[Long]]
  override def nativeToken: NativeToken = new Murmur3Token(value)
}

object LongToken {

  // Will work both for MonotonicOrdering[Token[Long]] and MonotonicOrdering[LongToken]
  // because MonotonicOrdering is contravariant on T
  implicit object LongTokenBucketing extends MonotonicBucketing[Token[Long]] {
    override def bucket(n: Int): Token[Long] => Int = {
      val longBucket = LongBucketing.bucket(n)
      x => longBucket(x.value)
    }
  }

  // Ordering[T] is not contravariant, so despite getting an automatic Ordering[Token[Long]] for free
  // (because it is Ordered), we don't get a similar Ordering[LongToken].
  // Therefore we have to define it here:
  implicit val LongTokenOrdering: Ordering[LongToken] =
    Ordering.by(_.value)
}

case class BigIntToken(value: BigInt) extends Token[BigInt] {
  override def compare(that: Token[BigInt]) = value.compare(that.value)
  override def toString = value.toString()
  override def ord: Ordering[BigInt] = implicitly[Ordering[BigInt]]
  override def nativeToken: NativeToken = new RandomToken(value.bigInteger)
}

object BigIntToken {

  // Will work both for MonotonicOrdering[Token[Long]] and MonotonicOrdering[BigIntToken],
  // because MonotonicOrdering is contravariant on T
  implicit object BigIntTokenBucketing extends MonotonicBucketing[Token[BigInt]] {
    override def bucket(n: Int): Token[BigInt] => Int = {
      val shift = 127 - MonotonicBucketing.log2(n).toInt
      def clamp(x: BigInt): BigInt = if (x == BigInt(-1)) BigInt(0) else x
      x => (clamp(x.value) >> shift).toInt
    }
  }

  // Ordering[T] is not contravariant, so despite getting an automatic Ordering[Token[BigInt]] for free
  // (because it is Ordered), we don't get a similar Ordering[BigIntToken].
  // Therefore we have to define it here:
  implicit val BigIntTokenOrdering: Ordering[BigIntToken] =
    Ordering.by(_.value)
}

