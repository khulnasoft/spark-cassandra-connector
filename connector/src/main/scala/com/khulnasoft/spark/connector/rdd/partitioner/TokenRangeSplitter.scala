package com.khulnasoft.spark.connector.rdd.partitioner

import scala.collection.parallel.{ForkJoinTaskSupport, ParIterable}
import java.util.concurrent.ForkJoinPool
import com.khulnasoft.spark.connector.rdd.partitioner.TokenRangeSplitter.WholeRing
import com.khulnasoft.spark.connector.rdd.partitioner.dht.{Token, TokenRange}
import com.khulnasoft.spark.connector.util.RuntimeUtil


/** Splits a token ranges into smaller sub-ranges,
  * each with the desired approximate number of rows. */
private[partitioner] trait TokenRangeSplitter[V, T <: Token[V]] {

  def split(tokenRanges: Iterable[TokenRange[V, T]], splitCount: Int): Iterable[TokenRange[V, T]] = {

    val ringFractionPerSplit = WholeRing / splitCount.toDouble
    val parTokenRanges: ParIterable[TokenRange[V, T]] = RuntimeUtil.toParallelIterable(tokenRanges)

    parTokenRanges.tasksupport = new ForkJoinTaskSupport(TokenRangeSplitter.pool)
    parTokenRanges.flatMap(tokenRange => {
      val splitCount = Math.rint(tokenRange.ringFraction / ringFractionPerSplit).toInt
      split(tokenRange, math.max(1, splitCount))
    }).toList
  }

  /** Splits the token range uniformly into splitCount sub-ranges. */
   def split(tokenRange: TokenRange[V, T], splitCount: Int): Seq[TokenRange[V, T]]
}

object TokenRangeSplitter {
  private val MaxParallelism = 16

  private val WholeRing = 1.0

  private val pool = new ForkJoinPool(MaxParallelism)
}
