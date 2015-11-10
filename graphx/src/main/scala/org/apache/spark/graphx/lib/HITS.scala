/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.lib

import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Implementation of Hyperlink Induced Topic Search (HITS) algorithm according to the original 1997
 * paper, "Authoritative Sources in a Hyperlinked Enviornment" by Jon Kleinberg.
 *
 * HITS is very similar to PageRank in that is a per-vertex computation which finds the hub
 * and authority values for a set of web pages. A hub value represents how much of a directory
 * the given site is - it quantifies how many other sites this page links to and how reliable they
 * are. An authority value represents the content reliability on the page, it is determined by
 * the number of links to that page.
 *
 * An authority update is defined by summing the hub scores of all pages point to it.
 * A hub update is defined by summing the authority scores of all pages it points to.
 *
 * For more information, including convergence criteria, see "HITS Can Converge Slowly,
 * but Not Too Slowly, in Score and Rank" by Enoch Peserico and Luca Pretto.
 */
object HITS extends Logging {
  private def sqnorm(xs: RDD[Double]): Double = { xs.map(x => x * x).sum() }
  private def norm(xs: VertexRDD[Double]): Double = { math.sqrt(sqnorm(xs.values)) }

  /**
   * Used to evaluate the square error between two graphs vertex weights.
   *
   * @param a the first graph's vertices
   * @param b the second graph's vertices
   *
   * @return the sum of squares of the difference between the two graphs' vertex weights.
   */
  private[lib] def sqerr(a: VertexRDD[Double], b: VertexRDD[Double]): Double = {
    sqnorm(a.join(b).values.map((x) => x._1 - x._2))
  }

  private def sqerr(a: Graph[Double, _], b: Graph[Double, _]): Double = {
    sqerr(a.vertices, b.vertices)
  }

  /**
   * Run HITS until an L2-convergence tolerance is reached or we hit the maximum
   * number of iterations.
   *
   * @param graph the graph on which to compute HITS
   * @param tol the target squared error between score vectors for both authority and hub
   * attributes.
   * @param maxIter the number of iterations of HITS to run
   *
   * @return the [[VertexRDD]]s with the vertex authority and hub values as attributes,
   * respectively, followed by the number of iterations it required to reach the target tolerance.
   */
  def run(graph: Graph[_, _], tol: Double, maxIter: Int):
      (VertexRDD[Double], VertexRDD[Double], Int) = {
    // Unfortunately, HITS requires a global normalization operation for every update, so it is not
    // naturally parallelizable to the vertex level. As such, the Pregel interface cannot
    // be applied.

    var auth: Graph[Double, _] = Graph(
      graph.vertices.mapValues(Function.const(1)(_)),
      graph.edges.mapValues(Function.const(())(_)))
    var hub: Graph[Double, _] = auth.cache()

    val edges = auth.edges

    var i = 0
    var squareError = tol
    while (i < maxIter && squareError >= tol) {
      val unnormalizedAuth = hub.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr), _ + _, TripletFields.Src).cache()
      val authNorm = norm(unnormalizedAuth)
      val authNew = Graph(unnormalizedAuth.mapValues(_ / authNorm), edges)

      val unnormalizedHub = auth.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr), _ + _, TripletFields.Dst).cache()
      val hubNorm = norm(unnormalizedHub)
      val hubNew = Graph(unnormalizedHub.mapValues(_ / hubNorm), edges)

      authNew.cache()
      val authErr = sqerr(auth, authNew)
      unnormalizedAuth.unpersist(false)
      auth = authNew

      hubNew.cache()
      val hubErr = sqerr(hub, hubNew)
      unnormalizedHub.unpersist(false)
      hub = hubNew

      squareError = math.max(authErr, hubErr)
      logInfo(s"HITS finished iteration $i.")
      i += 1
    }

    (auth.vertices, hub.vertices, i)
  }
}
