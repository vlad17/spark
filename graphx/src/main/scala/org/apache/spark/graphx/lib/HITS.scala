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
 * Implementation of HITS algorithm according to Wikipedia.
 *
 * HITS is very similar to PageRank in that is a per-vertex computation which finds the hub
 * and authority values for a set of web pages. A hub value represents how much of a directory
 * the given site is - it quantifies how many other sites this page links to and how reliable they
 * are. An authority value represents the content reliability on the page, it is determined by
 * the number of links to that page.
 *
 * Unfortunately, HITS requires a global normalization operation for every update, so it is not
 * naturally parallelizable to the vertex level. As such, [[Pregel]] cannot be used.
 *
 * An authority update is defined by summing the hub scores of all pages point to it.
 * A hub update is defined by summing the authority scores of all pages it points to.
 *
 * For more information, including convergence criteria, see "HITS Can Converge Slowly,
 * but Not Too Slowly, in Score and Rank" by Enoch Peserico and Luca Pretto
 */
object HITS extends Logging {
  private def sqnorm(xs: RDD[Double]) : Double = {
    xs.map(x => x * x).reduce(_ + _)
  }
  private def sqnorm(xs: VertexRDD[Double]) : Double = { sqnorm(xs.values) }
  private def norm(xs: VertexRDD[Double]) : Double = { Math.sqrt(sqnorm(xs)) }


  /**
   * Used to evaluate the square error between two graphs vertex weights.
   *
   * @tparam ED the original edge attribute (not used)
   *
   * @param a the first graph's vertices
   * @param b the second graph's vertices
   *
   * @return the sum of squares of the difference between the two graphs' vertex weights.
   */
  def sqerr(a: VertexRDD[Double], b: VertexRDD[Double]): Double = {
    sqnorm(a.join(b).values.map((x) => x._1 - x._2))
  }

  private def sqerr(a: Graph[Double, _], b: Graph[Double, _]): Double = {
    sqerr(a.vertices, b.vertices)
  }

  /**
   * Run HITS until an L2-convergence tolerance is reached or we hit the maximum
   * number of iterations.
   *
   *
   * @param graph the graph on which to compute HITS
   * @param tol the target squared error between score vectors for both authority and hub
   * attributes.
   * @param maxIter the number of iterations of HITS to run
   *
   * @return the Graphs with the vertex authority and hub values as attributes, respectively,
   * followed by the number of iterations it required to reach the target tolerance.
   */
  def run(graph: Graph[_, _], tol: Double, maxIter: Int):
      (VertexRDD[Double], VertexRDD[Double], Int) =
  {
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
      unnormalizedAuth.unpersist(false)

      val unnormalizedHub = auth.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr), _ + _, TripletFields.Dst).cache()
      val hubNorm = norm(unnormalizedHub)
      val hubNew = Graph(unnormalizedHub.mapValues(_ / hubNorm), edges)
      unnormalizedHub.unpersist(false)

      authNew.cache()
      val authErr = sqerr(auth, authNew)
      auth = authNew

      hubNew.cache()
      val hubErr = sqerr(hub, hubNew)
      hub = hubNew

      squareError = math.max(authErr, hubErr)
      logInfo(s"HITS finished iteration $i.")
      i += 1
    }

    (auth.vertices, hub.vertices, i)
  }
}
