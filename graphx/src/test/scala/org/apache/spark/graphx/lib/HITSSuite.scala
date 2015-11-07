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

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.EmptyRDD

class HITSSuite extends SparkFunSuite with LocalSparkContext {
  private val epsilon = 1e-6

  test("Test star") {
    withSpark { sc =>
      val nPeripheralVertices = 1000
      val starGraph = GraphGenerators.starGraph(sc, nPeripheralVertices + 1).cache()

      val (auth, hub, n) = starGraph.runHITS(0, 1)
      val expectedAuth = starGraph.vertices.mapValues(
        (vid, _) => if (vid == 0) 1.0 else 0.0)
      val expectedHub = starGraph.vertices.mapValues(
        (vid, _) => if (vid == 0) 0.0 else 1.0 / Math.sqrt(nPeripheralVertices))

      assert(HITS.sqerr(auth, expectedAuth) <= epsilon)
      assert(HITS.sqerr(hub, expectedHub) <= epsilon)

      val (auth2, hub2, n2) = starGraph.runHITS(0, 2)

      assert(n2 == 2)
      assert(HITS.sqerr(auth2, expectedAuth) <= epsilon)
      assert(HITS.sqerr(hub2, expectedHub) <= epsilon)

      val (auth3, hub3, n3) = starGraph.runHITS(epsilon, 3)

      assert(n3 == 2)
      assert(HITS.sqerr(auth3, expectedAuth) <= epsilon)
      assert(HITS.sqerr(hub3, expectedHub) <= epsilon)
    }
  }

  test("Test reverse star") {
    withSpark { sc =>
      val nPeripheralVertices = 1000
      val starGraph = GraphGenerators.starGraph(sc, nPeripheralVertices + 1).reverse.cache()

      val (auth, hub, n) = starGraph.runHITS(epsilon, 3)
      val expectedAuth = starGraph.vertices.mapValues(
        (vid, _) => if (vid == 0) 0.0 else 1.0 / Math.sqrt(nPeripheralVertices))
      val expectedHub = starGraph.vertices.mapValues(
        (vid, _) => if (vid == 0) 1.0 else 0.0)

      assert(n == 2)
      assert(HITS.sqerr(auth, expectedAuth) <= epsilon)
      assert(HITS.sqerr(hub, expectedHub) <= epsilon)
    }
  }

  test("Test chain") {
    withSpark { sc =>
      val nVertices = 10
      val chain = sc.parallelize((0 until nVertices - 1).map(x => x.toLong).map(x => (x, x + 1)))
      val chainGraph = Graph.fromEdgeTuples(chain, ())

      val (auth, hub, n) = chainGraph.runHITS(0, 1)
      val normedVal = 1.0 / Math.sqrt(nVertices - 1)
      val expectedAuth = chainGraph.vertices.mapValues(
        (vid, _) => if (vid == 0) 0.0 else normedVal)
      val expectedHub = chainGraph.vertices.mapValues(
        (vid, _) => if (vid == nVertices - 1) 0.0 else normedVal)

      assert(n == 1)
      assert(HITS.sqerr(auth, expectedAuth) <= epsilon)
      assert(HITS.sqerr(hub, expectedHub) <= epsilon)

      val (authEnd, hubEnd, nEnd) = chainGraph.runHITS(epsilon, 3)
      assert(nEnd == 2)
      assert(HITS.sqerr(authEnd, expectedAuth) <= epsilon)
      assert(HITS.sqerr(hubEnd, expectedHub) <= epsilon)
    }
  }

  test("Test convergence") {
    withSpark { sc =>
      // Convergence is feasible, but extremely slow:
      // Theorem 1 in http://www.dei.unipd.it/~pretto/cocoon/hits_convergence.pdf
      // This just double checks that the convergence criterion satisfies the tolerance
      // bound.
      val nVertices = 1000
      val tol = 0.001
      var graph = GraphGenerators.logNormalGraph(sc, nVertices)

      val (auth, hub, n) = graph.runHITS(tol, Int.MaxValue)
      val (expectedAuth, expectedHub, _) = graph.runHITS(0, n)
      assert(HITS.sqerr(auth, expectedAuth) == 0)
      assert(HITS.sqerr(hub, expectedHub) == 0)

      val (nextAuth, nextHub, _) = graph.runHITS(0, n + 1)
      assert(HITS.sqerr(auth, nextAuth) <= tol)
      assert(HITS.sqerr(hub, nextHub) <= tol)
    }
  }
}
