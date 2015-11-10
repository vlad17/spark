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

class HITSSuite extends SparkFunSuite with LocalSparkContext {
  private val epsilon = 1e-6

  test("Test star") {
    withSpark { sc =>
      val nPeripheralVertices = 10
      val starGraph = GraphGenerators.starGraph(sc, nPeripheralVertices + 1).cache()

      val (auth, hub, n) = starGraph.runHITS(0, 1)
      val expectedAuth = starGraph.vertices.mapValues(
        (vid, _) => if (vid == 0) 1.0 else 0.0)
      val expectedHub = starGraph.vertices.mapValues(
        (vid, _) => if (vid == 0) 0.0 else 1.0 / math.sqrt(nPeripheralVertices))

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
      val nPeripheralVertices = 10
      val starGraph = GraphGenerators.starGraph(sc, nPeripheralVertices + 1).reverse.cache()

      val (auth, hub, n) = starGraph.runHITS(epsilon, 3)
      val expectedAuth = starGraph.vertices.mapValues(
        (vid, _) => if (vid == 0) 0.0 else 1.0 / math.sqrt(nPeripheralVertices))
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
      val normedVal = 1.0 / math.sqrt(nVertices - 1)
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
      // With the following parameters, 100 trial runs all
      // converged within 4 iterations, so setting 20 as the cutoff
      // should be very safe.
      val maxIter = 20
      val nVertices = 50
      val tol = 0.001
      val graph = GraphGenerators.logNormalGraph(sc, nVertices)

      // Test verifies that convergence actually satisfies the tolerance
      // criterion (and that convergence is attainable within a reasonable
      // number of iterations).
      val (auth, hub, n) = graph.runHITS(tol, maxIter)

      // n < maxIter is very likely to be true. If false, the test may
      // need to be re-run (since tolerance has likely not been yet satisfied).
      assert(n < maxIter)
      val (nextAuth, nextHub, _) = graph.runHITS(0, n + 1)
      assert(HITS.sqerr(auth, nextAuth) <= tol)
      assert(HITS.sqerr(hub, nextHub) <= tol)
    }
  }
}
