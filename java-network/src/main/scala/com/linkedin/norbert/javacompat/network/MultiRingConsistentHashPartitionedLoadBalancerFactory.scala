/*
 * Copyright 2009-2010 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.norbert
package javacompat
package network

import com.linkedin.norbert.network.partitioned.loadbalancer.{PartitionedConsistentHashedLoadBalancerFactory => SPartitionedConsistentHashedLoadBalancerFactory}
import EndpointConversions._
import cluster.Node
import java.util.{Iterator, Set}

/**
 *  An adapter for a partitioned load balancer providing a consistent hash ring per partition.
 */
class MultiRingConsistentHashPartitionedLoadBalancerFactory[PartitionedId](numPartitions: Int,
                                                                    slicesPerEndpoint: Int,
                                                                    hashFunction: HashFunction[PartitionedId],
                                                                    endpointHashFunction: HashFunction[String],
                                                                    serveRequestsIfPartitionUnavailable: Boolean) extends PartitionedLoadBalancerFactory[PartitionedId] {

  def this(slicesPerEndpoint: Int, hashFunction: HashFunction[PartitionedId], endpointHashFunction: HashFunction[String], serveRequestsIfPartitionMissing: Boolean) = {
    this(-1, slicesPerEndpoint, hashFunction, endpointHashFunction, serveRequestsIfPartitionMissing)
  }

  val underlying = new SPartitionedConsistentHashedLoadBalancerFactory[PartitionedId](
    numPartitions,
    slicesPerEndpoint,
    (id: PartitionedId) => (hashFunction.hash(id) % Integer.MAX_VALUE).toInt,
    (distKey: String) => (endpointHashFunction.hash(distKey) % Integer.MAX_VALUE).toInt,
    serveRequestsIfPartitionUnavailable)

  val lbf = new ScalaLbfToJavaLbf[PartitionedId](underlying)

  def newLoadBalancer(endpoints: Set[Endpoint]) = lbf.newLoadBalancer(endpoints)

  def getNumPartitions(endpoints: Set[Endpoint]) = lbf.getNumPartitions(endpoints)
}