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

import com.linkedin.norbert.network.partitioned.loadbalancer.{DefaultPartitionedLoadBalancerFactory => SDefaultPartitionedLoadBalancerFactory}
import EndpointConversions._
import cluster.Node
import com.linkedin.norbert.network.common.{Endpoint => SEndpoint}
import java.util.{Set => JSet}

/**
 * An adapter for the default, round-robining load balancer
 * @param numPartitions
 * @param serveRequestsIfPartitionMissing
 * @tparam PartitionedId
 */
abstract class DefaultPartitionedLoadBalancerFactory[PartitionedId]
(numPartitions: Int, serveRequestsIfPartitionMissing: Boolean = true) extends PartitionedLoadBalancerFactory[PartitionedId] {
  def this(numPartitions: Int) = this(numPartitions, true)
  
  val underlying = new SDefaultPartitionedLoadBalancerFactory[PartitionedId](serveRequestsIfPartitionMissing) {
    protected def calculateHash(id: PartitionedId) = hashPartitionedId(id)

    def getNumPartitions(endpoints: Set[SEndpoint]) = {
      if (numPartitions == -1) {
        endpoints.flatMap(_.getNode.getPartitionIds).size
      } else {
        numPartitions
      }
    }
  }

  val adapter = new ScalaLbfToJavaLbf[PartitionedId](underlying)
  
  def newLoadBalancer(endpoints: JSet[Endpoint]): PartitionedLoadBalancer[PartitionedId] =
    adapter.newLoadBalancer(endpoints)

  protected def hashPartitionedId(id : PartitionedId) : Int
}