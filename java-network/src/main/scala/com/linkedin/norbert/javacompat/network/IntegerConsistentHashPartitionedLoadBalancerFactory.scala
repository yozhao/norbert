package com.linkedin.norbert.javacompat.network

import java.util.Set
import com.linkedin.norbert.EndpointConversions
import EndpointConversions._

class IntegerConsistentHashPartitionedLoadBalancerFactory(numPartitions: Int, serveRequestsIfPartitionMissing: Boolean)
        extends DefaultPartitionedLoadBalancerFactory[Int](numPartitions, serveRequestsIfPartitionMissing) {
  def this(numPartitions: Int) = this(numPartitions, true)
  def this() = this(-1, true)

  protected def hashPartitionedId(id: Int) =  id.hashCode

  def getNumPartitions(endpoints: Set[Endpoint]) = {
    if (numPartitions == -1) {
      val endpointSet = convertJavaEndpointSet(endpoints)
      endpointSet.flatMap(_.node.partitionIds).size
    } else {
      numPartitions
    }
  }
}