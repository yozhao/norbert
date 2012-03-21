package com.linkedin.norbert
package javacompat
package network

import com.linkedin.norbert.network.partitioned.loadbalancer.{PartitionedLoadBalancerFactory => SPartitionedLoadBalancerFactory}
import com.linkedin.norbert.EndpointConversions._
import cluster.Node

class ScalaLbfToJavaLbf[PartitionedId](scalaLbf: SPartitionedLoadBalancerFactory[PartitionedId]) extends PartitionedLoadBalancerFactory[PartitionedId] {

  def newLoadBalancer(endpoints: java.util.Set[Endpoint]) = {
    val scalaBalancer = scalaLbf.newLoadBalancer(endpoints)

    new PartitionedLoadBalancer[PartitionedId] {
      def nodesForOneReplica(id: PartitionedId) = {
        val replica = scalaBalancer.nodesForOneReplica(id)
        val result = new java.util.HashMap[Node, java.util.Set[java.lang.Integer]](replica.size)
        
        replica.foreach { case (node, partitions) =>
          result.put(node, partitions)
        }

        result
      }

      def nextNode(id: PartitionedId) = {
        scalaBalancer.nextNode(id) match {
          case Some(n) => n
          case None => null
        }
      }

      def nodesForPartitions(id: PartitionedId, partitions: java.util.Set[java.lang.Integer]) = {
        val replica = scalaBalancer.nodesForPartitions(id, partitions)
        val result = new java.util.HashMap[Node, java.util.Set[java.lang.Integer]](replica.size)

        replica.foreach { case (node, partitions) =>
          result.put(node, partitions)
        }

        result
      }
    }
  }

  def getNumPartitions(endpoints: java.util.Set[Endpoint]) = {
    scalaLbf.getNumPartitions(endpoints)
  }
}