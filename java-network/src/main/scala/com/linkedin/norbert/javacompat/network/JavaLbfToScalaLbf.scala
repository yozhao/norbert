package com.linkedin.norbert
package javacompat
package network

import com.linkedin.norbert.network.partitioned.loadbalancer.{PartitionedLoadBalancerFactory => SPartitionedLoadBalancerFactory, PartitionedLoadBalancer => SPartitionedLoadBalancer}
import com.linkedin.norbert.network.client.loadbalancer.{LoadBalancerFactory => SLoadBalancerFactory, LoadBalancer => SLoadBalancer}

import com.linkedin.norbert.cluster.{Node => SNode}
import com.linkedin.norbert.network.common.{Endpoint => SEndpoint}

import com.linkedin.norbert.EndpointConversions._

class JavaLbfToScalaLbf[PartitionedId](javaLbf: PartitionedLoadBalancerFactory[PartitionedId]) extends SPartitionedLoadBalancerFactory[PartitionedId] {
  def newLoadBalancer(nodes: Set[SEndpoint]) = {
    val lb = javaLbf.newLoadBalancer(nodes)
    new SPartitionedLoadBalancer[PartitionedId] {
      def nextNode(id: PartitionedId) = {
        Option(lb.nextNode(id))
      }

      def nodesForOneReplica(id: PartitionedId) = {
        val jMap = lb.nodesForOneReplica(id)
        var sMap = Map.empty[com.linkedin.norbert.cluster.Node, Set[Int]]

        val entries = jMap.entrySet.iterator
        while(entries.hasNext) {
          val entry = entries.next
          val node = javaNodeToScalaNode(entry.getKey)
          val set = entry.getValue.foldLeft(Set.empty[Int]) { (s, elem) => s + elem.intValue}

          sMap += (node -> set)
        }
        sMap
      }

      def nodesForPartitions(id: PartitionedId, partitions: Set[Int]) = {
        val jMap = lb.nodesForOneReplica(id)
        var sMap = Map.empty[com.linkedin.norbert.cluster.Node, Set[Int]]

        val entries = jMap.entrySet.iterator
        while(entries.hasNext) {
          val entry = entries.next
          val node = javaNodeToScalaNode(entry.getKey)
          val set = entry.getValue.foldLeft(Set.empty[Int]) { (s, elem) => s + elem.intValue}

          sMap += (node -> set)
        }
        sMap
      }
    }
    
  }

  def getNumPartitions(endpoints: Set[SEndpoint]) = javaLbf.getNumPartitions(endpoints).intValue()
}