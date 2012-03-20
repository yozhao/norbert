package com.linkedin.norbert.network.common

import com.linkedin.norbert.cluster.Node

trait Endpoint {
  def node: Node

  def canServeRequests: Boolean


  override def hashCode() = node.hashCode

  override def equals(that: Any) = that match {
    case e: Endpoint => this.node.equals(e.node)
    case _ => false
  }

  override def toString = "ID = %d ServingRequests = %b Node = %s".format(node.id, canServeRequests, node.toString)
}