package com.linkedin.norbert.javacompat.network;

import com.linkedin.norbert.javacompat.cluster.Node;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;


public class ConsistentHashPartitionedLoadBalancer<PartitionedId> implements PartitionedLoadBalancer<PartitionedId>
{
  private final HashFunction<String> _hashFunction;
  private final TreeMap<Long, Map<Endpoint, Set<Integer>>> _routingMap;
  private final PartitionedLoadBalancer<PartitionedId> _fallThrough;

  ConsistentHashPartitionedLoadBalancer(int bucketCount,
                                        HashFunction<String> hashFunction,
                                        Set<Endpoint> endpoints,
                                        PartitionedLoadBalancer<PartitionedId> fallThrough)
  {
    _hashFunction = hashFunction;
    _routingMap = new TreeMap<Long, Map<Endpoint, Set<Integer>>>();
    _fallThrough = fallThrough;

    // Gather set of nodes for each partition
    Map<Integer, Set<Endpoint>> partitionNodes = new TreeMap<Integer, Set<Endpoint>>();
    for (Endpoint endpoint : endpoints)
    {
      Node node = endpoint.getNode();
      for (Integer partId : node.getPartitionIds())
      {
        Set<Endpoint> partNodes = partitionNodes.get(partId);
        if (partNodes == null)
        {
          partNodes = new HashSet<Endpoint>();
          partitionNodes.put(partId, partNodes);
        }
        partNodes.add(endpoint);
      }
    }

    // Builds individual ring for each partitions
    int maxSize = 0;
    Map<Integer, NavigableMap<Long, Endpoint>> rings = new TreeMap<Integer, NavigableMap<Long, Endpoint>>();
    for (Map.Entry<Integer, Set<Endpoint>> entry : partitionNodes.entrySet())
    {
      Integer partId = entry.getKey();
      NavigableMap<Long, Endpoint> ring = rings.get(partId);
      if (ring == null)
      {
        ring = new TreeMap<Long, Endpoint>();
        rings.put(partId, ring);
      }

      if (maxSize < entry.getValue().size())
      {
        maxSize = entry.getValue().size();
      }

      for (Endpoint endpoint : entry.getValue())
      {
        for (int i = 0; i < bucketCount; i++)
        {
          // Use node-[node_id]-[bucket_id] as key
          // Hence for the same node, same bucket id will always hash to the same place
          // This helps to maintain consistency when the bucketCount changed
          ring.put(hashFunction.hash(String.format("node-%d-%d", endpoint.getNode().getId(), i)), endpoint);
        }
      }
    }

    // Build one final ring.
    for (int slot = 0; slot < bucketCount * maxSize; slot++)
    {
      Long point = hashFunction.hash(String.format("ring-%d", slot));

      // For each generated point on the ring, gather node for each partition.
      Map<Endpoint, Set<Integer>> pointRoute = new HashMap<Endpoint, Set<Integer>>();
      for (Map.Entry<Integer, NavigableMap<Long, Endpoint>> ringEntry : rings.entrySet())
      {
        Endpoint endpoint = lookup(ringEntry.getValue(), point);

        Set<Integer> partitionSet = pointRoute.get(endpoint);
        if (partitionSet == null)
        {
          partitionSet = new HashSet<Integer>();
          pointRoute.put(endpoint, partitionSet);
        }
        partitionSet.add(ringEntry.getKey()); // Add partition to the node
      }
      _routingMap.put(point, pointRoute);
    }
  }

  @Override
  public Node nextNode(PartitionedId partitionedId)
  {
    if(_fallThrough != null)
      return _fallThrough.nextNode(partitionedId);

    // TODO: How do we choose which node to return if we don't want to throw Exception?
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<Node, Set<Integer>> nodesForOneReplica(PartitionedId partitionedId)
  {
    Map<Endpoint, Set<Integer>> replica = lookup(_routingMap, _hashFunction.hash(partitionedId.toString()));
    Map<Node, Set<Integer>> results = new HashMap<Node, Set<Integer>>();

    Set<Integer> unsatisfiedPartitions = new HashSet<Integer>();
    
    // Attempt to filter out results that are not available
    for(Map.Entry<Endpoint, Set<Integer>> entry : replica.entrySet())
    {
      if(entry.getKey().canServeRequests())
      {
        results.put(entry.getKey().getNode(), entry.getValue());
      }
      else
      {
        unsatisfiedPartitions.addAll(entry.getValue());
      }
    }
    

    if(unsatisfiedPartitions.size() > 0)
    {
      Map<Node, Set<Integer>> resolved = _fallThrough.nodesForPartitions(partitionedId, unsatisfiedPartitions);
      for(Map.Entry<Node, Set<Integer>> entry : resolved.entrySet()) 
      {
        Set<Integer> partitions = results.get(entry.getKey());
        if(partitions != null) 
        {
          partitions.addAll(entry.getValue());
        }
        else
        {
          results.put(entry.getKey(), entry.getValue());
        }
      }
    }

    return results;
  }

  @Override
  public Map<Node, Set<Integer>> nodesForPartitions(PartitionedId partitionedId, Set<Integer> partitions) {
    Map<Node, Set<Integer>> entireReplica = nodesForOneReplica(partitionedId);
    
    Map<Node, Set<Integer>> result = new HashMap<Node, Set<Integer>>();
    for(Map.Entry<Node, Set<Integer>> entry : entireReplica.entrySet())
    {
      Set<Integer> localPartitions = entry.getValue();
      Set<Integer> partitionsToUse = new HashSet<Integer>(localPartitions.size());
      for(Integer localPartition : localPartitions)
      {
        if(partitions.contains(localPartition))
          partitionsToUse.add(localPartition);
      }
      
      if(!localPartitions.isEmpty())
      {
        result.put(entry.getKey(), localPartitions);
      }
    }
    return result;
  }

  private <K, V> V lookup(NavigableMap<K, V> ring, K key)
  {
    V result = ring.get(key);
    if (result == null)
    {       // Not a direct match
      Map.Entry<K, V> entry = ring.ceilingEntry(key);
      result = (entry == null) ? ring.firstEntry().getValue() : entry.getValue();
    }

    return result;
  }
}
