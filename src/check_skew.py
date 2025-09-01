#!/usr/bin/env python3
import sys
from collections import defaultdict

def is_internal_index(index_name):
    return index_name.startswith('.')

def parse_shard_data(lines):
    processed_data = []
    unassigned_shards = []
    
    for line in lines:
        parts = [part for part in line.split() if part]
        if len(parts) >= 4:
            shard_info = {
                'index_name': parts[0],
                'shard_id': parts[1],
                'prirep': 'primary' if parts[2] == 'p' else 'replica',
                'state': parts[3],
            }
            
            if parts[3] == 'UNASSIGNED':
                unassigned_shards.append(shard_info)
            elif len(parts) >= 7:
                shard_info['node_ip'] = parts[-2]
                shard_info['node_name'] = parts[-1]
                processed_data.append(shard_info)
    
    return processed_data, unassigned_shards

def analyze_shards(processed_data):
    node_summary = {}
    index_summary = defaultdict(lambda: {
        'primaries': defaultdict(int),
        'replicas': defaultdict(int),
        'total_primaries': 0,
        'total_replicas': 0,
        'nodes': set()
    })
    
    for shard in processed_data:
        # Node-level analysis
        node_name = shard.get('node_name')
        if node_name and node_name not in node_summary:
            node_summary[node_name] = {
                'primary': 0,
                'replica': 0,
                'total': 0,
                'node_ip': shard['node_ip']
            }
        
        # Update node statistics
        if node_name:
            if shard['prirep'] == 'primary':
                node_summary[node_name]['primary'] += 1
            else:
                node_summary[node_name]['replica'] += 1
            node_summary[node_name]['total'] += 1
        
        # Index-level analysis
        index_name = shard['index_name']
        if shard['prirep'] == 'primary':
            index_summary[index_name]['primaries'][node_name] += 1
            index_summary[index_name]['total_primaries'] += 1
        else:
            index_summary[index_name]['replicas'][node_name] += 1
            index_summary[index_name]['total_replicas'] += 1
        
        if node_name:
            index_summary[index_name]['nodes'].add(node_name)
    
    return node_summary, index_summary

def analyze_index_distribution(index_summary):
    index_distribution = {}
    
    for index_name, stats in index_summary.items():
        if not is_internal_index(index_name):
            distribution = {
                'primary_distribution': dict(stats['primaries']),
                'replica_distribution': dict(stats['replicas']),
                'total_primaries': stats['total_primaries'],
                'total_replicas': stats['total_replicas'],
                'node_count': len(stats['nodes']),
                'is_balanced': True,
                'imbalance_details': []
            }
            
            # Check primary distribution
            primary_counts = list(stats['primaries'].values())
            if primary_counts:
                min_primary = min(primary_counts)
                max_primary = max(primary_counts)
                if max_primary - min_primary > 1:
                    distribution['is_balanced'] = False
                    distribution['imbalance_details'].append(
                        f"Primary shard imbalance: min={min_primary}, max={max_primary}"
                    )
            
            # Check replica distribution
            replica_counts = list(stats['replicas'].values())
            if replica_counts:
                min_replica = min(replica_counts)
                max_replica = max(replica_counts)
                if max_replica - min_replica > 1:
                    distribution['is_balanced'] = False
                    distribution['imbalance_details'].append(
                        f"Replica shard imbalance: min={min_replica}, max={max_replica}"
                    )
            
            index_distribution[index_name] = distribution
    
    return index_distribution

def get_cluster_summary(node_summary):
    summary = {
        'min_primaries': float('inf'),
        'max_primaries': 0,
        'min_replicas': float('inf'),
        'max_replicas': 0,
        'min_total_shards': float('inf'),
        'max_total_shards': 0,
        'nodes_with_min_primaries': [],
        'nodes_with_max_primaries': [],
        'nodes_with_min_replicas': [],
        'nodes_with_max_replicas': [],
        'nodes_with_min_shards': [],
        'nodes_with_max_shards': []
    }
    
    if not node_summary:  # Handle case when all shards are unassigned
        return summary
    
    for node, stats in node_summary.items():
        # Primaries
        if stats['primary'] < summary['min_primaries']:
            summary['min_primaries'] = stats['primary']
            summary['nodes_with_min_primaries'] = [node]
        elif stats['primary'] == summary['min_primaries']:
            summary['nodes_with_min_primaries'].append(node)
            
        if stats['primary'] > summary['max_primaries']:
            summary['max_primaries'] = stats['primary']
            summary['nodes_with_max_primaries'] = [node]
        elif stats['primary'] == summary['max_primaries']:
            summary['nodes_with_max_primaries'].append(node)
            
        # Replicas
        if stats['replica'] < summary['min_replicas']:
            summary['min_replicas'] = stats['replica']
            summary['nodes_with_min_replicas'] = [node]
        elif stats['replica'] == summary['min_replicas']:
            summary['nodes_with_min_replicas'].append(node)
            
        if stats['replica'] > summary['max_replicas']:
            summary['max_replicas'] = stats['replica']
            summary['nodes_with_max_replicas'] = [node]
        elif stats['replica'] == summary['max_replicas']:
            summary['nodes_with_max_replicas'].append(node)
            
        # Total shards
        if stats['total'] < summary['min_total_shards']:
            summary['min_total_shards'] = stats['total']
            summary['nodes_with_min_shards'] = [node]
        elif stats['total'] == summary['min_total_shards']:
            summary['nodes_with_min_shards'].append(node)
            
        if stats['total'] > summary['max_total_shards']:
            summary['max_total_shards'] = stats['total']
            summary['nodes_with_max_shards'] = [node]
        elif stats['total'] == summary['max_total_shards']:
            summary['nodes_with_max_shards'].append(node)
    
    return summary

def main():
    lines = sys.stdin.readlines()
    processed_data, unassigned_shards = parse_shard_data(lines)
    node_summary, index_summary = analyze_shards(processed_data)
    index_distribution = analyze_index_distribution(index_summary)
    cluster_summary = get_cluster_summary(node_summary)

    # Print unassigned shards
    if unassigned_shards:
        print("\nWARNING: Unassigned Shards Found!")
        print("-" * 50)
        for shard in unassigned_shards:
            print(f"Index: {shard['index_name']}, Shard: {shard['shard_id']}, Type: {shard['prirep']}")
        print(f"\nTotal unassigned shards: {len(unassigned_shards)}")

    # Print index-level distribution for non-internal indices
    print("\nIndex Distribution Analysis:")
    print("-" * 50)
    for index_name, dist in index_distribution.items():
        print(f"\nIndex: {index_name}")
        print(f"Total Primaries: {dist['total_primaries']}")
        print(f"Total Replicas: {dist['total_replicas']}")
        print(f"Number of Nodes: {dist['node_count']}")
        print("Primary Distribution:", dict(dist['primary_distribution']))
        print("Replica Distribution:", dict(dist['replica_distribution']))
        print(f"Balanced: {dist['is_balanced']}")
        if not dist['is_balanced']:
            print("Imbalance Details:")
            for detail in dist['imbalance_details']:
                print(f"  - {detail}")

    # Print node-level summary
    print("\nNode-level Summary:")
    print("-" * 50)
    for node, stats in node_summary.items():
        print(f"\nNode: {node}")
        print(f"IP: {stats['node_ip']}")
        print(f"Primary shards: {stats['primary']}")
        print(f"Replica shards: {stats['replica']}")
        print(f"Total shards: {stats['total']}")
    
    # Print cluster-level summary
    print("\nCluster Summary:")
    print("-" * 50)
    print(f"\nPrimary Shards:")
    print(f"  Min: {cluster_summary['min_primaries']} (Nodes: {', '.join(cluster_summary['nodes_with_min_primaries'])})")
    print(f"  Max: {cluster_summary['max_primaries']} (Nodes: {', '.join(cluster_summary['nodes_with_max_primaries'])})")

    print(f"\nReplica Shards:")
    print(f"  Min: {cluster_summary['min_replicas']} (Nodes: {', '.join(cluster_summary['nodes_with_min_replicas'])})")
    print(f"  Max: {cluster_summary['max_replicas']} (Nodes: {', '.join(cluster_summary['nodes_with_max_replicas'])})")

    print(f"\nTotal Shards:")
    print(f"  Min: {cluster_summary['min_total_shards']} (Nodes: {', '.join(cluster_summary['nodes_with_min_shards'])})")
    print(f"  Max: {cluster_summary['max_total_shards']} (Nodes: {', '.join(cluster_summary['nodes_with_max_shards'])})")
if __name__ == "__main__":
    main()
