#!/usr/bin/env python3
import sys

def parse_shard_data(lines):
    processed_data = []
    
    for line in lines:
        parts = [part for part in line.split() if part]
        if len(parts) >= 7:
            shard_info = {
                'index_name': parts[0],
                'shard_id': parts[1],
                'prirep': 'primary' if parts[2] == 'p' else 'replica',
                'state': parts[3],
                'node_ip': parts[-2],
                'node_name': parts[-1]
            }
            processed_data.append(shard_info)
    
    return processed_data

def analyze_shards(processed_data):
    node_summary = {}
    for shard in processed_data:
        node_name = shard['node_name']
        if node_name not in node_summary:
            node_summary[node_name] = {
                'primary': 0,
                'replica': 0,
                'total': 0,
                'node_ip': shard['node_ip']
            }
        
        if shard['prirep'] == 'primary':
            node_summary[node_name]['primary'] += 1
        else:
            node_summary[node_name]['replica'] += 1
        node_summary[node_name]['total'] += 1
    
    return node_summary

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
    # Read from stdin
    lines = sys.stdin.readlines()
    
    # Process the data
    processed_data = parse_shard_data(lines)
    node_summary = analyze_shards(processed_data)
    cluster_summary = get_cluster_summary(node_summary)

    # Print node-level distribution
    print("\nShard distribution per node:")
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
