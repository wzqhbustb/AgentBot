package hnsw

import "container/heap"

// insert handles the insertion of a new node into the HNSW index.
func (h *HNSWIndex) insert(newNode *Node) {
	h.globalLock.RLock()
	ep := int(h.entryPoint)
	maxLvl := int(h.maxLevel)
	h.globalLock.RUnlock()

	newNodeLevel := newNode.Level()
	newNodeID := newNode.ID()

	currentNearest := ep
	for lc := maxLvl; lc > newNodeLevel; lc-- {
		// todo
	}

	// todo
}

// selectNeighborsHeuristic 启发式选择邻居
// 实现算法4：SELECT-NEIGHBORS-HEURISTIC
func (h *HNSWIndex) selectNeighborsHeuristic(query []float32, candidates []SearchResult, m int) []SearchResult {
	if len(candidates) <= m {
		return candidates
	}

	// 使用简单策略：选择距离最近的 m 个
	// TODO: 实现完整的启发式剪枝（考虑邻居间的距离）

	// 创建最小堆
	pq := &PriorityQueue{}
	heap.Init(pq)

	for _, c := range candidates {
		heap.Push(pq, &Item{
			value:    c.ID,
			priority: c.Distance,
		})
	}

	result := make([]SearchResult, 0, m)
	for i := 0; i < m && pq.Len() > 0; i++ {
		item := heap.Pop(pq).(*Item)
		result = append(result, SearchResult{
			ID:       item.value,
			Distance: item.priority,
		})
	}

	return result
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
