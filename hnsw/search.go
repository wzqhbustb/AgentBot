package hnsw

import "container/heap"

// 优先队列实现（最小堆）
type PriorityQueue []*Item

type Item struct {
	value    int     // 节点ID
	priority float32 // 距离（优先级）
	index    int     // 在堆中的索引
}

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority // 最小堆
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// 最大堆（用于维护结果集）
type MaxHeap []*Item

func (h MaxHeap) Len() int { return len(h) }

func (h MaxHeap) Less(i, j int) bool {
	return h[i].priority > h[j].priority // 最大堆
}

func (h MaxHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *MaxHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*Item)
	item.index = n
	*h = append(*h, item)
}

func (h *MaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

func (h *MaxHeap) Peek() interface{} {
	if len(*h) == 0 {
		return nil
	}
	return (*h)[0]
}

func (h *HNSWIndex) searchLayer(query []float32, ep int, ef int, level int) []SearchResult {
	visited := make(map[int]bool)

	// 候选集，最大堆，按距离从大到小
	candidates := &MaxHeap{}
	heap.Init(candidates)

	// 结果集，最大堆，按距离从大到小
	results := &MaxHeap{}
	heap.Init(results)

	// 计算入口点距离
	epDist := h.distFunc(query, h.nodes[ep].Vector())

	heap.Push(candidates, &Item{value: ep, priority: epDist})
	heap.Push(results, &Item{value: ep, priority: epDist})
	visited[ep] = true

	for candidates.Len() > 0 {
		// 取距离最近的候选点
		current := heap.Pop(candidates).(*Item)

		// 如果当前点比结果集中最远的点还远，停止搜索
		if results.Len() > 0 {
			furthest := results.Peek().(*Item)
			if current.priority > furthest.priority {
				break
			}
		}

		// 检查当前节点的所有邻居
		h.nodeLocks[current.value].RLock()
		neighbors := h.nodes[current.value].GetConnections(level)
		neighborsCopy := make([]int, len(neighbors))
		copy(neighborsCopy, neighbors)
		h.nodeLocks[current.value].RUnlock()

		for _, neighborID := range neighborsCopy {
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			// 计算距离
			dist := h.distFunc(query, h.nodes[neighborID].Vector())

			// 如果结果集未满，或者当前距离更近，添加到候选集
			if results.Len() < ef {
				heap.Push(candidates, &Item{value: neighborID, priority: dist})
				heap.Push(results, &Item{value: neighborID, priority: dist})
			} else {
				furthest := results.Peek().(*Item)
				if dist < furthest.priority {
					heap.Push(candidates, &Item{value: neighborID, priority: dist})
					heap.Push(results, &Item{value: neighborID, priority: dist})

					// 移除最远的点
					if results.Len() > ef {
						heap.Pop(results)
					}
				}
			}
		}
	}

	// 转换为结果数组（从近到远排序）
	resultArray := make([]SearchResult, results.Len())
	for i := results.Len() - 1; i >= 0; i-- {
		item := heap.Pop(results).(*Item)
		resultArray[i] = SearchResult{
			ID:       item.value,
			Distance: item.priority,
		}
	}

	return resultArray
}
