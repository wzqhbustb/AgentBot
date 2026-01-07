package page_rank

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/smallnest/langgraphgo/graph"
)

// PageRankState 存储PageRank计算的完整状态
type PageRankState struct {
	Iteration      int                 // 当前迭代次数
	Scores         map[string]float64  // 当前PageRank分数
	NewScores      map[string]float64  // 新计算的分数
	Graph          map[string][]string // 图的邻接表
	DampingFactor  float64             // 阻尼因子
	Converged      bool                // 是否收敛
	MaxIterations  int                 // 最大迭代次数
	Tolerance      float64             // 收敛阈值
	MaxDelta       float64             // 最大变化量
	StartTime      time.Time           // 开始时间
	IterationTimes []time.Duration     // 每次迭代耗时
}

func Verify() {
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║  PageRank Computation using LangGraphGo (Pregel-style)    ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")

	// 测试1: 简单4节点图
	graph1 := createSimpleGraph()
	runPageRankTest(graph1, "Simple 4-Node Graph", 0.85, 100, 0.0001)

	// 测试2: Star图
	graph2 := createStarGraph(10)
	runPageRankTest(graph2, "Star Graph (10 nodes)", 0.85, 100, 0.0001)

	// 测试3: Ring图
	graph3 := createRingGraph(20)
	runPageRankTest(graph3, "Ring Graph (20 nodes)", 0.85, 100, 0.0001)

	// 测试4: 复杂Web图
	graph4 := createComplexWebGraph(50)
	runPageRankTest(graph4, "Complex Web Graph (50 nodes)", 0.85, 100, 0.0001)
}

// 1. Initialize - 初始化所有节点的PageRank分数
func initialize(ctx context.Context, state PageRankState) (PageRankState, error) {
	numNodes := len(state.Graph)
	initialScore := 1.0 / float64(numNodes)

	state.Scores = make(map[string]float64)
	state.NewScores = make(map[string]float64)

	for node := range state.Graph {
		state.Scores[node] = initialScore
		state.NewScores[node] = initialScore
	}

	state.Iteration = 0
	state.Converged = false
	state.StartTime = time.Now()
	state.IterationTimes = []time.Duration{}

	fmt.Printf("├─ Initialized %d nodes with score %.4f\n", numNodes, initialScore)
	return state, nil
}

// 2. Compute - 计算新的PageRank分数
func compute(ctx context.Context, state PageRankState) (PageRankState, error) {
	iterStart := time.Now()

	numNodes := len(state.Graph)
	dampingValue := (1.0 - state.DampingFactor) / float64(numNodes)

	// 为每个节点计算新分数
	for node := range state.Graph {
		// 基础分数（随机跳转部分）
		newScore := dampingValue

		// 累加所有指向该节点的分数贡献
		for source, targets := range state.Graph {
			// 检查source是否指向node
			for _, target := range targets {
				if target == node {
					// source贡献的分数 = source的当前分数 / source的出度
					outDegree := len(state.Graph[source])
					if outDegree > 0 {
						contribution := state.Scores[source] / float64(outDegree)
						newScore += state.DampingFactor * contribution
					}
					break
				}
			}
		}

		state.NewScores[node] = newScore
	}

	// 计算最大变化量
	state.MaxDelta = 0.0
	for node := range state.Graph {
		delta := math.Abs(state.NewScores[node] - state.Scores[node])
		if delta > state.MaxDelta {
			state.MaxDelta = delta
		}
	}

	iterDuration := time.Since(iterStart)
	state.IterationTimes = append(state.IterationTimes, iterDuration)

	state.Iteration++
	fmt.Printf("├─ Iteration %2d: MaxDelta=%.6f, Time=%v\n",
		state.Iteration, state.MaxDelta, iterDuration)

	return state, nil
}

// 3. Update - 更新分数（准备下一轮迭代）
func update(ctx context.Context, state PageRankState) (PageRankState, error) {
	// 将NewScores复制到Scores
	for node, score := range state.NewScores {
		state.Scores[node] = score
	}
	return state, nil
}

// 4. CheckConvergence - 检查是否收敛
func checkConvergence(ctx context.Context, state PageRankState) string {
	// 检查收敛条件
	if state.MaxDelta < state.Tolerance {
		state.Converged = true
		fmt.Printf("├─ ✓ Converged at iteration %d (delta=%.6f)\n",
			state.Iteration, state.MaxDelta)
		return graph.END
	}

	// 检查最大迭代次数
	if state.Iteration >= state.MaxIterations {
		state.Converged = true
		fmt.Printf("├─ ⚠ Reached max iterations (%d)\n", state.MaxIterations)
		return graph.END
	}

	// 继续迭代
	return "update"
}

// 创建PageRank计算图
func createPageRankGraph() (*graph.StateRunnable[PageRankState], error) {
	g := graph.NewStateGraph[PageRankState]()

	// 添加节点
	g.AddNode("initialize", "Initialize PageRank scores", initialize)
	g.AddNode("compute", "Compute new PageRank scores", compute)
	g.AddNode("update", "Update scores for next iteration", update)

	// 设置边
	g.SetEntryPoint("initialize")
	g.AddEdge("initialize", "compute")
	g.AddEdge("update", "compute")

	// 添加条件边：compute后检查是否收敛
	g.AddConditionalEdge("compute", checkConvergence)

	return g.Compile()
}

// 运行PageRank测试
func runPageRankTest(graphData map[string][]string, testName string, dampingFactor float64, maxIter int, tolerance float64) {
	fmt.Printf("\n╔═══ Test Case: %s ═══╗\n", testName)

	// 创建初始状态
	initialState := PageRankState{
		Graph:         graphData,
		DampingFactor: dampingFactor,
		MaxIterations: maxIter,
		Tolerance:     tolerance,
	}

	// 创建并运行图
	app, err := createPageRankGraph()
	if err != nil {
		fmt.Printf("Error creating graph: %v\n", err)
		return
	}

	ctx := context.Background()
	finalState, err := app.Invoke(ctx, initialState)
	if err != nil {
		fmt.Printf("Error running graph: %v\n", err)
		return
	}

	// 输出结果
	totalTime := time.Since(finalState.StartTime)
	avgTime := totalTime / time.Duration(finalState.Iteration)

	fmt.Println("├─────────────────────────────────────────────")
	fmt.Println("├─ Computation Complete!")
	fmt.Printf("├─ Total Iterations: %d\n", finalState.Iteration)
	fmt.Printf("├─ Total Time: %v\n", totalTime)
	fmt.Printf("├─ Avg Time/Iteration: %v\n", avgTime)
	fmt.Println("├─────────────────────────────────────────────")

	// 排序并输出Top节点
	type NodeScore struct {
		Node  string
		Score float64
	}
	var scores []NodeScore
	for node, score := range finalState.Scores {
		scores = append(scores, NodeScore{node, score})
	}
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].Score > scores[j].Score
	})

	fmt.Println("├─ Top Nodes by PageRank:")
	displayCount := 10
	if len(scores) < displayCount {
		displayCount = len(scores)
	}
	for i := 0; i < displayCount; i++ {
		fmt.Printf("├─  %3d. Node %-10s: %.6f\n", i+1, scores[i].Node, scores[i].Score)
	}
	fmt.Println("└─────────────────────────────────────────────")

	// 验证结果
	verifyPageRank(finalState.Scores, finalState.Graph)
}

func verifyPageRank(scores map[string]float64, graphData map[string][]string) {
	fmt.Println("\n┌─ Verification ─────────────────────────────┐")

	// 检查1: 分数总和
	sum := 0.0
	for _, score := range scores {
		sum += score
	}
	fmt.Printf("│ Sum of PageRank scores: %.6f\n", sum)
	if math.Abs(sum-1.0) < 0.001 {
		fmt.Println("│ ✓ Sum verification PASSED")
	} else {
		fmt.Printf("│ ✗ Sum verification FAILED (expected 1.0, got %.6f)\n", sum)

		// 诊断信息：检查是否有叶子节点
		danglingNodes := 0
		for node, targets := range graphData {
			if len(targets) == 0 {
				danglingNodes++
				fmt.Printf("│   ⚠ Dangling node found: %s (out-degree=0)\n", node)
			}
		}
		if danglingNodes > 0 {
			fmt.Printf("│   💡 Found %d dangling nodes causing score leakage\n", danglingNodes)
		}
	}

	// 检查2: 正数检查
	allPositive := true
	for node, score := range scores {
		if score <= 0 {
			fmt.Printf("│ ✗ Node %s has non-positive score: %.6f\n", node, score)
			allPositive = false
		}
	}
	if allPositive {
		fmt.Println("│ ✓ Positivity check PASSED")
	}

	// 检查3: 分数多样性（改进：考虑对称图）
	firstScore := -1.0
	allSame := true
	for _, score := range scores {
		if firstScore < 0 {
			firstScore = score
		} else if math.Abs(score-firstScore) > 0.0001 {
			allSame = false
			break
		}
	}

	if allSame && len(scores) > 1 {
		// 检查图是否对称
		isSymmetric := checkGraphSymmetry(graphData)
		if isSymmetric {
			fmt.Println("│ ✓ All scores identical (graph is symmetric)")
		} else {
			fmt.Println("│ ⚠ WARNING: All scores are identical but graph is NOT symmetric!")
			fmt.Println("│   This suggests PageRank didn't run properly.")
		}
	} else {
		fmt.Println("│ ✓ Score diversity check PASSED")
	}

	fmt.Println("└────────────────────────────────────────────┘")
}

// 检查图是否对称（所有节点入度出度相等）
func checkGraphSymmetry(graphData map[string][]string) bool {
	inDegree := make(map[string]int)
	outDegree := make(map[string]int)

	for node, targets := range graphData {
		outDegree[node] = len(targets)
		for _, target := range targets {
			inDegree[target]++
		}
	}

	for node := range graphData {
		if inDegree[node] != outDegree[node] {
			return false
		}
	}
	return true
}

// 测试图生成函数
func createSimpleGraph() map[string][]string {
	return map[string][]string{
		"A": {"B", "C"},
		"B": {"C"},
		"C": {"A"},
		"D": {"C"},
	}
}

func createStarGraph(n int) map[string][]string {
	graph := make(map[string][]string)

	// 中心节点指向所有外围节点
	centerTargets := []string{}
	for i := 1; i < n; i++ {
		centerTargets = append(centerTargets, fmt.Sprintf("Node%d", i))
	}
	graph["Center"] = centerTargets

	// 每个外围节点也指向中心（形成双向）
	for i := 1; i < n; i++ {
		nodeName := fmt.Sprintf("Node%d", i)
		graph[nodeName] = []string{"Center"}
	}

	return graph
}

func createRingGraph(n int) map[string][]string {
	graph := make(map[string][]string)

	for i := 0; i < n; i++ {
		nodeName := fmt.Sprintf("Node%d", i)
		nextNode := fmt.Sprintf("Node%d", (i+1)%n)
		graph[nodeName] = []string{nextNode}
	}

	return graph
}

func createComplexWebGraph(n int) map[string][]string {
	graph := make(map[string][]string)

	for i := 0; i < n; i++ {
		nodeName := fmt.Sprintf("Page%d", i)
		graph[nodeName] = []string{}

		// 每个节点随机连接到2-5个其他节点
		numLinks := 2 + (i % 4)
		for j := 0; j < numLinks; j++ {
			targetIdx := (i + j + 1) % n
			targetNode := fmt.Sprintf("Page%d", targetIdx)
			graph[nodeName] = append(graph[nodeName], targetNode)
		}
	}

	return graph
}
