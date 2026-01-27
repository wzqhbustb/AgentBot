package main

import (
	"bufio"
	"context"
	"fmt"
	"math/rand/v2"
	"ollama-demo/hnsw"
	"ollama-demo/tutor_agent"
	"os"
	"strings"

	"github.com/smallnest/langgraphgo/graph"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/openai"
)

type MyState struct {
	Count   int
	Message string
}

// ChatState 定义对话状态类型
type ChatState struct {
	Messages []llms.MessageContent
}

type AgentState struct {
}

func main() {
	// demo1()
	// page_rank.Verify()
	runTutorAgent()
	// lanceDBTest()
	// chroma_run.TestChromaConnection()
	// runHNSWDemo()
}

// 在 main.go 文件末尾添加

// runTutorAgent 运行智能助教
func runTutorAgent() {
	agent, err := tutor_agent.NewTutorAgent(tutor_agent.Ollama)
	if err != nil {
		fmt.Printf("❌ 初始化失败: %v\n", err)
		fmt.Println("请确保 Ollama 正在运行: ollama serve")
		return
	}

	if err := agent.Run(); err != nil {
		fmt.Printf("❌ 运行错误: %v\n", err)
	}
}

func demo() {
	g := graph.NewStateGraph[MyState]()
	// 节点函数自动推断类型
	g.AddNode("inc", "增加", func(ctx context.Context, state MyState) (MyState, error) {
		state.Count++ // 编译时类型检查，无需断言！
		return state, nil
	})
}

// original version
func demo1() {
	fmt.Println("=== Ollama DeepSeek 14B 对话 Demo ===")
	fmt.Println("提示：输入 'quit' 或 'exit' 退出")
	fmt.Println()

	// 配置 Ollama（OpenAI 兼容模式）
	// Ollama 默认在 localhost:11434 运行，OpenAI 兼容接口在 /v1
	model, err := openai.New(
		openai.WithBaseURL("http://localhost:11434/v1"),
		openai.WithModel("deepseek-r1:14b"),
		openai.WithToken("ollama"), // Ollama 不需要真实 token
	)
	if err != nil {
		fmt.Printf("❌ 创建模型失败: %v\n", err)
		fmt.Println("请确保 Ollama 正在运行: ollama serve")
		return
	}

	// 创建对话图（使用泛型）
	g := graph.NewStateGraph[ChatState]()

	// 添加对话节点
	g.AddNode("chat", "与 DeepSeek 对话", func(ctx context.Context, state ChatState) (ChatState, error) {
		messages := state.Messages

		fmt.Print("🤖 DeepSeek 思考中...")

		// 调用模型生成回复
		response, err := model.GenerateContent(ctx, messages,
			llms.WithTemperature(0.7),
			llms.WithMaxTokens(2000),
		)
		if err != nil {
			return ChatState{Messages: []llms.MessageContent{}}, err
		}

		fmt.Print("\r")

		// 提取回复内容
		aiResponse := response.Choices[0].Content

		// 返回更新后的状态
		newMessages := append(messages, llms.TextParts(llms.ChatMessageTypeAI, aiResponse))
		return ChatState{Messages: newMessages}, nil
	})

	// 设置图结构
	g.AddEdge("chat", graph.END)
	g.SetEntryPoint("chat")

	// 编译图
	runnable, err := g.Compile()
	if err != nil {
		panic(err)
	}

	// 初始化对话状态
	chatState := ChatState{
		Messages: []llms.MessageContent{},
	}

	// 交互式对话循环
	scanner := bufio.NewScanner(os.Stdin)
	ctx := context.Background()

	for {
		fmt.Print("👤 你: ")
		if !scanner.Scan() {
			break
		}

		userInput := strings.TrimSpace(scanner.Text())

		// 检查退出命令
		if userInput == "quit" || userInput == "exit" || userInput == "" {
			fmt.Println("👋 再见！")
			break
		}

		// 添加用户消息到状态
		chatState.Messages = append(chatState.Messages,
			llms.TextParts(llms.ChatMessageTypeHuman, userInput))

		// 执行对话
		result, err := runnable.Invoke(ctx, chatState)
		if err != nil {
			fmt.Printf("❌ 错误: %v\n", err)
			continue
		}

		// 更新对话状态
		chatState = result

		// 显示 AI 回复
		lastMessage := chatState.Messages[len(chatState.Messages)-1]
		if len(lastMessage.Parts) > 0 {
			if textPart, ok := lastMessage.Parts[0].(llms.TextContent); ok {
				fmt.Printf("🤖 DeepSeek: %s\n\n", textPart.Text)
			}
		}
	}
}

const (
	MaxIterations     = 30
	ConvergenceThresh = 0.0001
	DampingFactor     = 0.85
)

type SchemaTestState struct {
	Count int
	Logs  []string
	Max   int
}

func schemaDemo() {
	_ = graph.NewStructSchema[SchemaTestState](SchemaTestState{Count: 0, Max: 0},
		func(current, new SchemaTestState) (SchemaTestState, error) {
			// Define your own merge policy
			current.Count += new.Count
			current.Logs = append(current.Logs, new.Logs...)
			if new.Max > current.Max {
				current.Max = new.Max
			}
			return current, nil
		},
	)
}

// runHNSWDemo 演示 HNSW 向量搜索
func runHNSWDemo() {
	fmt.Println("=== HNSW 向量搜索演示 ===\n")

	// 1. 创建 HNSW 索引
	dimension := 128
	index := hnsw.NewHNSW(hnsw.Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      dimension,
		DistanceFunc:   hnsw.L2Distance, // 可选: L2Distance, InnerProductDistance, CosineDistance
	})
	fmt.Printf("✓ 创建索引 (M=%d, efConstruction=%d, dimension=%d)\n\n", 16, 200, dimension)

	// 2. 准备测试数据
	numVectors := 1000
	fmt.Printf("⏳ 插入 %d 个向量...\n", numVectors)

	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dimension)
		for j := 0; j < dimension; j++ {
			vec[j] = rand.Float32()*2 - 1 // [-1, 1) 随机值
		}
		vectors[i] = vec

		// 插入向量
		nodeID, err := index.Add(vec)
		if err != nil {
			fmt.Printf("❌ 插入失败: %v\n", err)
			return
		}

		if (i+1)%100 == 0 {
			fmt.Printf("  已插入 %d 个向量 (最新 nodeID=%d)\n", i+1, nodeID)
		}
	}
	fmt.Printf("✓ 插入完成！\n\n")

	// 3. 执行搜索
	fmt.Println("🔍 执行向量搜索...")

	// 使用第一个向量作为查询
	queryVector := vectors[0]
	k := 10  // 返回 top-10 结果
	ef := 50 // 搜索时的候选集大小

	results, err := index.Search(queryVector, k, ef)
	if err != nil {
		fmt.Printf("❌ 搜索失败: %v\n", err)
		return
	}

	fmt.Printf("\n查询向量: vectors[0]\n")
	fmt.Printf("返回 Top-%d 最近邻:\n\n", k)

	for i, item := range results {
		fmt.Printf("%2d. NodeID=%4d | 距离=%.6f\n", i+1, item.ID, item.Distance)
	}

	// 4. 测试不同的查询向量
	fmt.Println("\n🔍 随机查询测试...")
	randomQuery := make([]float32, dimension)
	for j := 0; j < dimension; j++ {
		randomQuery[j] = rand.Float32()*2 - 1
	}

	results, err = index.Search(randomQuery, 5, 100)
	if err != nil {
		fmt.Printf("❌ 搜索失败: %v\n", err)
		return
	}

	fmt.Printf("\n随机查询向量\n")
	fmt.Printf("返回 Top-5 最近邻:\n\n")

	for i, item := range results {
		fmt.Printf("%2d. NodeID=%4d | 距离=%.6f\n", i+1, item.ID, item.Distance)
	}

	// 5. 性能统计
	fmt.Println("\n📊 性能统计:")
	fmt.Printf("  索引大小: %d 个向量\n", numVectors)
	fmt.Printf("  向量维度: %d\n", dimension)
	fmt.Printf("  搜索返回: %d 个最近邻\n", k)
	fmt.Println("\n✅ 演示完成！")
}
