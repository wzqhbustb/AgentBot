package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/smallnest/langgraphgo/graph"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/openai"
)

func main() {
	demo1()
	// demo4()
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

	// 创建对话图
	g := graph.NewStateGraph()

	// 添加对话节点
	g.AddNode("chat", "与 DeepSeek 对话", func(ctx context.Context, state interface{}) (interface{}, error) {
		messages := state.([]llms.MessageContent)

		fmt.Print("🤖 DeepSeek 思考中...")

		// 调用模型生成回复
		response, err := model.GenerateContent(ctx, messages,
			llms.WithTemperature(0.7),
			llms.WithMaxTokens(2000),
		)
		if err != nil {
			return nil, fmt.Errorf("生成失败: %w", err)
		}

		fmt.Print("\r")

		// 提取回复内容
		aiResponse := response.Choices[0].Content

		// 返回更新后的消息列表
		return append(messages, llms.TextParts(llms.ChatMessageTypeAI, aiResponse)), nil
	})

	// 设置图结构
	g.AddEdge("chat", graph.END)
	g.SetEntryPoint("chat")

	// 编译图
	runnable, err := g.Compile()
	if err != nil {
		panic(err)
	}

	// 对话历史
	var conversationHistory []llms.MessageContent

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

		// 添加用户消息到历史
		conversationHistory = append(conversationHistory,
			llms.TextParts(llms.ChatMessageTypeHuman, userInput))

		// 执行对话
		result, err := runnable.Invoke(ctx, conversationHistory)
		if err != nil {
			fmt.Printf("❌ 错误: %v\n", err)
			continue
		}

		// 更新对话历史
		conversationHistory = result.([]llms.MessageContent)

		// 显示 AI 回复
		lastMessage := conversationHistory[len(conversationHistory)-1]
		if len(lastMessage.Parts) > 0 {
			if textPart, ok := lastMessage.Parts[0].(llms.TextContent); ok {
				fmt.Printf("🤖 DeepSeek: %s\n\n", textPart.Text)
			}
		}
	}
}

// improved version with ChatAgent
// func demo2() {
// 	fmt.Println("=== Ollama DeepSeek 14B ChatAgent Demo ===")
// 	fmt.Println("提示：输入 'quit' 退出，'history' 查看历史，'clear' 清除历史")
// 	fmt.Println()

// 	// 配置 Ollama
// 	llm, err := openai.New(
// 		openai.WithBaseURL("http://localhost:11434/v1"),
// 		openai.WithModel("deepseek-r1:14b"),
// 		openai.WithToken("ollama"),
// 	)
// 	if err != nil {
// 		fmt.Printf("❌ 创建模型失败: %v\n", err)
// 		return
// 	}

// 	// 创建 ChatAgent（自动管理对话历史）
// 	agent, err := prebuilt.NewChatAgent(llm, nil)
// 	if err != nil {
// 		fmt.Printf("❌ 创建 Agent 失败: %v\n", err)
// 		return
// 	}

// 	fmt.Printf("📝 会话 ID: %s\n\n", agent.ThreadID())

// 	// 交互循环
// 	scanner := bufio.NewScanner(os.Stdin)
// 	ctx := context.Background()

// 	for {
// 		fmt.Print("👤 你: ")
// 		if !scanner.Scan() {
// 			break
// 		}

// 		input := strings.TrimSpace(scanner.Text())

// 		switch input {
// 		case "quit", "exit", "":
// 			fmt.Println("👋 再见！")
// 			return

// 		case "history":
// 			// 显示对话历史
// 			history := agent.GetHistory()
// 			fmt.Println("\n📜 对话历史:")
// 			for i, msg := range history {
// 				role := "未知"
// 				if msg.Role == "human" {
// 					role = "用户"
// 				} else if msg.Role == "ai" {
// 					role = "AI"
// 				}
// 				fmt.Printf("  %d. [%s]: %v\n", i+1, role, msg.Parts)
// 			}
// 			fmt.Println()
// 			continue

// 		case "clear":
// 			agent.ClearHistory()
// 			fmt.Println("✅ 历史已清除\n")
// 			continue
// 		}

// 		// 发送消息并获取回复
// 		fmt.Print("🤖 DeepSeek 思考中...")
// 		response, err := agent.Chat(ctx, input)
// 		if err != nil {
// 			fmt.Printf("\n❌ 错误: %v\n\n", err)
// 			continue
// 		}

// 		fmt.Printf("\r🤖 DeepSeek: %s\n\n", response)
// 	}
// }

func demo3() {
	fmt.Println("=== Ollama DeepSeek 流式对话 Demo ===")
	fmt.Println("提示：输入 'quit' 退出")
	fmt.Println()

	// 配置模型
	model, err := openai.New(
		openai.WithBaseURL("http://localhost:11434/v1"),
		openai.WithModel("deepseek-r1:14b"),
		openai.WithToken("ollama"),
	)
	if err != nil {
		fmt.Printf("❌ 错误: %v\n", err)
		return
	}

	// 创建图
	g := graph.NewStateGraph()

	g.AddNode("chat", "对话", func(ctx context.Context, state interface{}) (interface{}, error) {
		messages := state.([]llms.MessageContent)

		// 使用流式 API
		fmt.Print("🤖 DeepSeek: ")

		var fullResponse strings.Builder
		_, err := model.GenerateContent(ctx, messages,
			llms.WithTemperature(0.7),
			llms.WithMaxTokens(2000),
			llms.WithStreamingFunc(func(ctx context.Context, chunk []byte) error {
				text := string(chunk)
				fullResponse.WriteString(text)
				fmt.Print(text) // 实时打印
				return nil
			}),
		)

		fmt.Println() // 换行

		if err != nil {
			return nil, err
		}

		return append(messages, llms.TextParts(llms.ChatMessageTypeAI, fullResponse.String())), nil
	})

	g.AddEdge("chat", graph.END)
	g.SetEntryPoint("chat")

	runnable, _ := g.Compile()

	// 对话循环
	scanner := bufio.NewScanner(os.Stdin)
	ctx := context.Background()
	var history []llms.MessageContent

	for {
		fmt.Print("👤 你: ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "quit" || input == "exit" || input == "" {
			break
		}

		history = append(history, llms.TextParts(llms.ChatMessageTypeHuman, input))

		result, err := runnable.Invoke(ctx, history)
		if err != nil {
			fmt.Printf("❌ 错误: %v\n", err)
			continue
		}

		history = result.([]llms.MessageContent)
		fmt.Println()
	}

	fmt.Println("👋 再见！")
}

func demo4() {
	g := graph.NewMessageGraph()

	// 添加一个简单的处理节点
	g.AddNode("process", "chat", func(ctx context.Context, state interface{}) (interface{}, error) {
		// MessageGraph 需要 state 是 map[string]any 类型
		stateMap := state.(map[string]any)

		// 获取输入消息
		input, ok := stateMap["input"].(string)
		if !ok {
			return nil, fmt.Errorf("input not found or not a string")
		}

		// 处理消息
		output := fmt.Sprintf("PROCESSED_%s", input)

		// 返回更新后的状态
		stateMap["output"] = output
		return stateMap, nil
	})

	// 设置边：从 process 节点到 END 结束点
	g.AddEdge("process", graph.END)
	// 设置入口点
	g.SetEntryPoint("process")

	// 编译图以获得可执行实例
	runnable, err := g.Compile()
	if err != nil {
		panic(err)
	}

	// 使用初始状态调用图 - 必须是 map[string]any
	initialState := map[string]any{
		"input": "hello_world",
	}

	result, err := runnable.Invoke(context.Background(), initialState)
	if err != nil {
		panic(err)
	}

	resultMap := result.(map[string]any)
	fmt.Printf("结果: %v\n", resultMap["output"])
}
