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

type MyState struct {
	Count   int
	Message string
}

// ChatState 定义对话状态类型
type ChatState struct {
	Messages []llms.MessageContent
}

func main() {
	demo1()
}

func demo() {
	// g := graph.NewStateGraph[MyState]()
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
