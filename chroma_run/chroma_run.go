package chroma_run

import (
	"context"
	"fmt"
	"log"

	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/llms/openai"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores/chroma"
)

func TestChromaConnection() {
	fmt.Println("🧪 === Chroma 连接测试 ===\n")

	ctx := context.Background()

	// 1. 创建 Ollama LLM
	fmt.Println("📡 步骤 1: 创建 Embedder...")

	ollamaLLM, err := openai.New(
		openai.WithBaseURL("http://localhost:11434/v1"),
		openai.WithToken("ollama"),
		openai.WithModel("nomic-embed-text"),
		openai.WithEmbeddingModel("nomic-embed-text"),
	)
	if err != nil {
		log.Fatalf("❌ 创建 LLM 失败: %v", err)
	}

	// 2. 包装成 Embedder
	embedder, err := embeddings.NewEmbedder(ollamaLLM)
	if err != nil {
		log.Fatalf("❌ 创建 Embedder 失败: %v", err)
	}
	fmt.Println("✅ Embedder 创建成功")

	// 3. 连接 Chroma
	fmt.Println("\n📡 步骤 2: 连接 Chroma 服务...")
	store, err := chroma.New(
		chroma.WithChromaURL("http://localhost:8000"),
		chroma.WithEmbedder(embedder),
		chroma.WithNameSpace("test_collection_v3"),
	)
	if err != nil {
		log.Fatalf("❌ 连接 Chroma 失败: %v\n", err)
	}
	fmt.Println("✅ Chroma 连接成功")

	// 4. 写入测试数据
	fmt.Println("\n📝 步骤 3: 写入测试文档...")
	testDocs := []schema.Document{
		{
			PageContent: "Go 是 Google 开发的编程语言，以并发和简洁著称。",
			Metadata: map[string]any{
				"source":   "test1.txt",
				"category": "programming",
			},
		},
		{
			PageContent: "Python 是一种高级编程语言，广泛用于数据科学和机器学习。",
			Metadata: map[string]any{
				"source":   "test2.txt",
				"category": "programming",
			},
		},
		{
			PageContent: "向量数据库用于存储和检索高维向量，支持语义搜索。",
			Metadata: map[string]any{
				"source":   "test3.txt",
				"category": "database",
			},
		},
	}

	ids, err := store.AddDocuments(ctx, testDocs)
	if err != nil {
		log.Fatalf("❌ 写入文档失败: %v", err)
	}
	fmt.Printf("✅ 成功写入 %d 个文档\n", len(testDocs))
	fmt.Printf("   文档 IDs: %v\n", ids)

	// 5. 读取测试 - 相似度搜索
	fmt.Println("\n🔍 步骤 4: 测试相似度搜索...")
	query := "什么是编程语言？"
	fmt.Printf("查询: \"%s\"\n\n", query)

	results, err := store.SimilaritySearch(ctx, query, 2)
	if err != nil {
		log.Fatalf("❌ 搜索失败: %v", err)
	}

	fmt.Printf("🎯 找到 %d 个相关结果:\n", len(results))
	for i, doc := range results {
		fmt.Printf("\n结果 %d:\n", i+1)
		fmt.Printf("  内容: %s\n", doc.PageContent)
		if source, ok := doc.Metadata["source"]; ok {
			fmt.Printf("  来源: %s\n", source)
		}
		if category, ok := doc.Metadata["category"]; ok {
			fmt.Printf("  分类: %s\n", category)
		}
	}

	fmt.Println("\n✅ === 所有测试通过！===")
}
