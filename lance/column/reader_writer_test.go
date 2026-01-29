package column

import (
	"fmt"
	"ollama-demo/lance/arrow"
	"os"
	"path/filepath"
	"testing"
)

// ====================
// PageWriter/PageReader Tests
// ====================

func TestPageWriterReader_Int32Array(t *testing.T) {
	tests := []struct {
		name   string
		values []int32
		nulls  []bool
	}{
		{
			name:   "no nulls",
			values: []int32{1, 2, 3, 4, 5},
			nulls:  nil,
		},
		{
			name:   "with nulls",
			values: []int32{1, 0, 3, 0, 5},
			nulls:  []bool{true, false, true, false, true},
		},
		{
			name:   "all nulls",
			values: []int32{0, 0, 0},
			nulls:  []bool{false, false, false},
		},
		{
			name:   "single value",
			values: []int32{42},
			nulls:  nil,
		},
		{
			name:   "non-8-multiple length",
			values: []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			nulls:  []bool{true, false, true, false, true, false, true, false, true, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build array
			builder := arrow.NewInt32Builder()
			defer builder.Release()

			for i, v := range tt.values {
				if tt.nulls != nil && !tt.nulls[i] {
					builder.AppendNull()
				} else {
					builder.Append(v)
				}
			}
			originalArray := builder.NewArray()

			// Write to page
			writer := NewPageWriter(DefaultSerializationOptions())
			pages, err := writer.WritePages(originalArray, 0)
			if err != nil {
				t.Fatalf("WritePages failed: %v", err)
			}

			if len(pages) != 1 {
				t.Fatalf("expected 1 page, got %d", len(pages))
			}

			// Read from page
			reader := NewPageReader()
			resultArray, err := reader.ReadPage(pages[0], arrow.PrimInt32())
			if err != nil {
				t.Fatalf("ReadPage failed: %v", err)
			}

			// Verify
			if !arraysEqual(originalArray, resultArray) {
				t.Errorf("arrays not equal after roundtrip")
			}
		})
	}
}

func TestPageWriterReader_Int64Array(t *testing.T) {
	builder := arrow.NewInt64Builder()
	defer builder.Release()

	values := []int64{100, 200, 300, 400, 500}
	nulls := []bool{true, false, true, false, true}

	for i, v := range values {
		if nulls[i] {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	}

	originalArray := builder.NewArray()

	// Roundtrip
	writer := NewPageWriter(DefaultSerializationOptions())
	pages, err := writer.WritePages(originalArray, 0)
	if err != nil {
		t.Fatalf("WritePages failed: %v", err)
	}

	reader := NewPageReader()
	resultArray, err := reader.ReadPage(pages[0], arrow.PrimInt64())
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	if !arraysEqual(originalArray, resultArray) {
		t.Errorf("arrays not equal after roundtrip")
	}
}

func TestPageWriterReader_Float32Array(t *testing.T) {
	builder := arrow.NewFloat32Builder()
	defer builder.Release()

	values := []float32{1.1, 2.2, 3.3, 4.4, 5.5}
	for _, v := range values {
		builder.Append(v)
	}

	originalArray := builder.NewArray()

	// Roundtrip
	writer := NewPageWriter(DefaultSerializationOptions())
	pages, err := writer.WritePages(originalArray, 0)
	if err != nil {
		t.Fatalf("WritePages failed: %v", err)
	}

	reader := NewPageReader()
	resultArray, err := reader.ReadPage(pages[0], arrow.PrimFloat32())
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	if !arraysEqual(originalArray, resultArray) {
		t.Errorf("arrays not equal after roundtrip")
	}
}

func TestPageWriterReader_Float64Array(t *testing.T) {
	builder := arrow.NewFloat64Builder()
	defer builder.Release()

	values := []float64{1.111, 2.222, 3.333}
	nulls := []bool{true, false, true}

	for i, v := range values {
		if nulls[i] {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	}

	originalArray := builder.NewArray()

	// Roundtrip
	writer := NewPageWriter(DefaultSerializationOptions())
	pages, err := writer.WritePages(originalArray, 0)
	if err != nil {
		t.Fatalf("WritePages failed: %v", err)
	}

	reader := NewPageReader()
	resultArray, err := reader.ReadPage(pages[0], arrow.PrimFloat64())
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	if !arraysEqual(originalArray, resultArray) {
		t.Errorf("arrays not equal after roundtrip")
	}
}

func TestPageWriterReader_FixedSizeListArray(t *testing.T) {
	// Create 768-dimensional vectors for HNSW
	dim := 768
	numVectors := 3

	// Build child array (768 * 3 = 2304 float32 values)
	childBuilder := arrow.NewFloat32Builder()
	defer childBuilder.Release()

	for i := 0; i < numVectors*dim; i++ {
		childBuilder.Append(float32(i) * 0.1)
	}
	childArray := childBuilder.NewArray()

	// Create FixedSizeList array
	listType := arrow.FixedSizeListOf(arrow.PrimFloat32(), dim)
	originalArray := arrow.NewFixedSizeListArray(listType.(*arrow.FixedSizeListType), childArray, nil)

	// Roundtrip
	writer := NewPageWriter(DefaultSerializationOptions())
	pages, err := writer.WritePages(originalArray, 0)
	if err != nil {
		t.Fatalf("WritePages failed: %v", err)
	}

	reader := NewPageReader()
	resultArray, err := reader.ReadPage(pages[0], listType)
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	if !arraysEqual(originalArray, resultArray) {
		t.Errorf("arrays not equal after roundtrip")
	}
}

func TestPageWriterReader_FixedSizeListArray_WithNulls(t *testing.T) {
	dim := 128
	numVectors := 5

	// Build child array
	childBuilder := arrow.NewFloat32Builder()
	defer childBuilder.Release()

	for i := 0; i < numVectors*dim; i++ {
		childBuilder.Append(float32(i))
	}
	childArray := childBuilder.NewArray()

	// Create null bitmap
	nullBitmap := arrow.NewBitmap(numVectors)
	nullBitmap.Set(0) // valid
	nullBitmap.Set(2) // valid
	nullBitmap.Set(4) // valid
	// indices 1, 3 are null

	listType := arrow.FixedSizeListOf(arrow.PrimFloat32(), dim)
	originalArray := arrow.NewFixedSizeListArray(listType.(*arrow.FixedSizeListType), childArray, nullBitmap)

	// Roundtrip
	writer := NewPageWriter(DefaultSerializationOptions())
	pages, err := writer.WritePages(originalArray, 0)
	if err != nil {
		t.Fatalf("WritePages failed: %v", err)
	}

	reader := NewPageReader()
	resultArray, err := reader.ReadPage(pages[0], listType)
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	if !arraysEqual(originalArray, resultArray) {
		t.Errorf("arrays not equal after roundtrip")
	}
}

// ====================
// Writer/Reader Integration Tests
// ====================

func TestWriterReader_SingleRecordBatch(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test.lance")

	// Create schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimInt32(), Nullable: false},
		{Name: "value", Type: arrow.PrimFloat32(), Nullable: true},
	}, nil)

	// Build data
	idBuilder := arrow.NewInt32Builder()
	valueBuilder := arrow.NewFloat32Builder()
	defer idBuilder.Release()
	defer valueBuilder.Release()

	for i := 0; i < 100; i++ {
		idBuilder.Append(int32(i))
		if i%10 == 0 {
			valueBuilder.AppendNull()
		} else {
			valueBuilder.Append(float32(i) * 1.5)
		}
	}

	idArray := idBuilder.NewArray()
	valueArray := valueBuilder.NewArray()

	batch, err := arrow.NewRecordBatch(schema, 100, []arrow.Array{idArray, valueArray})
	if err != nil {
		t.Fatalf("NewRecordBatch failed: %v", err)
	}

	// Write
	writer, err := NewWriter(filename, schema, DefaultSerializationOptions())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("WriteRecordBatch failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer failed: %v", err)
	}

	// Read
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	// Verify schema
	if !reader.Schema().Equal(schema) {
		t.Errorf("schema mismatch")
	}

	// Verify num rows
	if reader.NumRows() != 100 {
		t.Errorf("expected 100 rows, got %d", reader.NumRows())
	}

	// Read data
	resultBatch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	if resultBatch.NumRows() != 100 {
		t.Errorf("expected 100 rows in result, got %d", resultBatch.NumRows())
	}

	// Verify columns
	if !arraysEqual(idArray, resultBatch.Column(0)) {
		t.Errorf("id column mismatch")
	}
	if !arraysEqual(valueArray, resultBatch.Column(1)) {
		t.Errorf("value column mismatch")
	}
}

func TestWriterReader_MultipleRecordBatches(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_multi.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "counter", Type: arrow.PrimInt64(), Nullable: false},
	}, nil)

	// Write
	writer, err := NewWriter(filename, schema, DefaultSerializationOptions())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	totalRows := 0
	var allValues []int64

	// Write 5 batches
	for batchNum := 0; batchNum < 5; batchNum++ {
		builder := arrow.NewInt64Builder()
		for i := 0; i < 50; i++ {
			val := int64(batchNum*50 + i)
			builder.Append(val)
			allValues = append(allValues, val)
		}
		array := builder.NewArray()
		builder.Release()

		batch, err := arrow.NewRecordBatch(schema, 50, []arrow.Array{array})
		if err != nil {
			t.Fatalf("NewRecordBatch failed: %v", err)
		}

		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("WriteRecordBatch %d failed: %v", batchNum, err)
		}

		totalRows += 50
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer failed: %v", err)
	}

	// Read
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	if reader.NumRows() != int64(totalRows) {
		t.Errorf("expected %d rows, got %d", totalRows, reader.NumRows())
	}

	resultBatch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	// Verify all values
	resultArray := resultBatch.Column(0).(*arrow.Int64Array)
	for i, expected := range allValues {
		if resultArray.Value(i) != expected {
			t.Errorf("value mismatch at index %d: expected %d, got %d", i, expected, resultArray.Value(i))
		}
	}
}

func TestWriterReader_VectorColumn(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_vectors.lance")

	// Create schema with 768-dim vectors
	dim := 768
	listType := arrow.FixedSizeListOf(arrow.PrimFloat32(), dim)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector_id", Type: arrow.PrimInt32(), Nullable: false},
		{Name: "embedding", Type: listType, Nullable: false},
	}, nil)

	// Build data
	numVectors := 10
	idBuilder := arrow.NewInt32Builder()
	childBuilder := arrow.NewFloat32Builder()
	defer idBuilder.Release()
	defer childBuilder.Release()

	for i := 0; i < numVectors; i++ {
		idBuilder.Append(int32(i))
		for d := 0; d < dim; d++ {
			childBuilder.Append(float32(i*dim+d) * 0.001)
		}
	}

	idArray := idBuilder.NewArray()
	childArray := childBuilder.NewArray()
	vectorArray := arrow.NewFixedSizeListArray(listType.(*arrow.FixedSizeListType), childArray, nil)

	batch, err := arrow.NewRecordBatch(schema, numVectors, []arrow.Array{idArray, vectorArray})
	if err != nil {
		t.Fatalf("NewRecordBatch failed: %v", err)
	}

	// Write
	writer, err := NewWriter(filename, schema, DefaultSerializationOptions())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("WriteRecordBatch failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer failed: %v", err)
	}

	// Read
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	resultBatch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	// Verify
	if !arraysEqual(idArray, resultBatch.Column(0)) {
		t.Errorf("id column mismatch")
	}
	if !arraysEqual(vectorArray, resultBatch.Column(1)) {
		t.Errorf("vector column mismatch")
	}
}

// ====================
// Multi-Page Tests
// ====================

func TestWriterReader_MultiPageColumn(t *testing.T) {
	// Note: Current implementation creates 1 page per array
	// This test verifies multi-batch writes create multiple pages
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_multipage.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	writer, err := NewWriter(filename, schema, DefaultSerializationOptions())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// Write 3 batches (will create 3 pages)
	var allValues []int32
	for batchNum := 0; batchNum < 3; batchNum++ {
		builder := arrow.NewInt32Builder()
		for i := 0; i < 100; i++ {
			val := int32(batchNum*100 + i)
			builder.Append(val)
			allValues = append(allValues, val)
		}
		array := builder.NewArray()
		builder.Release()

		batch, err := arrow.NewRecordBatch(schema, 100, []arrow.Array{array})
		if err != nil {
			t.Fatalf("NewRecordBatch failed: %v", err)
		}

		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("WriteRecordBatch failed: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer failed: %v", err)
	}

	// Read and verify merge worked correctly
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	if reader.NumRows() != 300 {
		t.Errorf("expected 300 rows, got %d", reader.NumRows())
	}

	resultBatch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	resultArray := resultBatch.Column(0).(*arrow.Int32Array)
	for i, expected := range allValues {
		if resultArray.Value(i) != expected {
			t.Errorf("value mismatch at index %d: expected %d, got %d", i, expected, resultArray.Value(i))
			break
		}
	}
}

func TestWriterReader_MultiPageWithNulls(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_multipage_nulls.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.PrimFloat64(), Nullable: true},
	}, nil)

	writer, err := NewWriter(filename, schema, DefaultSerializationOptions())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// Write 2 batches with nulls
	expectedValues := make([]float64, 0)
	expectedNulls := make([]bool, 0)

	for batchNum := 0; batchNum < 2; batchNum++ {
		builder := arrow.NewFloat64Builder()
		for i := 0; i < 50; i++ {
			if (batchNum*50+i)%7 == 0 {
				builder.AppendNull()
				expectedNulls = append(expectedNulls, false)
				expectedValues = append(expectedValues, 0)
			} else {
				val := float64(batchNum*50+i) * 0.5
				builder.Append(val)
				expectedNulls = append(expectedNulls, true)
				expectedValues = append(expectedValues, val)
			}
		}
		array := builder.NewArray()
		builder.Release()

		batch, err := arrow.NewRecordBatch(schema, 50, []arrow.Array{array})
		if err != nil {
			t.Fatalf("NewRecordBatch failed: %v", err)
		}

		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("WriteRecordBatch failed: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer failed: %v", err)
	}

	// Read and verify
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	resultBatch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	resultArray := resultBatch.Column(0).(*arrow.Float64Array)
	for i := 0; i < len(expectedNulls); i++ {
		isValid := resultArray.IsValid(i)
		if isValid != expectedNulls[i] {
			t.Errorf("null mismatch at index %d: expected valid=%v, got valid=%v", i, expectedNulls[i], isValid)
		}
		if isValid && resultArray.Value(i) != expectedValues[i] {
			t.Errorf("value mismatch at index %d: expected %f, got %f", i, expectedValues[i], resultArray.Value(i))
		}
	}
}

// ====================
// Error Cases
// ====================

func TestPageWriter_EmptyArray(t *testing.T) {
	builder := arrow.NewInt32Builder()
	array := builder.NewArray()
	builder.Release()

	writer := NewPageWriter(DefaultSerializationOptions())
	_, err := writer.WritePages(array, 0)
	if err == nil {
		t.Error("expected error for empty array")
	}
}

func TestPageWriter_NilArray(t *testing.T) {
	writer := NewPageWriter(DefaultSerializationOptions())
	_, err := writer.WritePages(nil, 0)
	if err == nil {
		t.Error("expected error for nil array")
	}
}

func TestWriter_SchemaMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_schema_mismatch.lance")

	schema1 := arrow.NewSchema([]arrow.Field{
		{Name: "field1", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	schema2 := arrow.NewSchema([]arrow.Field{
		{Name: "field2", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	writer, err := NewWriter(filename, schema1, DefaultSerializationOptions())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer writer.Close()

	// Try to write batch with different schema
	builder := arrow.NewInt32Builder()
	builder.Append(1)
	array := builder.NewArray()
	builder.Release()

	batch, err := arrow.NewRecordBatch(schema2, 1, []arrow.Array{array})
	if err != nil {
		t.Fatalf("NewRecordBatch failed: %v", err)
	}

	err = writer.WriteRecordBatch(batch)
	if err == nil {
		t.Error("expected error for schema mismatch")
	}
}

func TestWriter_ClosedWriter(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_closed.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	writer, err := NewWriter(filename, schema, DefaultSerializationOptions())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Try to write after close
	builder := arrow.NewInt32Builder()
	builder.Append(1)
	array := builder.NewArray()
	builder.Release()

	batch, err := arrow.NewRecordBatch(schema, 1, []arrow.Array{array})
	if err != nil {
		t.Fatalf("NewRecordBatch failed: %v", err)
	}

	err = writer.WriteRecordBatch(batch)
	if err == nil {
		t.Error("expected error for writing to closed writer")
	}
}

func TestReader_NonexistentFile(t *testing.T) {
	_, err := NewReader("/nonexistent/file.lance")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestReader_ClosedReader(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_closed_reader.lance")

	// Create a valid file
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	writer, err := NewWriter(filename, schema, DefaultSerializationOptions())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	builder := arrow.NewInt32Builder()
	builder.Append(1)
	array := builder.NewArray()
	builder.Release()

	batch, err := arrow.NewRecordBatch(schema, 1, []arrow.Array{array})
	if err != nil {
		t.Fatalf("NewRecordBatch failed: %v", err)
	}

	writer.WriteRecordBatch(batch)
	writer.Close()

	// Read then close
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("Close reader failed: %v", err)
	}

	// Try to read after close
	_, err = reader.ReadRecordBatch()
	if err == nil {
		t.Error("expected error for reading from closed reader")
	}
}

// ====================
// Helper Functions
// ====================

func arraysEqual(a, b arrow.Array) bool {
	if a.Len() != b.Len() {
		return false
	}

	if a.NullN() != b.NullN() {
		return false
	}

	switch arr := a.(type) {
	case *arrow.Int32Array:
		barr := b.(*arrow.Int32Array)
		for i := 0; i < a.Len(); i++ {
			if a.IsValid(i) != b.IsValid(i) {
				return false
			}
			if a.IsValid(i) && arr.Value(i) != barr.Value(i) {
				return false
			}
		}
	case *arrow.Int64Array:
		barr := b.(*arrow.Int64Array)
		for i := 0; i < a.Len(); i++ {
			if a.IsValid(i) != b.IsValid(i) {
				return false
			}
			if a.IsValid(i) && arr.Value(i) != barr.Value(i) {
				return false
			}
		}
	case *arrow.Float32Array:
		barr := b.(*arrow.Float32Array)
		for i := 0; i < a.Len(); i++ {
			if a.IsValid(i) != b.IsValid(i) {
				return false
			}
			if a.IsValid(i) && arr.Value(i) != barr.Value(i) {
				return false
			}
		}
	case *arrow.Float64Array:
		barr := b.(*arrow.Float64Array)
		for i := 0; i < a.Len(); i++ {
			if a.IsValid(i) != b.IsValid(i) {
				return false
			}
			if a.IsValid(i) && arr.Value(i) != barr.Value(i) {
				return false
			}
		}
	case *arrow.FixedSizeListArray:
		barr := b.(*arrow.FixedSizeListArray)
		if arr.Len() != barr.Len() {
			return false
		}
		// Compare child arrays
		return arraysEqual(arr.Values(), barr.Values())
	default:
		return false
	}

	return true
}

// ====================
// Benchmark Tests
// ====================

func BenchmarkWriteInt32Array(b *testing.B) {
	builder := arrow.NewInt32Builder()
	builder.Reserve(1000)
	for i := 0; i < 1000; i++ {
		builder.Append(int32(i))
	}
	array := builder.NewArray()
	builder.Release()

	writer := NewPageWriter(DefaultSerializationOptions())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = writer.WritePages(array, 0)
	}
}

func BenchmarkReadInt32Array(b *testing.B) {
	builder := arrow.NewInt32Builder()
	builder.Reserve(1000)
	for i := 0; i < 1000; i++ {
		builder.Append(int32(i))
	}
	array := builder.NewArray()
	builder.Release()

	writer := NewPageWriter(DefaultSerializationOptions())
	pages, _ := writer.WritePages(array, 0)

	reader := NewPageReader()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = reader.ReadPage(pages[0], arrow.PrimInt32())
	}
}

func BenchmarkWriteVectorArray(b *testing.B) {
	dim := 768
	numVectors := 100

	childBuilder := arrow.NewFloat32Builder()
	childBuilder.Reserve(dim * numVectors)
	for i := 0; i < numVectors*dim; i++ {
		childBuilder.Append(float32(i) * 0.001)
	}
	childArray := childBuilder.NewArray()
	childBuilder.Release()

	listType := arrow.FixedSizeListOf(arrow.PrimFloat32(), dim)
	array := arrow.NewFixedSizeListArray(listType.(*arrow.FixedSizeListType), childArray, nil)

	writer := NewPageWriter(DefaultSerializationOptions())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = writer.WritePages(array, 0)
	}
}

func BenchmarkFileRoundtrip(b *testing.B) {
	tmpDir := b.TempDir()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimInt32(), Nullable: false},
		{Name: "value", Type: arrow.PrimFloat64(), Nullable: false},
	}, nil)

	idBuilder := arrow.NewInt32Builder()
	valueBuilder := arrow.NewFloat64Builder()
	idBuilder.Reserve(1000)
	valueBuilder.Reserve(1000)

	for i := 0; i < 1000; i++ {
		idBuilder.Append(int32(i))
		valueBuilder.Append(float64(i) * 1.5)
	}

	idArray := idBuilder.NewArray()
	valueArray := valueBuilder.NewArray()
	idBuilder.Release()
	valueBuilder.Release()

	batch, _ := arrow.NewRecordBatch(schema, 1000, []arrow.Array{idArray, valueArray})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filename := filepath.Join(tmpDir, fmt.Sprintf("bench_%d.lance", i))

		writer, _ := NewWriter(filename, schema, DefaultSerializationOptions())
		writer.WriteRecordBatch(batch)
		writer.Close()

		reader, _ := NewReader(filename)
		reader.ReadRecordBatch()
		reader.Close()

		os.Remove(filename)
	}
}
