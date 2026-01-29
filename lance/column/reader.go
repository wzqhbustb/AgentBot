package column

import (
	"fmt"
	"io"
	"ollama-demo/lance/arrow"
	"ollama-demo/lance/format"
	"os"
)

// Reader reads RecordBatch data from a Lance file
type Reader struct {
	file       *os.File
	header     *format.Header
	footer     *format.Footer
	pageReader *PageReader
	closed     bool
}

// NewReader creates a new column reader
func NewReader(filename string) (*Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open file failed: %w", err)
	}

	reader := &Reader{
		file:       file,
		pageReader: NewPageReader(),
		closed:     false,
	}

	// Read header
	if err := reader.readHeader(); err != nil {
		file.Close()
		return nil, fmt.Errorf("read header failed: %w", err)
	}

	// Read footer
	if err := reader.readFooter(); err != nil {
		file.Close()
		return nil, fmt.Errorf("read footer failed: %w", err)
	}

	return reader, nil
}

// readHeader reads the file header
func (r *Reader) readHeader() error {
	// Seek to beginning
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	r.header = &format.Header{}
	if _, err := r.header.ReadFrom(r.file); err != nil {
		return err
	}

	return nil
}

// readFooter reads the file footer
func (r *Reader) readFooter() error {
	// Get file size
	fileInfo, err := r.file.Stat()
	if err != nil {
		return err
	}

	// Seek to footer (last FooterSize bytes)
	footerOffset := fileInfo.Size() - format.FooterSize
	if _, err := r.file.Seek(footerOffset, io.SeekStart); err != nil {
		return err
	}

	r.footer = &format.Footer{}
	if _, err := r.footer.ReadFrom(r.file); err != nil {
		return err
	}

	return nil
}

// Schema returns the schema of the file
func (r *Reader) Schema() *arrow.Schema {
	return r.header.Schema
}

// NumRows returns the total number of rows in the file
func (r *Reader) NumRows() int64 {
	return r.header.NumRows
}

// ReadRecordBatch reads all data and returns a RecordBatch
func (r *Reader) ReadRecordBatch() (*arrow.RecordBatch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	schema := r.header.Schema
	numColumns := schema.NumFields()

	// Read each column
	columns := make([]arrow.Array, numColumns)
	for colIdx := 0; colIdx < numColumns; colIdx++ {
		column, err := r.readColumn(int32(colIdx))
		if err != nil {
			return nil, fmt.Errorf("read column %d failed: %w", colIdx, err)
		}
		columns[colIdx] = column
	}

	// Create RecordBatch
	batch, err := arrow.NewRecordBatch(schema, int(r.header.NumRows), columns)
	if err != nil {
		return nil, fmt.Errorf("create record batch failed: %w", err)
	}

	return batch, nil
}

// readColumn reads a single column from the file
func (r *Reader) readColumn(columnIndex int32) (arrow.Array, error) {
	// Get pages for this column
	pageIndices := r.footer.GetColumnPages(columnIndex)
	if len(pageIndices) == 0 {
		return nil, fmt.Errorf("no pages found for column %d", columnIndex)
	}

	// Get field type
	if int(columnIndex) >= r.header.Schema.NumFields() {
		return nil, fmt.Errorf("column index %d out of range", columnIndex)
	}
	field := r.header.Schema.Field(int(columnIndex))

	// Read all pages
	var arrays []arrow.Array
	for _, pageIdx := range pageIndices {
		page, err := r.readPage(pageIdx)
		if err != nil {
			return nil, fmt.Errorf("read page failed: %w", err)
		}

		array, err := r.pageReader.ReadPage(page, field.Type)
		if err != nil {
			return nil, fmt.Errorf("deserialize page failed: %w", err)
		}

		arrays = append(arrays, array)
	}

	// If single page, return directly
	if len(arrays) == 1 {
		return arrays[0], nil
	}

	// Merge multiple pages using appropriate builder
	return r.mergeArrays(arrays, field.Type)
}

// mergeArrays merges multiple arrays of the same type into one
func (r *Reader) mergeArrays(arrays []arrow.Array, dataType arrow.DataType) (arrow.Array, error) {
	if len(arrays) == 0 {
		return nil, fmt.Errorf("no arrays to merge")
	}

	if len(arrays) == 1 {
		return arrays[0], nil
	}

	switch dataType.ID() {
	case arrow.INT32:
		return r.mergeInt32Arrays(arrays)
	case arrow.INT64:
		return r.mergeInt64Arrays(arrays)
	case arrow.FLOAT32:
		return r.mergeFloat32Arrays(arrays)
	case arrow.FLOAT64:
		return r.mergeFloat64Arrays(arrays)
	case arrow.FIXED_SIZE_LIST:
		return r.mergeFixedSizeListArrays(arrays, dataType.(*arrow.FixedSizeListType))
	default:
		return nil, fmt.Errorf("unsupported array type for merging: %s", dataType.Name())
	}
}

// mergeInt32Arrays merges multiple Int32Array into one
func (r *Reader) mergeInt32Arrays(arrays []arrow.Array) (arrow.Array, error) {
	builder := arrow.NewInt32Builder()
	defer builder.Release()

	// Calculate total size for reservation
	totalSize := 0
	for _, arr := range arrays {
		totalSize += arr.Len()
	}
	builder.Reserve(totalSize)

	// Append all values
	for _, arr := range arrays {
		int32Arr := arr.(*arrow.Int32Array)
		for i := 0; i < int32Arr.Len(); i++ {
			if int32Arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(int32Arr.Value(i))
			}
		}
	}

	return builder.NewArray(), nil
}

// mergeInt64Arrays merges multiple Int64Array into one
func (r *Reader) mergeInt64Arrays(arrays []arrow.Array) (arrow.Array, error) {
	builder := &arrow.Int64Builder{}

	totalSize := 0
	for _, arr := range arrays {
		totalSize += arr.Len()
	}
	builder.Reserve(totalSize)

	for _, arr := range arrays {
		int64Arr := arr.(*arrow.Int64Array)
		for i := 0; i < int64Arr.Len(); i++ {
			if int64Arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(int64Arr.Value(i))
			}
		}
	}

	return builder.NewArray(), nil
}

// mergeFloat32Arrays merges multiple Float32Array into one
func (r *Reader) mergeFloat32Arrays(arrays []arrow.Array) (arrow.Array, error) {
	builder := arrow.NewFloat32Builder()
	defer builder.Release()

	totalSize := 0
	for _, arr := range arrays {
		totalSize += arr.Len()
	}
	builder.Reserve(totalSize)

	for _, arr := range arrays {
		float32Arr := arr.(*arrow.Float32Array)
		for i := 0; i < float32Arr.Len(); i++ {
			if float32Arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(float32Arr.Value(i))
			}
		}
	}

	return builder.NewArray(), nil
}

// mergeFloat64Arrays merges multiple Float64Array into one
func (r *Reader) mergeFloat64Arrays(arrays []arrow.Array) (arrow.Array, error) {
	builder := &arrow.Float64Builder{}

	totalSize := 0
	for _, arr := range arrays {
		totalSize += arr.Len()
	}
	builder.Reserve(totalSize)

	for _, arr := range arrays {
		float64Arr := arr.(*arrow.Float64Array)
		for i := 0; i < float64Arr.Len(); i++ {
			if float64Arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(float64Arr.Value(i))
			}
		}
	}

	return builder.NewArray(), nil
}

// mergeFixedSizeListArrays merges multiple FixedSizeListArray into one
func (r *Reader) mergeFixedSizeListArrays(arrays []arrow.Array, listType *arrow.FixedSizeListType) (arrow.Array, error) {
	builder := arrow.NewFixedSizeListBuilder(listType)
	defer builder.Release()

	totalSize := 0
	for _, arr := range arrays {
		totalSize += arr.Len()
	}
	builder.Reserve(totalSize)

	for _, arr := range arrays {
		listArr := arr.(*arrow.FixedSizeListArray)

		for i := 0; i < listArr.Len(); i++ {
			if listArr.IsNull(i) {
				builder.AppendNull()
			} else {
				// Get values for this list
				values := r.getFixedSizeListValues(listArr, i)
				builder.AppendValues(values)
			}
		}
	}

	return builder.NewArray(), nil
}

// getFixedSizeListValues extracts values from a FixedSizeListArray at index i
func (r *Reader) getFixedSizeListValues(arr *arrow.FixedSizeListArray, index int) []float32 {
	listSize := arr.ListSize()
	values := make([]float32, listSize)

	// Get the underlying values array
	valuesArray := arr.Values()

	// Calculate offset in values array
	startOffset := index * listSize

	switch valArr := valuesArray.(type) {
	case *arrow.Float32Array:
		for j := 0; j < listSize; j++ {
			values[j] = valArr.Value(startOffset + j)
		}
	case *arrow.Int32Array:
		for j := 0; j < listSize; j++ {
			values[j] = float32(valArr.Value(startOffset + j))
		}
	}

	return values
}

// readPage reads a single page from the file
func (r *Reader) readPage(pageIndex format.PageIndex) (*format.Page, error) {
	// Seek to page offset
	if _, err := r.file.Seek(pageIndex.Offset, io.SeekStart); err != nil {
		return nil, err
	}

	// Read page
	page := &format.Page{}
	if _, err := page.ReadFrom(r.file); err != nil {
		return nil, err
	}

	return page, nil
}

// Close closes the reader
func (r *Reader) Close() error {
	if r.closed {
		return fmt.Errorf("reader already closed")
	}

	r.closed = true
	return r.file.Close()
}
