package column

import (
	"bytes"
	"fmt"
	"io"
	"ollama-demo/lance/arrow"
	"ollama-demo/lance/format"
	"os"
)

const (
	// HeaderReservedSize is the fixed size reserved for file header
	// This ensures header can be rewritten without affecting page offsets
	HeaderReservedSize = 8192 // 8KB should be enough for any reasonable schema
)

// Writer writes RecordBatch data to a Lance file
type Writer struct {
	file       *os.File
	header     *format.Header
	footer     *format.Footer
	pageWriter *PageWriter
	headerSize int64 // Always equals HeaderReservedSize
	currentPos int64 // Current write position
	options    SerializationOptions
	closed     bool
}

// NewWriter creates a new column writer
func NewWriter(filename string, schema *arrow.Schema, options SerializationOptions) (*Writer, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("create file failed: %w", err)
	}

	writer := &Writer{
		file:       file,
		header:     format.NewHeader(schema, 0), // NumRows will be updated later
		footer:     format.NewFooter(),
		pageWriter: NewPageWriter(options),
		options:    options,
		closed:     false,
		headerSize: HeaderReservedSize,
	}

	// Write initial header with padding to reserve space
	if err := writer.writeHeaderWithPadding(); err != nil {
		file.Close()
		return nil, fmt.Errorf("write initial header failed: %w", err)
	}

	writer.currentPos = HeaderReservedSize // Start writing pages after reserved header space

	return writer, nil
}

// writeHeaderWithPadding writes header and pads to HeaderReservedSize
func (w *Writer) writeHeaderWithPadding() error {
	// Serialize header to buffer first
	headerBuf := new(bytes.Buffer)
	_, err := w.header.WriteTo(headerBuf)
	if err != nil {
		return fmt.Errorf("serialize header failed: %w", err)
	}

	headerData := headerBuf.Bytes()
	headerLen := len(headerData)

	// Check if header fits in reserved space
	if headerLen > HeaderReservedSize {
		return fmt.Errorf("header size %d exceeds reserved size %d", headerLen, HeaderReservedSize)
	}

	// Write header data
	if _, err := w.file.Write(headerData); err != nil {
		return fmt.Errorf("write header data failed: %w", err)
	}

	// Write padding to fill reserved space
	paddingSize := HeaderReservedSize - headerLen
	if paddingSize > 0 {
		padding := make([]byte, paddingSize)
		if _, err := w.file.Write(padding); err != nil {
			return fmt.Errorf("write header padding failed: %w", err)
		}
	}

	return nil
}

// WriteRecordBatch writes a RecordBatch to the file
func (w *Writer) WriteRecordBatch(batch *arrow.RecordBatch) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	if batch == nil {
		return fmt.Errorf("batch is nil")
	}

	// Validate schema matches
	if !w.header.Schema.Equal(batch.Schema()) {
		return fmt.Errorf("schema mismatch")
	}

	// Update header row count
	w.header.NumRows += int64(batch.NumRows())

	// Write each column
	for colIdx := 0; colIdx < batch.NumCols(); colIdx++ {
		column := batch.Column(colIdx)
		field := batch.Schema().Field(colIdx)

		if err := validateArray(column, field); err != nil {
			return fmt.Errorf("column %d (%s) validation failed: %w", colIdx, field.Name, err)
		}

		if err := w.writeColumn(int32(colIdx), column); err != nil {
			return fmt.Errorf("write column %d (%s) failed: %w", colIdx, field.Name, err)
		}
	}

	return nil
}

// writeColumn writes a single column (Array) to the file
func (w *Writer) writeColumn(columnIndex int32, array arrow.Array) error {
	// Convert array to pages
	pages, err := w.pageWriter.WritePages(array, columnIndex)
	if err != nil {
		return fmt.Errorf("create pages failed: %w", err)
	}

	// Write each page and record metadata
	for pageNum, page := range pages {
		// Record current position (relative to file start)
		pageOffset := w.currentPos

		// Write page to file
		n, err := page.WriteTo(w.file)
		if err != nil {
			return fmt.Errorf("write page failed: %w", err)
		}

		// Update position
		w.currentPos += n

		// Add page index to footer
		w.footer.PageIndexList.Add(
			columnIndex,
			int32(pageNum),
			pageOffset,
			int32(n),
			page.NumValues,
		)
	}

	return nil
}

// Close finalizes the file by writing header and footer
func (w *Writer) Close() error {
	if w.closed {
		return fmt.Errorf("writer already closed")
	}

	w.closed = true

	// Update footer
	w.footer.NumPages = int32(len(w.footer.PageIndexList.Indices))

	// Write footer at current position (after all pages)
	if _, err := w.file.Seek(w.currentPos, io.SeekStart); err != nil {
		return fmt.Errorf("seek to footer position failed: %w", err)
	}

	if _, err := w.footer.WriteTo(w.file); err != nil {
		return fmt.Errorf("write footer failed: %w", err)
	}

	// Update header with final NumRows
	// Serialize to buffer first to check size
	headerBuf := new(bytes.Buffer)
	if _, err := w.header.WriteTo(headerBuf); err != nil {
		return fmt.Errorf("serialize final header failed: %w", err)
	}

	headerData := headerBuf.Bytes()
	headerLen := len(headerData)

	// Verify header still fits in reserved space
	if headerLen > HeaderReservedSize {
		return fmt.Errorf("final header size %d exceeds reserved size %d", headerLen, HeaderReservedSize)
	}

	// Seek back to beginning and rewrite header
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to header failed: %w", err)
	}

	// Write updated header (no need to write padding again, it's already there)
	if _, err := w.file.Write(headerData); err != nil {
		return fmt.Errorf("rewrite header failed: %w", err)
	}

	// Close file
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close file failed: %w", err)
	}

	return nil
}
