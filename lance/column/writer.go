package column

import (
	"fmt"
	"io"
	"ollama-demo/lance/arrow"
	"ollama-demo/lance/format"
	"os"
)

// Writer writes RecordBatch data to a Lance file
type Writer struct {
	file       *os.File
	header     *format.Header
	footer     *format.Footer
	pageWriter *PageWriter
	currentPos int64
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
		header:     format.NewHeader(schema, 0), // NumRows set later
		footer:     format.NewFooter(),
		pageWriter: NewPageWriter(options),
		options:    options,
		closed:     false,
	}

	return writer, nil
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
			return fmt.Errorf("column %d validation failed: %w", colIdx, err)
		}

		if err := w.writeColumn(int32(colIdx), column); err != nil {
			return fmt.Errorf("write column %d failed: %w", colIdx, err)
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
		// Record current position
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

	// Seek to beginning to write header
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start failed: %w", err)
	}

	// Write header
	headerSize, err := w.header.WriteTo(w.file)
	if err != nil {
		return fmt.Errorf("write header failed: %w", err)
	}

	// Now we need to rewrite the file with correct layout:
	// Header -> Pages -> Footer
	// Since pages are already written, we need to:
	// 1. Read all page data
	// 2. Rewrite file with correct offsets

	// For simplicity in Phase 2.2, we'll write in this order:
	// 1. Seek to end
	// 2. Write footer

	if _, err := w.file.Seek(w.currentPos, io.SeekStart); err != nil {
		return fmt.Errorf("seek to end failed: %w", err)
	}

	// Write footer
	_, err = w.footer.WriteTo(w.file)
	if err != nil {
		return fmt.Errorf("write footer failed: %w", err)
	}

	// Close file
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close file failed: %w", err)
	}

	// Now rewrite with correct layout
	return w.rewriteWithCorrectLayout(headerSize)
}

// rewriteWithCorrectLayout reorganizes the file to: Header -> Pages -> Footer
func (w *Writer) rewriteWithCorrectLayout(headerSize int64) error {
	// Reopen file for reading
	tempFile := w.file.Name() + ".tmp"

	// Read original file
	originalData, err := os.ReadFile(w.file.Name())
	if err != nil {
		return fmt.Errorf("read original file failed: %w", err)
	}

	// Create new file
	newFile, err := os.Create(tempFile)
	if err != nil {
		return fmt.Errorf("create temp file failed: %w", err)
	}
	defer newFile.Close()

	// Write header
	headerData := originalData[:headerSize]
	if _, err := newFile.Write(headerData); err != nil {
		return fmt.Errorf("write header to temp failed: %w", err)
	}

	// Calculate page data region (skip header, take until footer)
	footerOffset := int64(len(originalData)) - format.FooterSize
	pageData := originalData[headerSize:footerOffset]

	// Write page data
	if _, err := newFile.Write(pageData); err != nil {
		return fmt.Errorf("write pages to temp failed: %w", err)
	}

	// Write footer
	footerData := originalData[footerOffset:]
	if _, err := newFile.Write(footerData); err != nil {
		return fmt.Errorf("write footer to temp failed: %w", err)
	}

	// Replace original with temp
	if err := os.Rename(tempFile, w.file.Name()); err != nil {
		return fmt.Errorf("rename temp file failed: %w", err)
	}

	return nil
}
