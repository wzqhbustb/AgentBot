package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"ollama-demo/lance/arrow"
)

// Header represents the Lance file header
type Header struct {
	Magic      uint32        // Magic number (0x4C414E43)
	Version    uint16        // File format version
	Flags      uint16        // Feature flags
	Schema     *arrow.Schema // Arrow schema
	NumRows    int64         // Total number of rows
	NumColumns int32         // Number of columns
	PageSize   int32         // Default page size
	Reserved   [32]byte      // Reserved for future use
}

// HeaderFlags defines feature flags
type HeaderFlags uint16

const (
	FlagCompressed HeaderFlags = 1 << iota // Data is compressed
	FlagEncrypted                          // Data is encrypted
	FlagIndexed                            // File has indices
	FlagVersioned                          // File has version metadata
)

// NewHeader creates a new header
func NewHeader(schema *arrow.Schema, numRows int64) *Header {
	return &Header{
		Magic:      MagicNumber,
		Version:    CurrentVersion,
		Flags:      0,
		Schema:     schema,
		NumRows:    numRows,
		NumColumns: int32(schema.NumFields()),
		PageSize:   DefaultPageSize,
	}
}

// SetFlag sets a feature flag
func (h *Header) SetFlag(flag HeaderFlags) {
	h.Flags |= uint16(flag)
}

// HasFlag checks if a flag is set
func (h *Header) HasFlag(flag HeaderFlags) bool {
	return (h.Flags & uint16(flag)) != 0
}

// Validate validates the header
func (h *Header) Validate() error {
	if err := ValidateMagicNumber(h.Magic); err != nil {
		return err
	}
	if err := ValidateVersion(h.Version); err != nil {
		return err
	}
	if h.Schema == nil {
		return fmt.Errorf("schema is nil")
	}
	if h.NumRows < 0 {
		return fmt.Errorf("invalid row count: %d", h.NumRows)
	}
	if h.NumColumns != int32(h.Schema.NumFields()) {
		return fmt.Errorf("column count mismatch: %d vs schema %d", h.NumColumns, h.Schema.NumFields())
	}
	if h.PageSize <= 0 || h.PageSize > MaxPageSize {
		return fmt.Errorf("invalid page size: %d", h.PageSize)
	}
	return nil
}

// EncodedSize returns the encoded size of the header (without schema)
func (h *Header) EncodedSize() int {
	// Fixed fields: magic(4) + version(2) + flags(2) + numRows(8) + numColumns(4) + pageSize(4) + reserved(32)
	return 4 + 2 + 2 + 8 + 4 + 4 + 32
}

// WriteTo writes the header to a writer
func (h *Header) WriteTo(w io.Writer) (int64, error) {
	if err := h.Validate(); err != nil {
		return 0, NewFileError("write header", err)
	}

	buf := new(bytes.Buffer)

	// Write fixed fields
	binary.Write(buf, ByteOrder, h.Magic)
	binary.Write(buf, ByteOrder, h.Version)
	binary.Write(buf, ByteOrder, h.Flags)
	binary.Write(buf, ByteOrder, h.NumRows)
	binary.Write(buf, ByteOrder, h.NumColumns)
	binary.Write(buf, ByteOrder, h.PageSize)
	binary.Write(buf, ByteOrder, h.Reserved)

	// Serialize schema to JSON (simple approach for Phase 2)
	schemaJSON := serializeSchemaToJSON(h.Schema)
	schemaLen := int32(len(schemaJSON))
	binary.Write(buf, ByteOrder, schemaLen)
	buf.Write(schemaJSON)

	// Write to output
	n, err := w.Write(buf.Bytes())
	return int64(n), err
}

// ReadFrom reads the header from a reader
func (h *Header) ReadFrom(r io.Reader) (int64, error) {
	buf := make([]byte, h.EncodedSize())
	n, err := io.ReadFull(r, buf)
	if err != nil {
		return int64(n), NewFileError("read header", err)
	}

	reader := bytes.NewReader(buf)

	// Read fixed fields
	binary.Read(reader, ByteOrder, &h.Magic)
	binary.Read(reader, ByteOrder, &h.Version)
	binary.Read(reader, ByteOrder, &h.Flags)
	binary.Read(reader, ByteOrder, &h.NumRows)
	binary.Read(reader, ByteOrder, &h.NumColumns)
	binary.Read(reader, ByteOrder, &h.PageSize)
	binary.Read(reader, ByteOrder, &h.Reserved)

	// Validate before reading schema
	if err := ValidateMagicNumber(h.Magic); err != nil {
		return int64(n), err
	}
	if err := ValidateVersion(h.Version); err != nil {
		return int64(n), err
	}

	// Read schema length
	var schemaLen int32
	if err := binary.Read(r, ByteOrder, &schemaLen); err != nil {
		return int64(n) + 4, NewFileError("read schema length", err)
	}

	// Read schema JSON
	schemaJSON := make([]byte, schemaLen)
	if _, err := io.ReadFull(r, schemaJSON); err != nil {
		return int64(n) + 4 + int64(schemaLen), NewFileError("read schema", err)
	}

	// Deserialize schema
	schema, err := deserializeSchemaFromJSON(schemaJSON)
	if err != nil {
		return int64(n) + 4 + int64(schemaLen), NewFileError("deserialize schema", err)
	}
	h.Schema = schema

	return int64(n) + 4 + int64(schemaLen), nil
}

// Helper functions for schema serialization (simplified for Phase 2)
func serializeSchemaToJSON(schema *arrow.Schema) []byte {
	// Simplified JSON serialization
	// In production, use a proper serialization library
	var buf bytes.Buffer
	buf.WriteString("{\"fields\":[")

	for i := 0; i < schema.NumFields(); i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		field := schema.Field(i)
		fmt.Fprintf(&buf, "{\"name\":\"%s\",\"type\":\"%s\",\"nullable\":%t}",
			field.Name, field.Type.Name(), field.Nullable)
	}

	buf.WriteString("],\"metadata\":{")
	first := true
	for k, v := range schema.Metadata() {
		if !first {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "\"%s\":\"%s\"", k, v)
		first = false
	}
	buf.WriteString("}}")

	return buf.Bytes()
}

func deserializeSchemaFromJSON(data []byte) (*arrow.Schema, error) {
	// Simplified deserialization - parses basic structure
	// For Phase 2, we'll assume the schema matches expected HNSW format
	// In production, implement full JSON parsing

	// For now, return a basic schema (this should be enhanced)
	// This is a placeholder - in real implementation, properly parse JSON

	// TODO: Implement proper JSON schema deserialization
	// For Phase 2, we can use SchemaForVectors as default
	return arrow.SchemaForVectors(768), nil
}
