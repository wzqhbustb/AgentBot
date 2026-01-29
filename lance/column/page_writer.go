package column

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"ollama-demo/lance/arrow"
	"ollama-demo/lance/format"
)

// PageWriter handles serialization of Array data to Pages
type PageWriter struct {
	options SerializationOptions
}

// NewPageWriter creates a new page writer
func NewPageWriter(options SerializationOptions) *PageWriter {
	return &PageWriter{
		options: options,
	}
}

// WritePages converts an Array into one or more Pages
func (w *PageWriter) WritePages(array arrow.Array, columnIndex int32) ([]*format.Page, error) {
	if array == nil || array.Len() == 0 {
		return nil, fmt.Errorf("cannot write empty array")
	}

	// For simplicity, create one page per array
	// In production, would split large arrays across multiple pages
	pages := make([]*format.Page, 0, 1)

	// Serialize to bytes
	data, err := w.serializeArray(array)
	if err != nil {
		return nil, fmt.Errorf("serialize page failed: %w", err)
	}

	// Create page
	page := format.NewPage(columnIndex, format.PageTypeData, w.options.Encoding)
	page.NumValues = int32(array.Len())
	page.SetData(data, int32(len(data))) // Uncompressed for now

	pages = append(pages, page)

	return pages, nil
}

// serializeArray converts an Array to bytes
func (w *PageWriter) serializeArray(array arrow.Array) ([]byte, error) {
	switch arr := array.(type) {
	case *arrow.Int32Array:
		return w.serializeInt32Array(arr)
	case *arrow.Int64Array:
		return w.serializeInt64Array(arr)
	case *arrow.Float32Array:
		return w.serializeFloat32Array(arr)
	case *arrow.Float64Array:
		return w.serializeFloat64Array(arr)
	case *arrow.FixedSizeListArray:
		return w.serializeFixedSizeListArray(arr)
	default:
		return nil, fmt.Errorf("unsupported array type: %T", array)
	}
}

// serializeInt32Array serializes Int32Array
func (w *PageWriter) serializeInt32Array(array *arrow.Int32Array) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write null bitmap if exists
	hasNulls := array.NullN() > 0
	if err := binary.Write(buf, binary.LittleEndian, hasNulls); err != nil {
		return nil, err
	}

	if hasNulls {
		nullBitmap := array.Data().NullBitmap()
		bitmapBytes := (array.Len() + 7) / 8
		if err := binary.Write(buf, binary.LittleEndian, int32(bitmapBytes)); err != nil {
			return nil, err
		}
		buf.Write(nullBitmap.Bytes()[:bitmapBytes])
	}

	// Write values
	values := array.Values()
	if err := binary.Write(buf, binary.LittleEndian, int32(len(values))); err != nil {
		return nil, err
	}

	for _, v := range values {
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// serializeInt64Array serializes Int64Array
func (w *PageWriter) serializeInt64Array(array *arrow.Int64Array) ([]byte, error) {
	buf := new(bytes.Buffer)

	hasNulls := array.NullN() > 0
	if err := binary.Write(buf, binary.LittleEndian, hasNulls); err != nil {
		return nil, err
	}

	if hasNulls {
		nullBitmap := array.Data().NullBitmap()
		bitmapBytes := (array.Len() + 7) / 8
		if err := binary.Write(buf, binary.LittleEndian, int32(bitmapBytes)); err != nil {
			return nil, err
		}
		buf.Write(nullBitmap.Bytes()[:bitmapBytes])
	}

	values := array.Values()
	if err := binary.Write(buf, binary.LittleEndian, int32(len(values))); err != nil {
		return nil, err
	}

	for _, v := range values {
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// serializeFloat32Array serializes Float32Array
func (w *PageWriter) serializeFloat32Array(array *arrow.Float32Array) ([]byte, error) {
	buf := new(bytes.Buffer)

	hasNulls := array.NullN() > 0
	if err := binary.Write(buf, binary.LittleEndian, hasNulls); err != nil {
		return nil, err
	}

	if hasNulls {
		nullBitmap := array.Data().NullBitmap()
		bitmapBytes := (array.Len() + 7) / 8
		if err := binary.Write(buf, binary.LittleEndian, int32(bitmapBytes)); err != nil {
			return nil, err
		}
		buf.Write(nullBitmap.Bytes()[:bitmapBytes])
	}

	values := array.Values()
	if err := binary.Write(buf, binary.LittleEndian, int32(len(values))); err != nil {
		return nil, err
	}

	for _, v := range values {
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// serializeFloat64Array serializes Float64Array
func (w *PageWriter) serializeFloat64Array(array *arrow.Float64Array) ([]byte, error) {
	buf := new(bytes.Buffer)

	hasNulls := array.NullN() > 0
	if err := binary.Write(buf, binary.LittleEndian, hasNulls); err != nil {
		return nil, err
	}

	if hasNulls {
		nullBitmap := array.Data().NullBitmap()
		bitmapBytes := (array.Len() + 7) / 8
		if err := binary.Write(buf, binary.LittleEndian, int32(bitmapBytes)); err != nil {
			return nil, err
		}
		buf.Write(nullBitmap.Bytes()[:bitmapBytes])
	}

	values := array.Values()
	if err := binary.Write(buf, binary.LittleEndian, int32(len(values))); err != nil {
		return nil, err
	}

	for _, v := range values {
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// serializeFixedSizeListArray serializes FixedSizeListArray (for vectors)
func (w *PageWriter) serializeFixedSizeListArray(array *arrow.FixedSizeListArray) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write list size
	listSize := array.ListSize()
	if err := binary.Write(buf, binary.LittleEndian, int32(listSize)); err != nil {
		return nil, err
	}

	// Write null bitmap
	hasNulls := array.NullN() > 0
	if err := binary.Write(buf, binary.LittleEndian, hasNulls); err != nil {
		return nil, err
	}

	if hasNulls {
		nullBitmap := array.Data().NullBitmap()
		bitmapBytes := (array.Len() + 7) / 8
		if err := binary.Write(buf, binary.LittleEndian, int32(bitmapBytes)); err != nil {
			return nil, err
		}
		buf.Write(nullBitmap.Bytes()[:bitmapBytes])
	}

	// Write number of lists
	if err := binary.Write(buf, binary.LittleEndian, int32(array.Len())); err != nil {
		return nil, err
	}

	// Write flattened values
	valuesArray := array.Values()

	switch arr := valuesArray.(type) {
	case *arrow.Float32Array:
		values := arr.Values()
		// Write total number of float32 values
		totalValues := int32(len(values))
		if err := binary.Write(buf, binary.LittleEndian, totalValues); err != nil {
			return nil, err
		}
		// Write all values
		for _, v := range values {
			if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
				return nil, err
			}
		}
	case *arrow.Int32Array:
		values := arr.Values()
		totalValues := int32(len(values))
		if err := binary.Write(buf, binary.LittleEndian, totalValues); err != nil {
			return nil, err
		}
		for _, v := range values {
			if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("unsupported FixedSizeList element type: %T", valuesArray)
	}

	return buf.Bytes(), nil
}
