package column

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"ollama-demo/lance/arrow"
	"ollama-demo/lance/format"
)

// PageReader handles deserialization of Page data to Arrays
type PageReader struct{}

// NewPageReader creates a new page reader
func NewPageReader() *PageReader {
	return &PageReader{}
}

// ReadPage converts a Page back into an Array
func (r *PageReader) ReadPage(page *format.Page, dataType arrow.DataType) (arrow.Array, error) {
	if page == nil {
		return nil, fmt.Errorf("page is nil")
	}

	if len(page.Data) == 0 {
		return nil, fmt.Errorf("page data is empty")
	}

	// Deserialize based on data type
	return r.deserializeArray(page.Data, dataType, int(page.NumValues))
}

// deserializeArray converts bytes back to an Array
func (r *PageReader) deserializeArray(data []byte, dataType arrow.DataType, numValues int) (arrow.Array, error) {
	switch dataType.ID() {
	case arrow.INT32:
		return r.deserializeInt32Array(data, numValues)
	case arrow.INT64:
		return r.deserializeInt64Array(data, numValues)
	case arrow.FLOAT32:
		return r.deserializeFloat32Array(data, numValues)
	case arrow.FLOAT64:
		return r.deserializeFloat64Array(data, numValues)
	case arrow.FIXED_SIZE_LIST:
		listType := dataType.(*arrow.FixedSizeListType)
		return r.deserializeFixedSizeListArray(data, listType, numValues)
	default:
		return nil, fmt.Errorf("unsupported data type: %s", dataType.Name())
	}
}

// deserializeInt32Array deserializes Int32Array
func (r *PageReader) deserializeInt32Array(data []byte, numValues int) (*arrow.Int32Array, error) {
	reader := bytes.NewReader(data)

	// Read null bitmap flag
	var hasNulls bool
	if err := binary.Read(reader, binary.LittleEndian, &hasNulls); err != nil {
		return nil, err
	}

	var nullBitmap *arrow.Bitmap
	if hasNulls {
		var bitmapBytes int32
		if err := binary.Read(reader, binary.LittleEndian, &bitmapBytes); err != nil {
			return nil, err
		}

		bitmapData := make([]byte, bitmapBytes)
		if _, err := reader.Read(bitmapData); err != nil {
			return nil, err
		}

		nullBitmap = arrow.NewBitmap(int(bitmapBytes * 8))
		copy(nullBitmap.Bytes(), bitmapData)
	}

	// Read values
	var valCount int32
	if err := binary.Read(reader, binary.LittleEndian, &valCount); err != nil {
		return nil, err
	}

	values := make([]int32, valCount)
	for i := range values {
		if err := binary.Read(reader, binary.LittleEndian, &values[i]); err != nil {
			return nil, err
		}
	}

	return arrow.NewInt32Array(values, nullBitmap), nil
}

// deserializeInt64Array deserializes Int64Array
func (r *PageReader) deserializeInt64Array(data []byte, numValues int) (*arrow.Int64Array, error) {
	reader := bytes.NewReader(data)

	var hasNulls bool
	if err := binary.Read(reader, binary.LittleEndian, &hasNulls); err != nil {
		return nil, err
	}

	var nullBitmap *arrow.Bitmap
	if hasNulls {
		var bitmapBytes int32
		if err := binary.Read(reader, binary.LittleEndian, &bitmapBytes); err != nil {
			return nil, err
		}

		bitmapData := make([]byte, bitmapBytes)
		if _, err := reader.Read(bitmapData); err != nil {
			return nil, err
		}

		nullBitmap = arrow.NewBitmap(int(bitmapBytes * 8))
		copy(nullBitmap.Bytes(), bitmapData)
	}

	var valCount int32
	if err := binary.Read(reader, binary.LittleEndian, &valCount); err != nil {
		return nil, err
	}

	values := make([]int64, valCount)
	for i := range values {
		if err := binary.Read(reader, binary.LittleEndian, &values[i]); err != nil {
			return nil, err
		}
	}

	return arrow.NewInt64Array(values, nullBitmap), nil
}

// deserializeFloat32Array deserializes Float32Array
func (r *PageReader) deserializeFloat32Array(data []byte, numValues int) (*arrow.Float32Array, error) {
	reader := bytes.NewReader(data)

	var hasNulls bool
	if err := binary.Read(reader, binary.LittleEndian, &hasNulls); err != nil {
		return nil, err
	}

	var nullBitmap *arrow.Bitmap
	if hasNulls {
		var bitmapBytes int32
		if err := binary.Read(reader, binary.LittleEndian, &bitmapBytes); err != nil {
			return nil, err
		}

		bitmapData := make([]byte, bitmapBytes)
		if _, err := reader.Read(bitmapData); err != nil {
			return nil, err
		}

		nullBitmap = arrow.NewBitmap(int(bitmapBytes * 8))
		copy(nullBitmap.Bytes(), bitmapData)
	}

	var valCount int32
	if err := binary.Read(reader, binary.LittleEndian, &valCount); err != nil {
		return nil, err
	}

	values := make([]float32, valCount)
	for i := range values {
		if err := binary.Read(reader, binary.LittleEndian, &values[i]); err != nil {
			return nil, err
		}
	}

	return arrow.NewFloat32Array(values, nullBitmap), nil
}

// deserializeFloat64Array deserializes Float64Array
func (r *PageReader) deserializeFloat64Array(data []byte, numValues int) (*arrow.Float64Array, error) {
	reader := bytes.NewReader(data)

	var hasNulls bool
	if err := binary.Read(reader, binary.LittleEndian, &hasNulls); err != nil {
		return nil, err
	}

	var nullBitmap *arrow.Bitmap
	if hasNulls {
		var bitmapBytes int32
		if err := binary.Read(reader, binary.LittleEndian, &bitmapBytes); err != nil {
			return nil, err
		}

		bitmapData := make([]byte, bitmapBytes)
		if _, err := reader.Read(bitmapData); err != nil {
			return nil, err
		}

		nullBitmap = arrow.NewBitmap(int(bitmapBytes * 8))
		copy(nullBitmap.Bytes(), bitmapData)
	}

	var valCount int32
	if err := binary.Read(reader, binary.LittleEndian, &valCount); err != nil {
		return nil, err
	}

	values := make([]float64, valCount)
	for i := range values {
		if err := binary.Read(reader, binary.LittleEndian, &values[i]); err != nil {
			return nil, err
		}
	}

	return arrow.NewFloat64Array(values, nullBitmap), nil
}

// deserializeFixedSizeListArray deserializes FixedSizeListArray
func (r *PageReader) deserializeFixedSizeListArray(data []byte, listType *arrow.FixedSizeListType, numValues int) (*arrow.FixedSizeListArray, error) {
	reader := bytes.NewReader(data)

	// Read list size
	var listSize int32
	if err := binary.Read(reader, binary.LittleEndian, &listSize); err != nil {
		return nil, err
	}

	// Verify list size matches type
	if int(listSize) != listType.Size() {
		return nil, fmt.Errorf("list size mismatch: expected %d, got %d", listType.Size(), listSize)
	}

	// Read null bitmap
	var hasNulls bool
	if err := binary.Read(reader, binary.LittleEndian, &hasNulls); err != nil {
		return nil, err
	}

	var nullBitmap *arrow.Bitmap
	if hasNulls {
		var bitmapBytes int32
		if err := binary.Read(reader, binary.LittleEndian, &bitmapBytes); err != nil {
			return nil, err
		}

		bitmapData := make([]byte, bitmapBytes)
		if _, err := reader.Read(bitmapData); err != nil {
			return nil, err
		}

		nullBitmap = arrow.NewBitmap(int(bitmapBytes * 8))
		copy(nullBitmap.Bytes(), bitmapData)
	}

	// Read number of lists
	var numLists int32
	if err := binary.Read(reader, binary.LittleEndian, &numLists); err != nil {
		return nil, err
	}

	// Read flattened values based on element type
	elemType := listType.Elem()

	switch elemType.ID() {
	case arrow.FLOAT32:
		var totalValues int32
		if err := binary.Read(reader, binary.LittleEndian, &totalValues); err != nil {
			return nil, err
		}

		values := make([]float32, totalValues)
		for i := range values {
			if err := binary.Read(reader, binary.LittleEndian, &values[i]); err != nil {
				return nil, err
			}
		}

		return arrow.NewFixedSizeListArray(values, int(listSize), nullBitmap), nil

	case arrow.INT32:
		var totalValues int32
		if err := binary.Read(reader, binary.LittleEndian, &totalValues); err != nil {
			return nil, err
		}

		values := make([]int32, totalValues)
		for i := range values {
			if err := binary.Read(reader, binary.LittleEndian, &values[i]); err != nil {
				return nil, err
			}
		}

		valuesFloat32 := make([]float32, len(values))
		for i, v := range values {
			valuesFloat32[i] = float32(v)
		}

		return arrow.NewFixedSizeListArray(valuesFloat32, int(listSize), nullBitmap), nil

	default:
		return nil, fmt.Errorf("unsupported FixedSizeList element type: %s", elemType.Name())
	}
}
