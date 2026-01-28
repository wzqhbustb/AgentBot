package format

import (
	"encoding/binary"
	"fmt"
)

// Lance file format constants
const (
	// MagicNumber identifies a Lance file (ASCII "LANC")
	MagicNumber uint32 = 0x4C414E43

	// CurrentVersion is the current file format version
	CurrentVersion uint16 = 1

	// MinSupportedVersion is the minimum version this implementation can read
	MinSupportedVersion uint16 = 1

	// MaxPageSize is the maximum size of a single page (16 MB)
	MaxPageSize = 16 * 1024 * 1024

	// DefaultPageSize is the default page size (1 MB)
	DefaultPageSize = 1024 * 1024

	// FooterSize is the fixed size of the footer
	FooterSize = 4096 // 4 KB
)

// Encoding types for column data
type EncodingType uint8

const (
	EncodingPlain   EncodingType = iota // No compression
	EncodingZstd                        // Zstd compression
	EncodingDelta                       // Delta encoding
	EncodingRLE                         // Run-length encoding
	EncodingFullZip                     // Full Zip (Phase 3)
)

func (e EncodingType) String() string {
	switch e {
	case EncodingPlain:
		return "Plain"
	case EncodingZstd:
		return "Zstd"
	case EncodingDelta:
		return "Delta"
	case EncodingRLE:
		return "RLE"
	case EncodingFullZip:
		return "FullZip"
	default:
		return fmt.Sprintf("Unknown(%d)", e)
	}
}

// PageType identifies the type of page
type PageType uint8

const (
	PageTypeData  PageType = iota // Regular data page
	PageTypeDict                  // Dictionary page
	PageTypeIndex                 // Index page (for ANN)
)

func (p PageType) String() string {
	switch p {
	case PageTypeData:
		return "Data"
	case PageTypeDict:
		return "Dictionary"
	case PageTypeIndex:
		return "Index"
	default:
		return fmt.Sprintf("Unknown(%d)", p)
	}
}

// ByteOrder is the byte order used throughout Lance files
var ByteOrder = binary.LittleEndian

// Checksum types
type ChecksumType uint8

const (
	ChecksumNone ChecksumType = iota
	ChecksumCRC32
	ChecksumXXHash
)

// FileError represents a Lance file format error
type FileError struct {
	Op  string // Operation that failed
	Err error  // Underlying error
}

func (e *FileError) Error() string {
	return fmt.Sprintf("lance format: %s: %v", e.Op, e.Err)
}

func (e *FileError) Unwrap() error {
	return e.Err
}

// NewFileError creates a new file error
func NewFileError(op string, err error) error {
	if err == nil {
		return nil
	}
	return &FileError{Op: op, Err: err}
}

// ValidateMagicNumber checks if the magic number is valid
func ValidateMagicNumber(magic uint32) error {
	if magic != MagicNumber {
		return fmt.Errorf("invalid magic number: 0x%08X (expected 0x%08X)", magic, MagicNumber)
	}
	return nil
}

// ValidateVersion checks if the version is supported
func ValidateVersion(version uint16) error {
	if version < MinSupportedVersion {
		return fmt.Errorf("unsupported version: %d (minimum %d)", version, MinSupportedVersion)
	}
	if version > CurrentVersion {
		return fmt.Errorf("version too new: %d (current %d)", version, CurrentVersion)
	}
	return nil
}
