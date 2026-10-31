package gcosid

import (
	"context"
	"fmt"
)

// StringSegmentID is a segment generator rendered as a string — CosID's
// StringSegmentId. The numeric generator stays the source of truth; the wrapper
// only changes the surface, and it exposes the current segment and the stat of
// both halves.
type StringSegmentID struct {
	Actual    *SegmentGenerator
	Converter StringConverter
}

// NewStringSegmentID wraps a segment generator with a converter.
func NewStringSegmentID(actual *SegmentGenerator, converter StringConverter) (*StringSegmentID, error) {
	if actual == nil {
		return nil, fmt.Errorf("%w: segment generator is required", ErrInvalidConfig)
	}
	if converter == nil {
		return nil, fmt.Errorf("%w: converter is required", ErrInvalidConfig)
	}
	return &StringSegmentID{Actual: actual, Converter: converter}, nil
}

// GenerateAsString implements StringGenerator.
func (s *StringSegmentID) GenerateAsString(ctx context.Context) (string, error) {
	id, err := s.Actual.Next(ctx)
	if err != nil {
		return "", err
	}
	return s.Converter.Format(id), nil
}

// Next keeps the numeric view available.
func (s *StringSegmentID) Next(ctx context.Context) (int64, error) { return s.Actual.Next(ctx) }

// Current exposes the segment in use, like SegmentId.current().
func (s *StringSegmentID) Current() Segment {
	s.Actual.mu.Lock()
	defer s.Actual.mu.Unlock()
	return s.Actual.current
}

// Stat implements Statistical, keeping the converter visible in the tree.
func (s *StringSegmentID) Stat() Stat {
	stat := s.Actual.Stat()
	if converter, ok := s.Converter.(Statistical); ok {
		converted := converter.Stat()
		stat.Actual = &converted
	}
	return stat
}

// StringSnowflakeID is a snowflake generator rendered as a string — CosID's
// StringSnowflakeId.
type StringSnowflakeID struct {
	Actual    *SnowflakeGenerator
	Converter StringConverter
}

// NewStringSnowflakeID wraps a snowflake generator with a converter.
func NewStringSnowflakeID(actual *SnowflakeGenerator, converter StringConverter) (*StringSnowflakeID, error) {
	if actual == nil {
		return nil, fmt.Errorf("%w: snowflake generator is required", ErrInvalidConfig)
	}
	if converter == nil {
		return nil, fmt.Errorf("%w: converter is required", ErrInvalidConfig)
	}
	return &StringSnowflakeID{Actual: actual, Converter: converter}, nil
}

// GenerateAsString implements StringGenerator.
func (s *StringSnowflakeID) GenerateAsString(ctx context.Context) (string, error) {
	id, err := s.Actual.Next(ctx)
	if err != nil {
		return "", err
	}
	return s.Converter.Format(id), nil
}

// Next keeps the numeric view available.
func (s *StringSnowflakeID) Next(ctx context.Context) (int64, error) { return s.Actual.Next(ctx) }

// Layout exposes the wrapped layout, like SnowflakeId.getEpoch()/getTimestampBit().
func (s *StringSnowflakeID) Layout() Layout { return s.Actual.Layout() }

// Stat implements Statistical.
func (s *StringSnowflakeID) Stat() Stat {
	stat := s.Actual.Stat()
	if converter, ok := s.Converter.(Statistical); ok {
		converted := converter.Stat()
		stat.Actual = &converted
	}
	return stat
}
