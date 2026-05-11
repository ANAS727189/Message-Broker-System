package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type Segment struct {
	BaseOffset uint64
	LogFile    *os.File
	IdxFile    *os.File
}

type Store struct {
	mu          sync.RWMutex
	dirPath     string
	baseName    string
	segments    []*Segment
	active      *Segment
	nextID      uint64
	maxBytes    int64
	currentSize int64
}

func New(dirPath string, fileName string) (*Store, error) {
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	s := &Store{
		dirPath:  dirPath,
		baseName: fileName,
		maxBytes: 1024 * 1024,
	}

	if err := s.discoverSegments(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Store) discoverSegments() error {
	entries, err := os.ReadDir(s.dirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	segmentMap := make(map[uint64]*Segment)

	// Find all segment files and group by base offset
	for _, entry := range entries {
		name := entry.Name()
		if !strings.Contains(name, s.baseName) {
			continue
		}

		// Parse base offset from filename: "messages-0000000001.log" or "messages.log"
		var baseOffset uint64
		if strings.HasPrefix(name, s.baseName+"-") && strings.HasSuffix(name, ".log") {
			offsetStr := strings.TrimPrefix(name, s.baseName+"-")
			offsetStr = strings.TrimSuffix(offsetStr, ".log")
			fmt.Sscanf(offsetStr, "%d", &baseOffset)
		} else if name == s.baseName+".log" {
			baseOffset = 0
		} else {
			continue
		}

		if _, exists := segmentMap[baseOffset]; !exists {
			segmentMap[baseOffset] = &Segment{BaseOffset: baseOffset}
		}
	}

	// Sort segments by base offset
	var offsets []uint64
	for offset := range segmentMap {
		offsets = append(offsets, offset)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })

	// Open files for each segment
	for _, offset := range offsets {
		seg := segmentMap[offset]

		logPath := s.getLogPath(offset)
		idxPath := s.getIdxPath(offset)

		lf, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return fmt.Errorf("failed to open log file %s: %w", logPath, err)
		}

		ifile, err := os.OpenFile(idxPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			lf.Close()
			return fmt.Errorf("failed to open idx file %s: %w", idxPath, err)
		}

		seg.LogFile = lf
		seg.IdxFile = ifile
		s.segments = append(s.segments, seg)
	}

	// If no segments exist, create initial segment
	if len(s.segments) == 0 {
		seg := &Segment{BaseOffset: 0}
		logPath := s.getLogPath(0)
		idxPath := s.getIdxPath(0)

		lf, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
		ifile, err := os.OpenFile(idxPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			lf.Close()
			return err
		}

		seg.LogFile = lf
		seg.IdxFile = ifile
		s.segments = append(s.segments, seg)
	}

	s.active = s.segments[len(s.segments)-1]

	// Recover nextID and validate WAL
	if err := s.recoverState(); err != nil {
		return err
	}

	return nil
}

func (s *Store) getLogPath(offset uint64) string {
	if offset == 0 {
		return filepath.Join(s.dirPath, s.baseName+".log")
	}
	return filepath.Join(s.dirPath, fmt.Sprintf("%s-%010d.log", s.baseName, offset))
}

func (s *Store) getIdxPath(offset uint64) string {
	if offset == 0 {
		return filepath.Join(s.dirPath, s.baseName+".idx")
	}
	return filepath.Join(s.dirPath, fmt.Sprintf("%s-%010d.idx", s.baseName, offset))
}

func (s *Store) recoverState() error {
	maxOffset := int64(-1) // Start at -1 so first message gets ID 0

	// Scan all segments to find the highest offset
	for _, seg := range s.segments {
		idxInfo, err := seg.IdxFile.Stat()
		if err != nil {
			continue
		}

		if idxInfo.Size() == 0 {
			continue
		}

		// Each index entry is 16 bytes (8 bytes ID + 8 bytes position)
		numEntries := int64(idxInfo.Size() / 16)
		if numEntries == 0 {
			continue
		}

		// Read last entry to get the highest ID in this segment
		lastEntry := make([]byte, 16)
		if _, err := seg.IdxFile.ReadAt(lastEntry, idxInfo.Size()-16); err != nil {
			continue
		}

		lastID := int64(binary.BigEndian.Uint64(lastEntry[0:8]))
		if lastID > maxOffset {
			maxOffset = lastID
		}
	}

	s.nextID = uint64(maxOffset + 1)

	// Validate WAL: check index/log consistency in active segment
	if err := s.validateWAL(); err != nil {
		return err
	}

	// Calculate current size of active segment
	logInfo, _ := s.active.LogFile.Stat()
	s.currentSize = logInfo.Size()

	return nil
}

func (s *Store) validateWAL() error {
	idxInfo, err := s.active.IdxFile.Stat()
	if err != nil || idxInfo.Size() == 0 {
		return nil
	}

	logInfo, err := s.active.LogFile.Stat()
	if err != nil {
		return nil
	}

	// Read last index entry
	lastEntry := make([]byte, 16)
	if _, err := s.active.IdxFile.ReadAt(lastEntry, idxInfo.Size()-16); err != nil {
		return nil
	}

	logPos := binary.BigEndian.Uint64(lastEntry[8:16])

	// Try to read from that position in log
	lenBuf := make([]byte, 4)
	n, err := s.active.LogFile.ReadAt(lenBuf, int64(logPos))
	if err != nil && err != io.EOF {
		// Log file is corrupted, truncate it
		s.active.LogFile.Truncate(int64(logPos))
		s.active.IdxFile.Truncate(idxInfo.Size() - 16)
		return nil
	}

	if n != 4 {
		// Incomplete record, truncate both files
		s.active.LogFile.Truncate(int64(logPos))
		s.active.IdxFile.Truncate(idxInfo.Size() - 16)
		return nil
	}

	msgLen := binary.BigEndian.Uint32(lenBuf)
	expectedEndPos := int64(logPos) + 4 + int64(msgLen)

	if expectedEndPos > logInfo.Size() {
		// Incomplete message, truncate
		s.active.LogFile.Truncate(int64(logPos))
		s.active.IdxFile.Truncate(idxInfo.Size() - 16)
	}

	return nil
}

func (s *Store) findSegment(id uint64) *Segment {
	for i := len(s.segments) - 1; i >= 0; i-- {
		if s.segments[i].BaseOffset <= id {
			return s.segments[i]
		}
	}
	return nil
}

func (s *Store) rotate() error {
	// Close current active segment files
	s.active.LogFile.Close()
	s.active.IdxFile.Close()

	// Rename active files to new segment names
	oldLogPath := s.getLogPath(0)
	oldIdxPath := s.getIdxPath(0)

	newLogPath := s.getLogPath(s.nextID)
	newIdxPath := s.getIdxPath(s.nextID)

	if s.active.BaseOffset == 0 {
		// Only rename if this is the initial segment
		os.Rename(oldLogPath, newLogPath)
		os.Rename(oldIdxPath, newIdxPath)
		s.active.BaseOffset = s.nextID
	}

	// Create new active segment
	newSeg := &Segment{BaseOffset: 0}

	lf, err := os.OpenFile(oldLogPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}

	ifile, err := os.OpenFile(oldIdxPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		lf.Close()
		return err
	}

	newSeg.LogFile = lf
	newSeg.IdxFile = ifile

	s.segments = append(s.segments, newSeg)
	s.active = newSeg
	s.currentSize = 0

	return nil
}

func (s *Store) Append(data []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Rotation Logic: If current log + new data > maxBytes, rotate
	if s.currentSize+int64(len(data))+4 > s.maxBytes {
		if err := s.rotate(); err != nil {
			return 0, fmt.Errorf("rotation failed: %w", err)
		}
	}

	pos, _ := s.active.LogFile.Seek(0, io.SeekEnd)

	// Write Log
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	s.active.LogFile.Write(lenBuf)
	s.active.LogFile.Write(data)

	// Write Index
	idxBuf := make([]byte, 16)
	binary.BigEndian.PutUint64(idxBuf[0:8], s.nextID)
	binary.BigEndian.PutUint64(idxBuf[8:16], uint64(pos))
	s.active.IdxFile.Write(idxBuf)

	s.active.LogFile.Sync()
	s.active.IdxFile.Sync()

	id := s.nextID
	s.nextID++
	s.currentSize += int64(len(data)) + 4

	return id, nil
}

func (s *Store) ReadByID(id uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Find the segment that contains this ID
	seg := s.findSegment(id)
	if seg == nil {
		return nil, fmt.Errorf("message id %d not found: no segment available", id)
	}

	// Calculate offset within the segment
	idxOffset := int64((id - seg.BaseOffset) * 16)

	// Read from the correct segment's index
	idxEntry := make([]byte, 16)
	if _, err := seg.IdxFile.ReadAt(idxEntry, idxOffset); err != nil {
		return nil, fmt.Errorf("message id %d not found in segment %d: %w", id, seg.BaseOffset, err)
	}

	logPos := binary.BigEndian.Uint64(idxEntry[8:16])

	// Read from the correct segment's log
	lenBuf := make([]byte, 4)
	if _, err := seg.LogFile.ReadAt(lenBuf, int64(logPos)); err != nil {
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}
	msgLen := binary.BigEndian.Uint32(lenBuf)

	payload := make([]byte, msgLen)
	if _, err := seg.LogFile.ReadAt(payload, int64(logPos)+4); err != nil {
		return nil, fmt.Errorf("failed to read message payload: %w", err)
	}

	return payload, nil
}

func (s *Store) GetOffset(groupID string) uint64 {
	path := filepath.Join(s.dirPath, groupID+".offset")
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}

func (s *Store) CommitOffset(groupID string, id uint64) error {
	path := filepath.Join(s.dirPath, groupID+".offset")
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, id)
	return os.WriteFile(path, buf, 0o644)
}

func (s *Store) GetNextOffset(groupID string) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if group has a committed offset
	path := filepath.Join(s.dirPath, groupID+".offset")
	data, err := os.ReadFile(path)

	var lastOffset uint64
	if err == nil && len(data) == 8 {
		// Group has committed an offset, start from next
		lastOffset = binary.BigEndian.Uint64(data)
		return lastOffset + 1, nil
	}

	// No committed offset yet, start from 0
	return 0, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, seg := range s.segments {
		if seg.LogFile != nil {
			seg.LogFile.Close()
		}
		if seg.IdxFile != nil {
			seg.IdxFile.Close()
		}
	}
	return nil
}
