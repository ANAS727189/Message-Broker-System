# Message Broker System - Complete Architecture Guide

## Table of Contents
1. [System Overview](#system-overview)
2. [Core Components](#core-components)
3. [Data Structures](#data-structures)
4. [Protocol Design](#protocol-design)
5. [Data Flow](#data-flow)
6. [Segment Management](#segment-management)
7. [Consumer Group Tracking](#consumer-group-tracking)
8. [Recovery & Durability](#recovery--durability)
9. [Concurrency Model](#concurrency-model)
10. [File Layout](#file-layout)

---

## System Overview

The message broker system is a simplified Kafka-like distributed message queue with the following characteristics:

- **Persistent Storage**: Messages are written to disk with index files for fast lookup
- **Segment-Based Rotation**: Log files rotate when they exceed size limits
- **Consumer Groups**: Multiple consumers can process messages independently
- **Durability**: All writes are synced to disk
- **Recovery**: Complete state recovery on startup from disk artifacts
- **Binary Protocol**: Version-aware protocol for future compatibility

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                    Network Layer                        │
│                 (TCP :8080)                             │
└──────────────┬──────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────┐
│          Protocol Handler (handler.go)                  │
│  ┌─────────────┬─────────────┬────────────┬────────────┐│
│  │ Produce     │ Consume     │ Fetch      │ Commit     ││
│  │ Request     │ Request     │ Request    │ Request    ││
│  └─────┬───────┴─────┬───────┴──────┬─────┴──────┬─────┘│
│        │             │              │            │      │
└────────┼─────────────┼──────────────┼────────────┼──────┘
         │             │              │            │
┌────────▼─────────────▼──────────────▼────────────▼──────┐
│            Store Layer (store.go)                       │
│  ┌──────────────────────────────────────────────────────┤
│  │ Segment Management                                   │
│  │ ┌────────────────────────────────────────────────────│
│  │ │ • Segment Discovery (startup)                      │
│  │ │ • Segment Lookup (by offset)                       │
│  │ │ • Segment Rotation (when size exceeded)            │
│  │ │ • Segment Metadata (BaseOffset per segment)        │
│  └──────────────────────────────────────────────────────┤
│  │ Data Operations                                      │
│  │ ┌────────────────────────────────────────────────────│
│  │ │ • Append (write messages)                          │
│  │ │ • ReadByID (read across segments)                  │
│  │ │ • GetNextOffset (consumer group offset)            │
│  │ │ • CommitOffset (persist consumer offset)           │
│  └──────────────────────────────────────────────────────┤
│  │ Recovery                                             │
│  │ ┌────────────────────────────────────────────────────│
│  │ │ • DiscoverSegments (find all log files)            │
│  │ │ • RecoverState (restore nextID)                    │
│  │ │ • ValidateWAL (check index/log consistency)        │
│  └──────────────────────────────────────────────────────┘
└─────────────────────────────────────────────────────────┘
         │                            │
         │                            │
┌────────▼──────────────┐    ┌────────▼──────────────┐
│   Disk Storage        │    │  Consumer Offsets     │
│ ┌──────────────────┐  │    │ ┌──────────────────┐  │
│ │ messages.log     │  │    │ │ group-1.offset   │  │
│ │ messages.idx     │  │    │ │ group-2.offset   │  │
│ │                  │  │    │ │ ...              │  │
│ │ messages-5.log   │  │    │ └──────────────────┘  │
│ │ messages-5.idx   │  │    │  (8-byte offsets)    │
│ │                  │  │    │  persisted to disk   │
│ │ messages-10.log  │  │    │                      │
│ │ messages-10.idx  │  │    │                      │
│ └──────────────────┘  │    │                      │
│ (segments directory)  │    └──────────────────────┘
└───────────────────────┘
```

---

## Core Components

### 1. **Protocol Layer** (`internal/protocol/protocol.go`)

Defines the binary protocol for communication between clients and broker.

#### Request Header Structure
```
[version:4 bytes]
  ├─ 0x00000001 = Protocol version 1
  └─ Reserved for future versions

[correlationID:4 bytes]
  ├─ Unique ID to match responses to requests
  ├─ Allows multiplexing multiple requests
  └─ Used for debugging and request tracking

[clientIDLen:4 bytes]
  └─ Length of clientID string

[clientID:variable]
  ├─ e.g., "producer-1", "consumer-group-1"
  └─ Used for monitoring and logging

[command:1 byte]
  ├─ 1 = CommandProduce (write message)
  ├─ 2 = CommandConsume (read by ID)
  ├─ 3 = CommandCommit (save consumer offset)
  └─ 4 = CommandFetch (get next message for group)

[payloadLength:4 bytes]
  └─ Size of the payload data
```

#### Response Header Structure
```
[version:4 bytes]
  └─ Same version as request

[correlationID:4 bytes]
  └─ Echoes the correlationID from request

[status:1 byte]
  ├─ 0 = Success
  └─ 1 = Error/Not Found

[payloadLength:4 bytes]
  └─ Size of the response payload
```

**Key Functions:**
- `WriteRequestHeader()` - Serialize request header to network
- `ReadRequestHeader()` - Parse request header from network
- `WriteResponseHeader()` - Serialize response header to network
- `ReadResponseHeader()` - Parse response header from network

---

### 2. **Handler Layer** (`internal/server/handler.go`)

Processes incoming client requests and delegates to the store.

#### Main Handler Loop
```go
HandleConn(conn, store):
  1. Loop forever:
     a. ReadRequestHeader from connection
     b. Read payload data
     c. Switch on command type:
        - CommandProduce → handleProduce()
        - CommandConsume → handleConsume()
        - CommandCommit → handleCommit()
        - CommandFetch → handleFetch()
```

#### Request Handlers

**handleProduce()**
```
Input:  Payload = raw message bytes
Flow:
  1. Call store.Append(payload)
  2. Get returned offset ID
  3. Send response header (status=0)
  4. Send 8-byte message ID
Output: Client receives offset ID
```

**handleConsume()**
```
Input:  Payload = 8-byte message ID
Flow:
  1. Parse message ID from payload
  2. Call store.ReadByID(id)
  3. If found:
     - Send response header (status=0)
     - Send message data
  4. If not found:
     - Send response header (status=1)
Output: Client receives message or error
```

**handleFetch()**
```
Input:  Payload = [groupIDLen:4][groupID:variable]
Flow:
  1. Parse consumer group ID
  2. Call store.GetNextOffset(groupID)
     - Returns next unread offset for this group
  3. Call store.ReadByID(nextOffset)
  4. If message exists:
     - Send response header (status=0)
     - Send [offset:8][msgLen:4][message]
  5. If no message:
     - Send response header (status=1)
     - Send [offset:8] (next offset to wait for)
Output: Client receives message or position info
Note: This is broker-side offset tracking
```

**handleCommit()**
```
Input:  Payload = [groupIDLen:4][groupID:var][offset:8]
Flow:
  1. Parse group ID and offset
  2. Call store.CommitOffset(groupID, offset)
     - Persists to {groupID}.offset file
  3. Send response header (status=0)
Output: Offset committed to disk
```

---

### 3. **Store Layer** (`internal/store/store.go`)

Core data structure that manages all data persistence and retrieval.

#### Store Structure
```go
type Store struct {
    mu          sync.RWMutex    // Concurrency control
    dirPath     string          // Base directory for data
    baseName    string          // File prefix (e.g., "messages")
    segments    []*Segment      // All segment files
    active      *Segment        // Currently writing segment
    nextID      uint64          // Next message ID to assign
    maxBytes    int64           // Max bytes per segment (1MB)
    currentSize int64           // Current active segment size
}
```

#### Segment Structure
```go
type Segment struct {
    BaseOffset uint64      // First message ID in this segment
    LogFile    *os.File    // .log file (message data)
    IdxFile    *os.File    // .idx file (offset→position index)
}
```

---

## Data Structures

### 1. **Log File Format** (.log)

Each log file stores the actual message data.

```
[msgLen:4 bytes][msg data:msgLen]
[msgLen:4 bytes][msg data:msgLen]
[msgLen:4 bytes][msg data:msgLen]
...
```

**Example with messages "hello" (5 bytes), "world" (5 bytes):**
```
00 00 00 05 h e l l o 00 00 00 05 w o r l d
│───────┘ │─────────│ │───────┘ │─────────│
 length   message   length     message
```

**Why this design:**
- Variable-length messages
- Length prefix allows efficient parsing
- No delimiters needed

### 2. **Index File Format** (.idx)

Each index file maps message IDs to log file positions.

```
[messageID:8 bytes][logFilePosition:8 bytes]
[messageID:8 bytes][logFilePosition:8 bytes]
[messageID:8 bytes][logFilePosition:8 bytes]
...
```

**Example:**
```
Segment: messages-5.log (BaseOffset = 5)

Index entry 0: [ID=5][pos=0]        → message 5 is at position 0 in log
Index entry 1: [ID=6][pos=1000]     → message 6 is at position 1000 in log
Index entry 2: [ID=7][pos=2050]     → message 7 is at position 2050 in log
```

**Lookup algorithm for ID=6 in this segment:**
```go
idxOffset := (6 - 5) * 16  // (messageID - segmentBaseOffset) * 16 bytes
entry := indexFile[idxOffset : idxOffset+16]
logPosition := binary.BigEndian.Uint64(entry[8:16])
```

### 3. **Consumer Offset File Format** (.offset)

Tracks the last committed offset for each consumer group.

```
[offsetValue:8 bytes]
```

**Example:**
```
worker-group-1.offset  → 8 bytes = last message ID read by this group
```

---

## Protocol Design

### Wire Format Example

#### Producing a message "hello"

**Request:**
```
00 00 00 01           [version = 1]
00 00 00 01           [correlationID = 1]
00 00 00 10           [clientIDLen = 16]
70 72 6f 64 75 63 65 72 2d 31 2d 74 65 73 74    [clientID = "producer-1-test"]
01                    [command = CommandProduce]
00 00 00 05           [payloadLen = 5]
68 65 6c 6c 6f        [payload = "hello"]
```

**Response:**
```
00 00 00 01           [version = 1]
00 00 00 01           [correlationID = 1]
00                    [status = success]
00 00 00 08           [payloadLen = 8]
00 00 00 00 00 00 00 00  [message ID = 0]
```

#### Fetching next message for consumer group

**Request:**
```
00 00 00 01           [version = 1]
00 00 00 02           [correlationID = 2]
00 00 00 0b           [clientIDLen = 11]
63 6f 6e 73 75 6d 65 72 2d 31    [clientID = "consumer-1"]
04                    [command = CommandFetch]
00 00 00 0f           [payloadLen = 15]
00 00 00 0b           [groupIDLen = 11]
77 6f 72 6b 65 72 2d 67 72 6f 75 70    [groupID = "worker-group"]
```

**Response (message available):**
```
00 00 00 01           [version = 1]
00 00 00 02           [correlationID = 2]
00                    [status = success]
00 00 00 0d           [payloadLen = 13]
00 00 00 00 00 00 00 00  [offset = 0]
00 00 00 05           [msgLen = 5]
68 65 6c 6c 6f        [message = "hello"]
```

---

## Data Flow

### 1. PRODUCE Flow (Write a message)

```
Client                Broker                Store
  │                     │                    │
  ├─ Write ────────────>│                    │
  │  [cmd][payload]     │                    │
  │                     ├─ handleProduce() ──┤
  │                     │                    ├─ Lock()
  │                     │                    │
  │                     │                    ├─ Check rotation
  │                     │                    │  (size + new msg > 1MB?)
  │                     │                    │
  │                     │                    ├─ If rotate:
  │                     │                    │  1. Close old segment files
  │                     │                    │  2. Rename to new names
  │                     │                    │  3. Create new segment 0
  │                     │                    │
  │                     │                    ├─ Write to log:
  │                     │                    │  [len][data]
  │                     │                    │
  │                     │                    ├─ Write to index:
  │                     │                    │  [id][pos]
  │                     │                    │
  │                     │                    ├─ Fsync log & index
  │                     │                    │
  │                     │                    ├─ Increment nextID
  │                     │                    │
  │                     │                    └─ Unlock()
  │                     │<─ return id ───────┤
  │<─ Response ────────┤
  │  [id = offset]     
```

**State Changes:**
- Before: nextID=0, log empty, index empty
- After:  nextID=1, log has "hello", index has [ID=0][pos=0]

**Example with rotation (messages: "hello" 500KB, "world" 600KB, "test" 400KB):**
```
1st message (500KB):   nextID=0 → 1, currentSize=500KB, active segment 0
2nd message (600KB):   currentSize+600KB = 1.1MB > maxBytes(1MB)
                       → ROTATE
                       • Segment 0 closed, renamed to segment-0
                       • New segment created (baseOffset=1)
                       • nextID=1 → 2, currentSize=600KB
3rd message (400KB):   nextID=2 → 3, currentSize=1MB
                       → ROTATE again
                       • Segment 1 closed, renamed to segment-1
                       • New segment created (baseOffset=3)
```

File layout after:
```
messages.log      (400KB, segment 2, IDs 3+)
messages.idx      (1 entry)
messages-1.log    (600KB, segment 1, ID 1)
messages-1.idx    (1 entry)
messages-0.log    (500KB, segment 0, ID 0)
messages-0.idx    (1 entry)
```

---

### 2. CONSUME Flow (Read specific message by ID)

```
Client                Broker                Store
  │                     │                    │
  ├─ Read(ID=0) ─────>│                    │
  │  [cmd][id]         │                    │
  │                     ├─ handleConsume() ─┤
  │                     │                    ├─ RLock()
  │                     │                    │
  │                     │                    ├─ findSegment(ID=0)
  │                     │                    │  • Loop through segments
  │                     │                    │  • Find: BaseOffset <= ID
  │                     │                    │  • Return: segment-0
  │                     │                    │
  │                     │                    ├─ Calculate index offset
  │                     │                    │  idxOff = (0-0)*16 = 0
  │                     │                    │
  │                     │                    ├─ Read index entry
  │                     │                    │  pos = segment[8:16]
  │                     │                    │
  │                     │                    ├─ Read log data
  │                     │                    │  [len][data]
  │                     │                    │
  │                     │                    └─ RUnlock()
  │<─ Response ───────┤
  │  [message data]
```

**ReadByID Algorithm:**
```
ReadByID(id=5) in example with segments 0, 1, 2:

1. findSegment(5):
   - Segment 0: BaseOffset=0 ≤ 5? YES → candidate
   - Segment 1: BaseOffset=1 ≤ 5? YES → better
   - Segment 2: BaseOffset=3 ≤ 5? YES → best
   - Return: Segment 2

2. Local index offset = (5 - 3) * 16 = 32 bytes
   (within segment 2's index file)

3. Read segment2.idx[32:48]:
   → logPosition = 1000

4. Read segment2.log[1000:1000+len]:
   → get message
```

---

### 3. FETCH Flow (Get next message for consumer group)

```
Client                Broker                Store
  │                     │                    │
  ├─ Fetch ──────────>│                    │
  │  [groupID]         │                    │
  │                     ├─ handleFetch() ───┤
  │                     │                    ├─ RLock()
  │                     │                    │
  │                     │                    ├─ GetNextOffset(groupID)
  │                     │                    │  1. Read groupID.offset
  │                     │                    │  2. If exists:
  │                     │                    │     return lastOffset+1
  │                     │                    │  3. If not exists:
  │                     │                    │     return 0
  │                     │                    │  → nextOffset = 0
  │                     │                    │
  │                     │                    ├─ Check if nextOffset exists
  │                     │                    │  (nextOffset < nextID?)
  │                     │                    │
  │                     │                    ├─ If exists:
  │                     │                    │  • ReadByID(nextOffset)
  │                     │                    │  • Return message
  │                     │                    │
  │                     │                    ├─ If not exists:
  │                     │                    │  • Return no data
  │                     │                    │  • Include nextOffset
  │                     │                    │
  │                     │                    └─ RUnlock()
  │<─ Response ───────┤
  │  [offset][msg]
  │  or [offset](empty)
```

**State Example:**
```
Scenario: Consumer group "workers" reading first time

Initial state:
  - nextID = 5
  - messages exist: 0, 1, 2, 3, 4
  - workers.offset does not exist

1st FETCH request:
  • GetNextOffset("workers")
    - No file → return 0
  • ReadByID(0)
    - Found → return message 0
  • Response: [offset=0][msg="hello"]

Consumer processes message 0, commits:
  • CommitOffset("workers", 0)
  • Write 0 to workers.offset file

2nd FETCH request:
  • GetNextOffset("workers")
    - File exists → return 0+1 = 1
  • ReadByID(1)
    - Found → return message 1
  • Response: [offset=1][msg="world"]

3rd FETCH request (after processing all messages):
  • GetNextOffset("workers")
    - File exists → return 4+1 = 5
  • ReadByID(5)
    - Not found (nextID=5, so max ID=4)
  • Response: [status=1][offset=5]
  • Consumer knows to wait for offset 5
```

---

### 4. COMMIT Flow (Save consumer progress)

```
Client                Broker                Store
  │                     │                    │
  ├─ Commit(gID, off) >│                    │
  │  [groupID][offset] │                    │
  │                     ├─ handleCommit() ──┤
  │                     │                    ├─ Lock()
  │                     │                    │
  │                     │                    ├─ Write to file:
  │                     │                    │  groupID.offset
  │                     │                    │  [8-byte offset]
  │                     │                    │
  │                     │                    ├─ Unlock()
  │                     │                    │
  │<─ Response ───────┤
  │  [status=0]
```

**File Layout:**
```
data/log/
  messages.log
  messages.idx
  messages-5.log
  messages-5.idx
  workers.offset (8 bytes: last committed offset)
  analytics.offset (8 bytes: last committed offset)
```

---

## Segment Management

### 1. **Segment Discovery** (Startup)

```
discoverSegments() Algorithm:

1. Read directory listing
2. Find all files matching pattern "messages*"
3. Parse filenames to extract baseOffset:
   - messages.log → baseOffset = 0
   - messages-5.log → baseOffset = 5
   - messages-10.log → baseOffset = 10
4. Sort by baseOffset ascending
5. Open all files (read mode)
6. Set active segment = last segment
7. Call recoverState()

Result: All segments loaded and ready for reads
```

**Example Discovery Process:**

```
Disk files:
  messages.log (1MB, contains IDs 20-40)
  messages.idx
  messages-10.log (1MB, contains IDs 10-19)
  messages-10.idx
  messages-20.log (512KB, active, contains IDs 41+)
  messages-20.idx

Steps:
1. Discover baseOffsets: 0, 10, 20
2. Sort: [0, 10, 20]
3. Load segments:
   Segment 0: messages.log [BaseOffset=0]
   Segment 1: messages-10.log [BaseOffset=10]
   Segment 2: messages-20.log [BaseOffset=20] ← active

Actual mapping:
   ID 5 → Segment 0 (0 ≤ 5)
   ID 15 → Segment 1 (10 ≤ 15 < 20)
   ID 35 → Segment 2 (20 ≤ 35)
```

### 2. **Segment Rotation** (Write exceeds size)

```
rotate() Algorithm:

1. Acquire exclusive lock (Lock())
2. Check if append would exceed maxBytes:
   if currentSize + newMsgSize + 4 > maxBytes:
3. Close current active segment files:
   activeLog.Close()
   activeIdx.Close()
4. Rename files to baseOffset name:
   messages.log → messages-{nextID}.log
   messages.idx → messages-{nextID}.idx
5. Create new fresh segment (BaseOffset=0):
   Create new messages.log
   Create new messages.idx
6. Set active = newSegment
7. Reset currentSize = 0
8. Release lock (Unlock())

Result: New segment created, old persisted as rotated segment
```

**Visual Example:**

```
Before Rotation:
  Active Segment (BaseOffset=0):
    nextID=1, currentSize=1.1MB
    messages.log (1.1MB)
    messages.idx
    
Before Append:
  Incoming message: 500KB
  Check: 1.1MB + 500KB > 1MB? YES → ROTATE

Rotation Steps:
  1. messages.log → messages-1.log
  2. messages.idx → messages-1.idx
  3. Create new messages.log (empty)
  4. Create new messages.idx (empty)

After Rotation:
  Rotated Segment (BaseOffset=1):
    messages-1.log (1.1MB) ← now archived
    messages-1.idx
    
  Active Segment (BaseOffset=0):
    nextID=1, currentSize=0
    messages.log (empty, ready for appends)
    messages.idx (empty)
    
Then Append:
  Write 500KB to active segment
  nextID=1 → 2, currentSize=500KB
```

### 3. **Segment Lookup** (Find segment for ID)

```
findSegment(targetID) Algorithm:

Loop from last segment to first:
  For each segment in reverse:
    if segment.BaseOffset <= targetID:
      return segment  ← Found! This segment contains targetID

If no segment found:
  return nil  ← ID too old or doesn't exist
```

**Why Reverse Loop?**
- Segments are sorted by BaseOffset
- Highest BaseOffset is most likely to match
- Early termination for performance
- Example: findSegment(45) with segments [0, 10, 20]:
  - Check 20: 20 ≤ 45? YES → return segment[20]
  - Never need to check 10, 0

---

## Consumer Group Tracking

### How It Works

```
Consumer Group Offset Tracking:

1. Each consumer group is independent
2. Offsets persisted in separate files:
   {groupID}.offset = 8 bytes = last committed offset

3. GetNextOffset(groupID):
   - If file exists: return lastOffset + 1
   - If file not exists: return 0 (start from beginning)

4. CommitOffset(groupID, offset):
   - Write offset to {groupID}.offset file
   - Next GetNextOffset call returns offset + 1

5. Multiple groups work independently:
   group-1.offset = 5 (can be at message 5)
   group-2.offset = 10 (can be at message 10)
   Both groups can process same messages at different speeds
```

### Example Scenario

```
Initial state:
  Messages: 0="hello", 1="world", 2="foo"
  nextID = 3
  No groups

Group A joins:
  1. FETCH → GetNextOffset("A") → 0 (no file)
  2. Receive message 0
  3. COMMIT(offset=0) → create A.offset with value 0
  
  A.offset file: [00 00 00 00 00 00 00 00]

Group B joins:
  1. FETCH → GetNextOffset("B") → 0
  2. Receive message 0
  3. COMMIT(offset=0) → create B.offset
  
Group A continues:
  1. FETCH → GetNextOffset("A") → 1 (read A.offset)
  2. Receive message 1
  3. COMMIT(offset=1) → update A.offset
  
  A.offset file: [00 00 00 00 00 00 00 01]

Now:
  A reads next offset 2
  B reads next offset 1
  Independent progress!
```

---

## Recovery & Durability

### 1. **Startup Recovery Process**

```
Server startup:
  1. Create Store
     ↓
  2. discoverSegments()
     • List all files in data/log directory
     • Parse segment files
     • Load all .log and .idx files
     ↓
  3. recoverState()
     • Loop through all segments
     • Find highest message ID
     • Set nextID = highestID + 1
     • Call validateWAL()
     ↓
  4. validateWAL()
     • Read last index entry
     • Verify log at that position is readable
     • Check message length is valid
     • If corrupted:
       - Truncate both log and idx
       - Continue with valid data
     ↓
  5. Ready to accept connections
     nextID is correct
     All segments loaded
     Corruption detected and fixed
```

### 2. **Crash Recovery Example**

```
Scenario: Server crashes mid-write

Before crash:
  Messages 0, 1, 2 successfully written
  Message 3 being appended:
    - Write to log [len][data] - COMPLETE
    - Write to index [id][pos] - COMPLETE
    - Fsync log - COMPLETE
    - Fsync index - COMPLETE
    - Update nextID - CRASH! (before nextID++ executed)

On disk after crash:
  messages.log - has all 4 messages, but last one not in index
  messages.idx - has entries for 0, 1, 2 only

Recovery process:
  1. discoverSegments() → loads all files
  2. recoverState():
     • Scans index: finds highest ID = 2
     • Sets nextID = 3
     • Calls validateWAL()
  3. validateWAL():
     • Reads last index entry (ID=2)
     • Reads corresponding log position
     • Message at that position is complete
     • No corruption detected
  4. Message 3 is lost BUT:
     • No data corruption
     • nextID=3 is correct for next append
     • All committed data survives

Recovery process (alternative crash):
  Server crashes during partial index write:
  
  Before crash:
    Index partially written (only 8 bytes)
    
  On disk:
    messages.idx - corrupted (incomplete entry)
    
  Recovery:
    validateWAL() detects incomplete index entry
    Truncates index to last complete entry
    Sets position correctly
    Next append starts fresh
```

### 3. **Write Durability**

```
Append Operation:

1. Lock() - Exclusive write lock
2. Check rotation needed
3. Seek to end of log
4. Write 4-byte length
5. Write message data
6. Write 8-byte ID to index
7. Write 8-byte position to index
8. Fsync log - WAIT FOR DISK
9. Fsync index - WAIT FOR DISK
10. Increment nextID
11. Unlock()

After fsyncs complete:
  • Data is physically on disk
  • Survives any crash
  • Consumer can read immediately
  
Why double fsync?
  • Log has actual message data
  • Index has metadata
  • Both must be consistent
  • Separate writes allow independent recovery
```

---

## Concurrency Model

### Lock Strategy

```
Type: sync.RWMutex

Readers (RLock):
  • ReadByID() - multiple concurrent reads OK
  • GetNextOffset() - multiple concurrent reads OK
  • GetOffset() - multiple concurrent reads OK
  
Writers (Lock):
  • Append() - exclusive, one writer at a time
  • CommitOffset() - technically not locked (file write)
  • rotate() - exclusive during rotation

Why RWMutex?
  • High read concurrency needed
  • Multiple consumers can read simultaneously
  • Writes are less frequent
  • Writes must be exclusive for data consistency
```

### Example Concurrency

```
Timeline:

t0: Consumer 1 reads (RLock)
t1: Consumer 2 reads (RLock)     ← Allowed, concurrent
t2: Consumer 3 reads (RLock)     ← Allowed, concurrent
t3: Producer tries write (Lock)  ← BLOCKS, waiting for all readers
t4: Consumer 1 releases (RUnlock)
t5: Consumer 2 releases (RUnlock)
t6: Consumer 3 releases (RUnlock)
t7: Producer acquires (Lock)     ← Now exclusive
t8: Producer writes, fsyncs
t9: Producer releases (Unlock)
t10: Waiting readers acquire (RLock) ← Resume reading
```

---

## File Layout

### Directory Structure

```
/home/anas/Desktop/masterDrive/message-broker-system/
├── cmd/
│   ├── consumer/main.go
│   ├── producer/main.go
│   └── server/main.go
├── internal/
│   ├── protocol/protocol.go
│   ├── server/handler.go
│   └── store/store.go
├── data/
│   └── log/                    ← All persistent data
│       ├── messages.log        ← Active segment log
│       ├── messages.idx        ← Active segment index
│       ├── messages-5.log      ← Rotated segment 1
│       ├── messages-5.idx
│       ├── messages-10.log     ← Rotated segment 2
│       ├── messages-10.idx
│       ├── worker-group-1.offset    ← Consumer offset
│       ├── worker-group-2.offset    ← Consumer offset
│       └── analytics.offset         ← Consumer offset
├── go.mod
└── IMPLEMENTATION_SUMMARY.md
```

### File Naming Convention

```
Active Segment:
  messages.log      (currently writing)
  messages.idx      (current index)

Rotated Segments:
  messages-{N}.log  where N = nextID when rotation happened
  messages-{N}.idx

Consumer Offsets:
  {groupID}.offset

Example:
  If rotation happened when nextID=5:
    messages-5.log (BaseOffset=5)
    messages-5.idx
    
  Next rotation at nextID=10:
    messages-10.log (BaseOffset=10)
    messages-10.idx
```

### Disk Layout Example

```
Active session writes messages:
1. Append msg "hello" (5 bytes)
   → messages.log:  [00 00 00 05]hello
   → messages.idx:  [00 00 00 00 00 00 00 00][00 00 00 00 00 00 00 00]
   → nextID = 1

2. Append msg "world" (5 bytes)
   → messages.log:  [00 00 00 05]hello[00 00 00 05]world
   → messages.idx:  [00 00 00 00...][00 00 00 00...][00 00 00 01][00 00 00 09]

3. Rotation happens at nextID=2
   → Rename messages.log → messages-2.log
   → Rename messages.idx → messages-2.idx
   → Create new messages.log (empty)
   → Create new messages.idx (empty)

4. Append msg "test" (4 bytes)
   → messages.log:  [00 00 00 04]test
   → messages.idx:  [00 00 00 02][00 00 00 00]

Segments created:
  Segment 0 (IDs 0-1): messages-2.log
  Segment 1 (IDs 2+):  messages.log

Segments directory after:
  messages.log (8 bytes)
  messages.idx (16 bytes)
  messages-2.log (14 bytes)
  messages-2.idx (32 bytes)
```

---

## Request/Response Examples

### Example 1: Full Produce Workflow

```
CLIENT: Produce "hello"
───────────────────────

Request Binary:
[00 00 00 01]  version=1
[00 00 00 42]  correlationID=66
[0A]           clientIDLen=10
[70 72 6f 64 75 63 65 72 31]  "producer1"
[01]           command=Produce
[00 00 00 05]  payloadLen=5
[68 65 6c 6c 6f]  "hello"

BROKER: Handles request
──────────────────────

Parsing:
  version = 1 ✓
  correlationID = 66
  clientID = "producer1"
  command = 1 (Produce)
  payload = "hello"

Processing:
  store.Append("hello")
  • Lock acquired
  • Check rotation: 0 + 5+4 = 9 < 1MB ✓
  • Write log: [00 00 00 05]hello
  • Write index: [00 00 00 00 00 00 00 00][00 00 00 00 00 00 00 00]
  • Fsync log ✓
  • Fsync index ✓
  • nextID: 0 → 1
  • Unlock

Response Binary:
[00 00 00 01]  version=1
[00 00 00 42]  correlationID=66 (echo)
[00]           status=0 (success)
[00 00 00 08]  payloadLen=8
[00 00 00 00 00 00 00 00]  messageID=0

CLIENT: Receives response
────────────────────────

Parsing:
  version = 1 ✓
  correlationID = 66 (matches request)
  status = 0 (success)
  messageID = 0

Log output:
  Sent: 'hello' | Acknowledged at Offset: 0
```

### Example 2: Full Fetch Workflow

```
CLIENT: Fetch for group "workers"
────────────────────────────────

Request Binary:
[00 00 00 01]  version=1
[00 00 00 43]  correlationID=67
[07]           clientIDLen=7
[63 6f 6e 73 75 6d 65 72]  "consumer"
[04]           command=Fetch
[00 00 00 0f]  payloadLen=15
[00 00 00 07]  groupIDLen=7
[77 6f 72 6b 65 72 73]  "workers"

BROKER: Handles request
──────────────────────

Processing:
  1. ParseGroup: "workers"
  2. GetNextOffset("workers")
     • Read workers.offset
     • File doesn't exist (new group)
     • Return 0
  3. ReadByID(0)
     • findSegment(0): Found segment 0
     • Index offset: (0-0)*16 = 0
     • Read index[0:16]: pos=0
     • Read log[0:9]: [00 00 00 05]hello
     • Return "hello"
  4. Prepare response

Response Binary:
[00 00 00 01]  version=1
[00 00 00 43]  correlationID=67
[00]           status=0 (message found)
[00 00 00 0d]  payloadLen=13
[00 00 00 00 00 00 00 00]  offset=0
[00 00 00 05]  msgLen=5
[68 65 6c 6c 6f]  "hello"

CLIENT: Consumes message
───────────────────────

Parsing:
  offset = 0
  message = "hello"

Log output:
  Received: hello (offset: 0)

CLIENT: Commit offset
─────────────────────

Request Binary:
[00 00 00 01]  version=1
[00 00 00 44]  correlationID=68
[07]           clientIDLen=7
[63 6f 6e 73 75 6d 65 72]  "consumer"
[03]           command=Commit
[00 00 00 0f]  payloadLen=15
[00 00 00 07]  groupIDLen=7
[77 6f 72 6b 65 72 73]  "workers"
[00 00 00 00 00 00 00 00]  offset=0

BROKER: Handles commit
──────────────────────

Processing:
  1. ParseGroup: "workers", offset=0
  2. CommitOffset("workers", 0)
     • Write 0 to workers.offset file
     • [00 00 00 00 00 00 00 00]

Response:
[00 00 00 01]  version=1
[00 00 00 44]  correlationID=68
[00]           status=0 (success)
[00 00 00 00]  payloadLen=0

CLIENT: Confirms
──────────────

Log output:
  Successfully committed offset 0 for group workers
```

---

## Summary

This message broker system implements:

1. **Persistent Storage** via segment-based log files with index
2. **Durability** through fsync and WAL validation
3. **Recovery** by discovering and validating all segments on startup
4. **Scalability** through segment rotation
5. **Consumer Groups** with independent offset tracking
6. **Versioned Protocol** for forward compatibility
7. **Concurrency** via RWMutex for reader parallelism

The architecture prioritizes:
- **Correctness** - Data never lost or corrupted
- **Simplicity** - Clear segment/index/offset model
- **Recovery** - Complete restart from disk state
- **Extensibility** - Protocol version allows future enhancements
