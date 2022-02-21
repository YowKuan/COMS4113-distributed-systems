package pbservice

import "hash/fnv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type UpdateSyncArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	DoHash bool
	HashVal int64
	Primary string

}

type UpdateSyncReply struct {
  Err Err
}

type PutAppendSyncReply struct {
	Err Err
}

type GetSyncArgs struct {
  Key string
  Primary string
}

type GetSyncReply struct {
  Err Err
  Value string
}

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  // You'll have to add definitions here.

  // Field names must start with capital letters,
  // otherwise RPC will break.
  HashVal int64
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
}

type ReplicateArgs struct {
  KeyValStore map[string]string
  HashVals map[int64]string
}

type ReplicateReply struct {
  Err Err
}

type GetReply struct {
  Err Err
  Value string
}


// Your RPC definitions here.

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

