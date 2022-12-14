package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "strconv"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  OpID int64
  Op string
  Key string
  Value string
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  database sync.Map
  hashVals sync.Map
  seq int
}

func (kv *KVPaxos) DoPut(op string, Key string, Value string, opID int64) string{
  pre_val := ""
  _, hashValExist := kv.hashVals.Load(opID)
  if hashValExist{
    if op == "Put"{
      //fmt.Println("Duplicate Put operation! Reject!")
      return ""
    } else{
      previous_val, _ := kv.hashVals.Load(opID)
      return_val := previous_val.(string)
      return return_val
    }
  }
  
  
  loaded_val, ok := kv.database.Load(Key)
  // If key not in db, just store it.
  if op == "Put"{
    //just update its value
    kv.database.Store(Key, Value)
  } else {
    //do PutHash
    //retreive previous value
    if !ok{
      pre_val = ""
    }else{
      pre_val = loaded_val.(string)
    }
    
    _, ok := kv.hashVals.Load(opID)
    if !ok{
      kv.database.Store(Key, strconv.FormatUint(uint64(hash(pre_val+Value)), 10))
    }
    
    //fmt.Println("Previous value is:", pre_val)
    //kv.database.Store(key, fmt.Sprint(hash(value+pre_val)))
    //new_value, _ := kv.database.Load(Key)
    //fmt.Println("Updated value is:", new_value)


  }
  //to maintain at most once semantic.
  kv.hashVals.Store(opID, pre_val)
  return pre_val
}

func (kv *KVPaxos) CheckConsistency(cur_op Op) {
  //fmt.Println("check consistency start")
  //start_time := time.Now()

  to := 10 * time.Millisecond
  var consistent = false
  for {
    // if time.Now().Sub(start_time) / time.Millisecond  > 100 {
    //   start_time = time.Now()
    //   if kv.dead{
    //     return
    //   }
    // }
    if kv.dead{
      return 
    }
    decided, prev_op := kv.px.Status(kv.seq)
    if decided {
      // seq already decided. So we need to update our value according to the decided value of paxos server.
      prev_op := prev_op.(Op)

      // our job has been done!!! Our KVPaxos has catched up => break
      if cur_op.OpID == prev_op.OpID {
        break
      }
      //Update our KVPaxos
      if prev_op.Op == "Put" || prev_op.Op == "PutHash" {
        kv.DoPut(prev_op.Op, prev_op.Key, prev_op.Value, prev_op.OpID)
      }

      kv.seq += 1
      //continue check for next seq
      consistent = false
    } else{
      // The instance with seq = kv.seq has not been decided.
      // Start the paxos agreement process
      // Either learned that the value has been decided (We move to next seq and retry)
      // Or successfully perform the cur_op.
      if !consistent{
        kv.px.Start(kv.seq, cur_op)
        consistent = true
      }
      time.Sleep(to)
      if to < 1 * time.Second {
        to += 2 * time.Millisecond
      }


    }
  }
  //Operation done and KV is stored. Clean paxos server memory
  kv.px.Done(kv.seq)
  //Increment to next seq
  kv.seq += 1
  //fmt.Println("Check consistency ends")
}

func (kv *KVPaxos) DoGet(key string) string{
  val, ok := kv.database.Load(key)
  if ok{
    return val.(string)
  }else{
    return ""
  }
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  if kv.dead{
    reply.Err = "server dead"
    return nil
  }
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  op := Op{args.Hash, "Get", args.Key, ""}
  //fmt.Println("Get: check consistency start")
  kv.CheckConsistency(op)
  //fmt.Println("Get: check consistency ends")
  reply.Value = kv.DoGet(args.Key)
  //fmt.Println("Set Get reply value complete")

  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  if kv.dead{
    reply.Err = "server dead"
    return nil
  }
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  op := Op{args.Hash, args.Op, args.Key, args.Value}
  //fmt.Println("Put: check consistency start")
  kv.CheckConsistency(op)
  //fmt.Println("Put: check consistency end")
  pre_val := kv.DoPut(args.Op, args.Key, args.Value, args.Hash)
  //fmt.Println("DoPut Complete")
  reply.PreviousValue = pre_val

  

  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.seq = 0

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // go func() {
  //   for {
  //     decided, prev_op := kv.px.Status(kv.seq)
  //     if decided {
  //       // seq already decided. So we need to update our value according to the decided value of paxos server.
  //       prev_op := prev_op.(Op)
  //       //Update our KVPaxos
  //       if prev_op.Op == "Put" || prev_op.Op == "PutHash" {
  //         kv.DoPut(prev_op.Op, prev_op.Key, prev_op.Value, prev_op.OpID)
  //       }
  
  //       kv.seq += 1
  //       //continue check for next seq
  //   }

  // }
  // }()
  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

