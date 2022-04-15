package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "errors"
import "reflect"
import "strconv"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}


type Op struct {
  Operation string
  Value interface{}
  // Your definitions here.
}


type ShardState struct{
  //storing lastest data
  Database map[string]string
  // Key: OpID, val: either prev get results or preVal when puthash
  PreVals map[int64]string
  //map the client id to the maximum seq number from the client
  //MaxClientSeq map[int64]int
  //map the origin group to config number
  OriginGroupToConfig map[int64]int
}

type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID
  config shardmaster.Config
  kvseq int
  shardState [shardmaster.NShards]*ShardState

  // Your definitions here.
}

func (ss *ShardState) Init() {
	ss.Database = make(map[string]string)
  ss.PreVals = make(map[int64]string)
	ss.OriginGroupToConfig = make(map[int64]int)
}

func MakeShardState() *ShardState {
	var shardState ShardState
	shardState.Init()
	return &shardState
}

func (ss *ShardState) Bootstrap(origin *ShardState){

  for key, val := range origin.Database{
    ss.Database[key] = val
  }
  for key, val := range origin.PreVals{
    ss.PreVals[key] = val
  }
  for key, val := range origin.OriginGroupToConfig{
    ss.OriginGroupToConfig[key] = val
  }
}



func (kv *ShardKV) CheckComplete(seq int) (Op, error){
  
  sleepTime := 50 * time.Millisecond
  for {
    decided, op := kv.px.Status(seq)
    if decided{
      return op.(Op), nil
    }
    time.Sleep(sleepTime)
    if sleepTime < 1000 * time.Millisecond{
      sleepTime += (50 * time.Millisecond)
      // sleepTime *= 2
    }
  }
  //return Op{}, errors.New("Not complete")
}

func (kv *ShardKV) Execute(op Op) error{
  if op.Operation == "Get"{
    args := op.Value.(GetArgs)
    kv.shardState[args.Shard].PreVals[args.OpID] = kv.shardState[args.Shard].Database[args.Key]
  } else if op.Operation == "Put"{

    args := op.Value.(PutArgs)
    stateMachine := kv.shardState[args.Shard]
    stateMachine.Database[args.Key] = args.Value
    stateMachine.PreVals[args.OpID] = "nothing special"
    // kv.shardState[args.Shard].Database[args.Key] = args.Value
    // kv.shardState[args.Shard].PreVals[args.OpID] = "nothing special"


  } else if op.Operation == "PutHash"{

    args := op.Value.(PutArgs)
    stateMachine := kv.shardState[args.Shard]
    preVal, ok := stateMachine.Database[args.Key]
    if !ok{
      preVal = ""
    }
    //fmt.Println("previous value, OpID:", preVal, args.OpID)
    stateMachine.Database[args.Key] = strconv.FormatUint(uint64(hash(preVal+args.Value)), 10)
    stateMachine.PreVals[args.OpID] = preVal
    // preVal, ok := kv.shardState[args.Shard].Database[args.Key]
    // if !ok{
    //   preVal = ""
    // }
    // //fmt.Println("previous value, OpID:", preVal, args.OpID)
    // kv.shardState[args.Shard].Database[args.Key] = strconv.FormatUint(uint64(hash(preVal+args.Value)), 10)
    // kv.shardState[args.Shard].PreVals[args.OpID] = preVal
  } else if op.Operation == "Bootstrap"{

    reply := op.Value.(BootstrapReply)
    stateMachine := kv.shardState[reply.Shard]

		stateMachine.Bootstrap(&reply.ShardState)
    //kv.shardState[reply.Shard].Bootstrap(&reply.ShardState)

    if reply.ConfigNum > kv.shardState[reply.Shard].OriginGroupToConfig[reply.ProducerGID] {
			kv.shardState[reply.Shard].OriginGroupToConfig[reply.ProducerGID] = reply.ConfigNum
		}

  } else if op.Operation == "CatchUp"{

  } else if op.Operation == "Reconfig"{
    // fmt.Println("query newconfignum")
    args := op.Value.(ReconfigureArgs)
		kv.config = kv.sm.Query(args.NewConfigNum)
    // fmt.Println("For group", kv.gid)
    // fmt.Println("Updated confignum", kv.config.Num)
    // fmt.Println("Updated Group Info:", kv.config.Groups)
    // fmt.Println("Updated Shards Info:", kv.config.Shards)
    
  }
  return nil

}


//Using Paxos to maintain consistency within replica group
func (kv *ShardKV) Propose(xop Op) error{
  //fmt.Println("Proposing:", kv.kvseq, kv.gid, kv.me, xop)
  for {
   
    kv.px.Start(kv.kvseq + 1, xop)
    

    //wait until this sequence number is completed
    op, err := kv.CheckComplete(kv.kvseq + 1)
    //fmt.Println(kv.kvseq, kv.gid, op)
    //fmt.Println("Doing:", kv.kvseq, kv.gid, kv.me, op)
    if err != nil{
      return err
    }
    //Execute the operation
    kv.Execute(op)
    kv.kvseq += 1
    if op.Operation == "Reconfig"{
      args := op.Value.(ReconfigureArgs)
      if xop.Operation == "Get"{
        argsIncoming := xop.Value.(GetArgs)
        if argsIncoming.ConfigNum < args.NewConfigNum {

          kv.px.Done(kv.kvseq)
          return errors.New("ErrWrongGroup")
        } 
      }else if xop.Operation == "Put" || xop.Operation == "PutHash"{
        argsIncoming := xop.Value.(PutArgs)
        if argsIncoming.ConfigNum < args.NewConfigNum{
          kv.px.Done(kv.kvseq)
          return errors.New("ErrWrongGroup")
        } 
      }
    }
    //Check whether the executed operation is the one we're proposing. If yes, we're done.
    if reflect.DeepEqual(op, xop){
      break
    }
    kv.px.Done(kv.kvseq)
  }
  kv.px.Done(kv.kvseq)
  return nil

}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  // Your code here.

  if kv.config.Num != args.ConfigNum ||  kv.gid != kv.config.Shards[key2shard(args.Key)] {
    //FIRST CHECK ConfigNum is identical, second check whether this group serve this key.
    reply.Err = ErrWrongGroup
    return nil
    //return errors.New("Get: Wrong Group!")
  }
  //Then check whether this operationID has been done before.
  value, ok := kv.shardState[args.Shard].PreVals[args.OpID]
  if ok{
    reply.Err = OK
    reply.Value = value
    return nil
  }

  //come back: why pointer?
  op := Op{Operation:"Get", Value: *args}
  err := kv.Propose(op)

  if err != nil{
    if err.Error() == "ErrWrongGroup"{
      //fmt.Println("detected err wrong group:")
      reply.Err = ErrWrongGroup

    }
    

    return err
  }
  value, ok = kv.shardState[args.Shard].Database[args.Key]

  if ok{
    reply.Value = value
    reply.Err = OK
    //assign value to preval store, to handle duplicate request
    kv.shardState[args.Shard].PreVals[args.OpID] = value
  }else{
    reply.Value = ""
    reply.Err = ErrNoKey
  }

  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  // Your code here.

  if kv.config.Num != args.ConfigNum ||  kv.gid != kv.config.Shards[key2shard(args.Key)] {
    reply.Err = ErrWrongGroup
    // fmt.Println(kv.config.Num, args.ConfigNum)
    // fmt.Println(kv.gid, kv.config.Shards[key2shard(args.Key)])
    return nil
    //return errors.New("Put: Wrong Group!")
  }

  value, ok := kv.shardState[args.Shard].PreVals[args.OpID]
  if ok{
    reply.Err = OK
    reply.PreviousValue = value
    //fmt.Println("Duplicate operation - direct return")
    return nil
  }
 
  op := Op{Operation:"PutHash", Value: *args}
  if !args.DoHash{
    op = Op{Operation:"Put", Value: *args}
  }
  
  err := kv.Propose(op)

  if err != nil{
    if err.Error() == "ErrWrongGroup"{
      //fmt.Println("detected err wrong group:")
      reply.Err = ErrWrongGroup
    }
    //reply.Err = "Propose Failure"
    return err
  }
  reply.Err = OK
  if args.DoHash{
    //fmt.Println(args.Key, args.Value, args.OpID, kv.shardState[args.Shard].PreVals[args.OpID])
    reply.PreviousValue = kv.shardState[args.Shard].PreVals[args.OpID]
  }
  return nil
}


//come back
func (kv *ShardKV) Migrate(shard int) (bool, *BootstrapReply){
  gid := kv.config.Shards[shard]
  servers, ok := kv.config.Groups[gid]
  if !ok{
    return false, nil
  }

  if kv.shardState[shard].OriginGroupToConfig[gid] >= kv.config.Num{
    //Got repeated bootstrap operation
    //fmt.Println("repeated migrate operation")
    return true, nil

  }

  args := &BootstrapArgs{Shard: shard, ConfigNum: kv.config.Num}

  done0 := make(chan bool)
  var reply BootstrapReply

  go func (args *BootstrapArgs, reply *BootstrapReply, gid int64, servers []string) {
    for {
      for _, srv := range servers{
        reply.Shard = args.Shard
        newState := MakeShardState()
        ok := call(srv, "ShardKV.GetShardFromOrigin", args, &reply)

        if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
          newState.Bootstrap(&reply.ShardState)
          reply.ShardState = *newState
					reply.ProducerGID = gid
					reply.ConfigNum = args.ConfigNum
					done0 <- true
          
					return
        }
      }

    }
  }(args, &reply, gid, servers)

  select {
    case <-done0:
      return true, &reply
    case <-time.After(5000 * time.Millisecond):
      //fmt.Println("Migrate Did not done in 5 sec")
      return false, nil
    

  }
}

func (kv *ShardKV) GetShardFromOrigin(args *BootstrapArgs, reply *BootstrapReply) error{
  kv.mu.Lock()
  defer kv.mu.Unlock()

  
  if kv.config.Num <= args.ConfigNum {
    //The origin is not ready to give out its shard yet.
		reply.Err = "ErrNotReady"
    
		return nil
	}
  reply.ShardState.Init()
  // reply.ShardState.Database = make(map[string]string)
	// reply.ShardState.PreVals = make(map[int64]string)
	// reply.ShardState.OriginGroupToConfig = make(map[int64]int)

  for key, val := range kv.shardState[args.Shard].Database{
    reply.ShardState.Database[key] = val
  }
  for key, val := range kv.shardState[args.Shard].PreVals{
    reply.ShardState.PreVals[key] = val
  }
  for key, val := range kv.shardState[args.Shard].OriginGroupToConfig{
    reply.ShardState.OriginGroupToConfig[key] = val
  }
  //fmt.Println("get values from origin with shard:",kv.gid, args.Shard )
  reply.Err = OK

  return nil


}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  // Get the next config from shard master, if exists
  newConfig := kv.sm.Query(kv.config.Num + 1)

  if newConfig.Num != kv.config.Num {
    //fmt.Println("mine is not latest config, go and catch up!")

    op := Op{Operation: "CatchUp"}
    shardAssignToMe := make([]int, 0) 

    for i := 0; i < shardmaster.NShards; i++{
      prevShard := kv.config.Shards[i]
      if prevShard != 0 && prevShard != kv.gid && newConfig.Shards[i] == kv.gid {
        //fmt.Println("In for loop assign:", i)
        shardAssignToMe = append(shardAssignToMe, i)
      }
    }
    //fmt.Println("shardAssigntome:", shardAssignToMe)
    if len(shardAssignToMe) > 0 {
      kv.Propose(op)
      ops := make([]Op, 0)


      for _, i := range shardAssignToMe{
        //fmt.Println("migrating shard:", i)
        ok, reply := kv.Migrate(i)

        if ok{
          if reply != nil{

            op := Op{Operation: "Bootstrap", Value: *reply}
						ops = append(ops, op)
          }
        } else{
          wait := 100*(kv.gid-100+2)
          time.Sleep(time.Millisecond * time.Duration(wait) )
          //migrate fail
          return 
        }

      }
      for _, op := range ops {
        kv.Propose(op)
      }
      
    }

    //fmt.Println("Do reconfig")
    //kv.config = newConfig
    xop := Op{Operation: "Reconfig", Value: ReconfigureArgs{NewConfigNum: kv.config.Num + 1}}
    kv.Propose(xop)
  }
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})
  gob.Register(PutArgs{})
  gob.Register(PutReply{})
	gob.Register(GetArgs{})
  gob.Register(GetReply{})
	gob.Register(BootstrapReply{})
	gob.Register(ReconfigureArgs{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)
  kv.config = shardmaster.Config{Num: 0, Groups: map[int64][]string{}}
	for shard := 0; shard < shardmaster.NShards; shard++ {
		kv.shardState[shard] = MakeShardState()
	}


  // Your initialization code here.
  // Don't call Join().

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)


  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

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
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
