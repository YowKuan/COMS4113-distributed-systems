package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math"
import "math/rand"
import "time"
import "errors"
import "reflect"

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  seq int
}


type Op struct {
  // Your data here.
  Operation string
  Args interface{}
}

func (sm *ShardMaster) BalanceShard(config *Config, leaveID int64){
  groupLen := len(config.Groups)
  avg := NShards / groupLen 

  for i:= 0; i < NShards; i++{
    if config.Shards[i] == leaveID{
      config.Shards[i] = 0
    }
  }
  shardCount := make(map[int64]int)
  for i:= 0; i < NShards; i++{
    shardCount[config.Shards[i]] += 1
  }

  for i:= 0; i < NShards; i++{
    shardGID := config.Shards[i]
    if shardGID == 0 || shardCount[shardGID] > avg{
      mini := math.MaxInt
      fewest := int64(-1)

      for currGid, _ := range config.Groups {
				// if init OR
				// group `currGid` is taking care of less # of shards
				// compared to minGidCount
				// update our best choice Gid (the one will MINIMUM count)
				if fewest == -1 || shardCount[currGid] < mini {
					fewest = currGid
					mini = shardCount[currGid]
				}
			}
      if shardGID == 0 {
				shardCount[shardGID] -= 1
			  shardCount[fewest] += 1
				config.Shards[i] = fewest
      } else{
        if shardCount[fewest] + 1 < shardCount[shardGID]{
          shardCount[fewest] += 1
          shardCount[shardGID] -= 1
          config.Shards[i] = fewest
        }
      }

    }
  }

}

func (sm *ShardMaster) CheckComplete(seq int) (Op, error){
  sleepTime := 10 * time.Millisecond
  for {
    decided, op := sm.px.Status(seq)
    if decided{
      return op.(Op), nil
    }
    time.Sleep(sleepTime)
    if sleepTime < 500 * time.Millisecond{
      sleepTime += (20 * time.Millisecond)
    }
  }
  return Op{}, errors.New("Not complete")

}

func (sm *ShardMaster) Execute(op Op){
  prevConfig := sm.configs[sm.seq]
  var newConfig Config
	newConfig.Num = prevConfig.Num
	newConfig.Groups = make(map[int64][]string)
	for k, v := range prevConfig.Groups {
		newConfig.Groups[k] = v
	}
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = prevConfig.Shards[i]
	}
  if op.Operation == "Join"{
    joinArgs := op.Args.(JoinArgs)
    newConfig.Groups[joinArgs.GID] = joinArgs.Servers
    newConfig.Num += 1

    sm.BalanceShard(&newConfig, 0)
  } else if op.Operation == "Leave"{
    leaveArgs := op.Args.(LeaveArgs)
    delete(newConfig.Groups, leaveArgs.GID)
    sm.BalanceShard(&newConfig, leaveArgs.GID)
    newConfig.Num += 1
  } else if op.Operation == "Move"{
    moveArgs := op.Args.(MoveArgs)
    newConfig.Shards[moveArgs.Shard] = moveArgs.GID
    newConfig.Num += 1
  }
  sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Propose(cur_op Op) error {
  for {
    sm.px.Start(sm.seq+1, cur_op)
    op, err := sm.CheckComplete(sm.seq+1)
    if err == nil{
      sm.Execute(op)
      sm.seq += 1
      if reflect.DeepEqual(op, cur_op){
        break
      }
      sm.px.Done(sm.seq)
    } else{
      return err
    }
  }
  sm.px.Done(sm.seq)
  return nil
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{Args: *args, Operation: "Join"}
	err := sm.Propose(op)
	if err != nil {
		return err
	}
	return nil

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{Args: *args, Operation:"Leave"}
  err := sm.Propose(op)

  if err != nil {
		return err
	}
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{Args: *args, Operation:"Move"}
  err := sm.Propose(op)

  if err != nil {
		return err
	}
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{Args: *args, Operation:"Query"}
  err := sm.Propose(op)
  if err != nil{
    return err
  }

  for i := 0; i < sm.seq; i++ {
		if sm.configs[i].Num == args.Num {
			reply.Config = sm.configs[i]
			//log.Printf("i=%v, num=%v", i, args.Num)
			return nil
		}
	}
	// args.Num == -1 OR args.Num is larger than any other Num in configs
	reply.Config = sm.configs[sm.seq]
	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})
  gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}
  //added
	//sm.configs[0].shards = [NShards]int64

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l
  //sm.seq = 0
  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
