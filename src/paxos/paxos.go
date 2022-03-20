package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"
import "math"

type Fate int


type Instance struct { // key: n_a, value: v_a
	fate        string
	n_p         int
	n_a         int
	v_a         interface{}
}


// const (
// 	Decided   Fate = iota + 1
// 	Pending        // not yet decided.
// 	Forgotten      // decided but forgotten.
// )

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here.
  instances sync.Map
  doneValues sync.Map

}

func (px *Paxos) ProposerPropose(seq int, v interface{}){
  var decided = false
  //Hint: a proposer needs a way to choose a higher proposal number than any seen so far. 
  //This is a reasonable exception to the rule that proposer and acceptor should be separate.
  // It may also be useful for the propose RPC handler to return the highest known proposal number if it rejects an RPC, to help the caller pick a higher one next time. 
  //The px.me value will be different in each Paxos peer, so you can use px.me to help ensure that proposal numbers are unique.
  var N = seq + px.me
  start_time := time.Now()

  for !decided {
    //Hint: the tester calls Kill() when it wants your Paxos to shut down; 
    //Kill() sets px.dead. You should check px.dead in any loops you have that might run for a while, 
    //and break out of the loop if px.dead is true. It’s particularly important to do this in any long-running threads you create.
		if time.Now().Sub(start_time) / time.Millisecond  > 100 {
      start_time = time.Now()
      if px.dead{
        return
      }
		}

    var v_p = v
    var reachMajority = false
    var highest_n = N
    //fmt.Println("Propose Prepare start")
    reachMajority, v_p, highest_n = px.ProposerPrepare(N, seq, v)
    if !reachMajority {
      //fmt.Println("majority not reached!")
      N = highest_n + 1
      time.Sleep(time.Millisecond * time.Duration(rand.Intn(500)))
      continue

    }
    //fmt.Println("Propose Accept Start.")

    if px.ProposerAccept(N, seq, v_p) == false {
			// if we failed the accept phase -> re-start from the prepare phase
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(500)))
			continue
		}

    //fmt.Println("Propose Decided Start.")
    // if px.ProposerDecided(N, seq, v_p) == true {
    //   decided = true
    // }

    // for decided == false {
    if px.ProposerDecided(N, seq, v_p) ==true {
      decided = true   
    } else{
      time.Sleep(time.Millisecond * time.Duration(rand.Intn(500)))
    }
    // }

    // sendDecided := 0
    // for px.ProposerDecided(N, seq, v_p) == false && sendDecided < 10 {
    //   fmt.Println("proposer tried times: ", sendDecided)
    //   sendDecided += 1
    //   time.Sleep(time.Millisecond * 100)
    // }
    // decided = true
    // fmt.Println(px.doneValues.Load(px.me))

  }
}

func (px *Paxos) ProposerPrepare(N int, seq int, v interface{}) (bool, interface{}, int) {
    var count = 0
    var max_n_a = -1
    var v_prime = v
    var highest_n = N
    var wg sync.WaitGroup

    for i, peer := range px.peers{
      wg.Add(1)
      go func (wg *sync.WaitGroup, i int, peer string, N int, seq int, v interface {}, count *int, max_n_a *int, v_prime *interface{}, highest_n *int){
          defer wg.Done()
          args := &PrepareArgs{seq, N}
          var reply PrepareReply
          var ok = false
          if i == px.me {
            px.AcceptorPrepare(args, &reply)
            ok = true
          } else{
            ok = call(peer, "Paxos.AcceptorPrepare", args, &reply)
          }

          if ok && reply.Err == "" {
            px.mu.Lock()
            *count += 1

            if reply.N_a > *max_n_a {
              *max_n_a = reply.N_a
              *v_prime = reply.V_a
            }
            //need continue dev
            px.UpdateDoneValue(reply.Z_i, i)
            px.mu.Unlock()
          } else {
            px.mu.Lock()
            if reply.Higher_N > *highest_n {
              *highest_n = reply.Higher_N
            }
            px.mu.Unlock()
          }
          //px.UpdateDoneValue(reply.Z_i, i)


    }(&wg, i, peer, N, seq, v, &count, &max_n_a, &v_prime, &highest_n)
  }
    wg.Wait()

    if count < (len(px.peers) + 1)/2 {
      return false, 0, highest_n
    } else {
      return true, v_prime, highest_n
    }
}

func (px *Paxos) ProposerAccept(N int, seq int, v_p interface{}) bool {
  var count = 0
  var wg sync.WaitGroup

  for i, peer := range px.peers {
    wg.Add(1)
    go func (wg *sync.WaitGroup, i int, peer string, N int, seq int, v_p interface {}, count *int){
      defer wg.Done()
      if *count >= (len(px.peers) + 1)/ 2{
        //fmt.Println("already majority, return !!")
        return
      }
      args := &AcceptArgs{seq, N, v_p}
      var reply AcceptReply
      var ok = false
      if i == px.me {
        px.AcceptorAccept(args, &reply)
        ok = true
      } else{
        ok = call(peer, "Paxos.AcceptorAccept", args, &reply)
      }

      if ok && reply.Err == "" {
				px.mu.Lock()
				*count += 1
        px.UpdateDoneValue(reply.Z_i, i)
				px.mu.Unlock()
			}
    }(&wg, i, peer, N, seq, v_p, &count)

  }
  wg.Wait()
	if count < (len(px.peers) + 1)/ 2 {
		return false // no majority
	}
	return true
}

func (px *Paxos) ProposerDecided(N int, seq int, v_p interface{}) bool {
  var count = 0
  var wg sync.WaitGroup
  for i, peer := range px.peers {
    wg.Add(1)
    go func (wg *sync.WaitGroup, i int, peer string, N int, seq int, v_p interface {}, count *int){
      defer wg.Done()
      args := &DecidedArgs{seq, N, v_p}
      var reply DecidedReply
      var ok = false
      //fmt.Println("111")
      if i == px.me {
        //fmt.Println("222")
        px.AcceptorDecided(args, &reply)
        ok = true
      } else{
        //fmt.Println("333")
        ok = call(peer, "Paxos.AcceptorDecided", args, &reply)
      }

      if ok && reply.Err == "" {
				px.mu.Lock()
				*count += 1
				px.mu.Unlock()
			}
    }(&wg, i, peer, N, seq, v_p, &count)

  }
  wg.Wait()
	if count < len(px.peers) {
		return false 
	}
	return true
}

func (px *Paxos) UpdateDoneValue(z_i int, i int){
  //fmt.Println("Update done value...")
  old_z_i, _ := px.doneValues.Load(i)
  if z_i > old_z_i.(int) {
    px.doneValues.Store(i, z_i)
  }

}

func (px *Paxos) AcceptorPrepare(args *PrepareArgs, reply *PrepareReply) error {
  //fmt.Println("Acceptor prepare function start")
  ins, loaded := px.instances.LoadOrStore(args.Seq, &Instance{fate: "Pending", n_p:-1, n_a:-1, v_a: nil})
  //check!
  inst := ins.(*Instance)
  //fmt.Println("loaded:", loaded, inst)
  //fmt.Println("1")
  if args.N > inst.n_p {
    //fmt.Println("2")
    //check!
    if inst.fate == "Decided"{
      px.instances.Store(args.Seq, &Instance{fate: "Decided", n_p:args.N, n_a:inst.n_a, v_a: inst.v_a})
    } else{
      px.instances.Store(args.Seq, &Instance{fate: "Pending", n_p:args.N, n_a:inst.n_a, v_a: inst.v_a})
    }
    
    //extract done value of paxos itself, and give it to the proposer who calls the acceptor.
    var doneValue, _ = px.doneValues.Load(px.me)
    reply.Z_i = doneValue.(int)
    reply.N_a = inst.n_a
		reply.V_a = inst.v_a

  } else {
    //fmt.Println("3")
    reply.Higher_N = inst.n_p
    reply.Err = "Refuse to accept"
  }
  return nil
}

func (px *Paxos) AcceptorAccept(args *AcceptArgs, reply *AcceptReply) error{
  ins, loaded := px.instances.LoadOrStore(args.Seq, &Instance{fate: "Pending", n_p: -1, n_a: -1, v_a: nil})
  

	inst := ins.(*Instance)
  //fmt.Println("loaded:", loaded, inst)

	if args.N >= inst.n_p {
    if inst.fate == "Decided"{
      px.instances.Store(args.Seq, &Instance{fate: "Decided", n_p: args.N, n_a: args.N, v_a: args.V_p})
    } else{
      px.instances.Store(args.Seq, &Instance{fate: "Pending", n_p: args.N, n_a: args.N, v_a: args.V_p})

    } 
		
    //extract done value of paxos itself, and give it to the proposer who calls the acceptor.
    var doneValue, _ = px.doneValues.Load(px.me)
    reply.Z_i = doneValue.(int)
	} else {
		reply.Err = "Acceptor didn't accept."
	}
	return nil

}

func (px *Paxos) AcceptorDecided(args *DecidedArgs, reply *DecidedReply) error {
  px.instances.Store(args.Seq, &Instance{fate: "Decided", n_p: args.N, n_a: args.N, v_a: args.V_p})
  //px.doneValues.Store(args.Seq, )
  return nil

}



//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  go func(seq int, v interface{}){
    if seq < px.Min() {
      return
    }
    px.ProposerPropose(seq, v)
  }(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  px.UpdateDoneValue(seq, px.me)

  mini := px.Min()
  //d_value, _ := px.doneValues.Load(px.me)
  //fmt.Println("My done value is: ", d_value)
  //fmt.Println("Minimum donevalue is:", mini)
  for i := 1; i < mini; i++ {
    //fmt.Println("delete seq number", i)
    px.instances.Delete(i)
  }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  var maxi = -1
  px.instances.Range(func(k, v interface{}) bool { // seq is the key
		if k.(int) > maxi {
			maxi = k.(int)
		}
		return true
	})
  return maxi
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  // You code here.
  var mini = math.MaxInt32

  px.doneValues.Range(func(k, v interface{}) bool { // seq is the key
		if v.(int) < mini {
			mini = v.(int)
		}
		return true
	})
  for i := 1; i < mini; i++ {
    //fmt.Println("delete seq number", i)
    px.instances.Delete(i)
  }
  return mini+1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  // If Status() is called with a sequence number less than Min(), 
  //Status() should return false (indicating no agreement).
  mini := px.Min()
  if seq < mini {
    return false, nil
  }
  ins, ok := px.instances.Load(seq)
	if ok {
      if ins.(*Instance).fate == "Decided"{
        //fmt.Println("Status check: decided")
        //fmt.Println(ins.(*Instance).v_a)
        //fmt.Println("Decided", ins.(*Instance).v_a, ins.(*Instance).n_a, px.me)
        return true, ins.(*Instance).v_a

      }else{
        //fmt.Println("Not yet decided", ins.(*Instance).v_a, ins.(*Instance).n_a, px.me)
        return false, nil
      }
    }
  return false, nil
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
  px.instances = sync.Map{}
	px.doneValues = sync.Map{}

  for i, _ := range px.peers {
		px.doneValues.Store(i, -1)
	}

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
