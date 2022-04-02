package pbservice
import (
	"errors"
)
import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	currentView *viewservice.View
	keyValStore map[string]string
	hashVals    map[int64]string
	rwlock      sync.RWMutex
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.rwlock.Lock()
	defer pb.rwlock.Unlock()
	// Your code here.
	if pb.me != pb.currentView.Primary{
		reply.Err = "Get: pb.me is not primary yet."
		return errors.New("Get: pb.me is not primary yet.")
	}
	//fmt.Println("Current key val", pb.keyValStore)
	reply.Value = pb.keyValStore[args.Key]

	sargs := GetSyncArgs{args.Key,pb.me}
	sreply := GetSyncReply{}

	ok := pb.currentView.Backup == ""
	for !ok{
		ok = call(pb.currentView.Backup, "PBServer.ForwardGet", sargs, &sreply)
		if ok{
			break
		} else{
			if sreply.Err == "ForwardGet: sender is not the current primary, reject." {
				reply.Err = sreply.Err
				return errors.New("GetTest: sender is not the current primary, reject.") // don't need to update anymore
			}
			time.Sleep(viewservice.PingInterval)

			newview, _ := pb.vs.Ping(pb.currentView.Viewnum)
			pb.updateBackup(newview)
			pb.currentView = &newview

			ok = pb.currentView.Backup == ""

		}
	}
	return nil
}
func (pb *PBServer) ReplicateProcess(args *ReplicateArgs, reply *ReplicateReply) error {
	pb.rwlock.Lock()
	defer pb.rwlock.Unlock()

	for key, val := range args.KeyValStore{
		pb.keyValStore[key] = val
	}
	for key, val := range args.HashVals{
		pb.hashVals[key] = val
	}
	//fmt.Println("Replicate value to backup: Complete" )
	return nil
}
func (pb *PBServer) updateBackup(newView viewservice.View){
	if pb.me == newView.Primary && pb.currentView.Backup != newView.Backup && newView.Backup != ""{
		args := &ReplicateArgs{pb.keyValStore, pb.hashVals}
		var reply ReplicateReply

		ok := false
		for ok == false {
			ok = call(newView.Backup, "PBServer.ReplicateProcess", args, &reply)
			if ok{
				break
			} else{
				time.Sleep(viewservice.PingInterval)
			}
		}

	}
}
func (pb *PBServer) ForwardGet(sargs *GetSyncArgs, sreply *GetSyncReply) error {
	pb.rwlock.Lock()
	defer pb.rwlock.Unlock()

	if sargs.Primary != pb.currentView.Primary {
		// the backup first need to check if the primary is still the current primary
		// e.g. split-brain: {s1, s3} -> s1 dies -> {s3, s2} -> s1 revokes
		// -> s1 still receives some requests from client -> so s1 forward to its cache backup, s3
		// -> s3 will tell s1 that "you are no longer the current primary now"
		// -> so finally s1 will reject the client's request
		sreply.Err = "ForwardGet: sender is not the current primary, reject."
		return errors.New("ForwardGet: sender is not the current primary, reject.")
	} else {
		// if it is the primary, then we do Get normally
		sreply.Value = pb.keyValStore[sargs.Key]
	}
	return nil

}
func (pb *PBServer) Forward(sargs *UpdateSyncArgs, sreply *UpdateSyncReply) error{
	pb.rwlock.Lock()
	defer pb.rwlock.Unlock()

	if sargs.Primary != pb.currentView.Primary{
		sreply.Err = "Forward: sender is not the current primary, reject."
		return errors.New("Forward: sender is not the current primary, reject.")
	} else {
		pb.Update(sargs.Key, sargs.Value, sargs.DoHash, sargs.HashVal)
	}
	return nil

}
func (pb *PBServer) PutExt(args *PutArgs, reply *PutReply) error {
	pb.rwlock.Lock()
	defer pb.rwlock.Unlock()

	if pb.me != pb.currentView.Primary {
		reply.Err = "PutExt: Not primary"
		return errors.New("PutExt: Not primary")
	}
	//If the hashVal already exists, just return that value and do not proceed.
	if val, ok := pb.hashVals[args.HashVal]; ok {
		reply.PreviousValue = val
		return nil
	} 
	reply.PreviousValue = pb.keyValStore[args.Key]
	pb.Update(args.Key, args.Value, args.DoHash, args.HashVal)
	fmt.Println("Cur map:", pb.keyValStore)


	sargs := UpdateSyncArgs{args.Key, args.Value, args.DoHash, args.HashVal, pb.me}
	sreply := UpdateSyncReply{}

	// if there is no backup currently -> don't do Forward
	ok := pb.currentView.Backup == ""

	for !ok {
		ok = call(pb.currentView.Backup, "PBServer.Forward", sargs, &sreply)

		if ok == true {
			break
		} else {
			if sreply.Err == "Forward: sender is not the current primary, reject." {
				reply.Err = sreply.Err
				return errors.New("PutExt: sender is not the current primary, reject.") // don't need to update anymore
			}

			time.Sleep(viewservice.PingInterval)
			// case 2. check if the backup was still alive
			// perform exactly the same as tick(). Cannot call it directly as we will acquire lock twice
			newview, _ := pb.vs.Ping(pb.currentView.Viewnum)
			pb.updateBackup(newview)
			pb.currentView = &newview

			ok = pb.currentView.Backup == ""
		}
	} 
	return nil

}

func (pb *PBServer) Update(key string, value string, dohash bool, hashVal int64){
	// no need to do lock.
	// Update() must be called by Forward() or PutExt() and they both did acquire the lock
	if dohash == false {
		pb.keyValStore[key] = value
	} else if dohash == true {
		// detect duplicates
		if _, ok := pb.hashVals[hashVal]; !ok {
			pb.hashVals[hashVal] = pb.keyValStore[key] 

			//fmt.Println("Previous key value:", key, pb.keyValStore[key] )
			if pb.keyValStore[key] == ""{
				pb.keyValStore[key] = fmt.Sprint(hash(value))
			}else{
				pb.keyValStore[key] = fmt.Sprint(hash(pb.keyValStore[key]+value))
			}
			
		}
	}
}
// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
	pb.rwlock.Lock()
	defer pb.rwlock.Unlock()

	newView, _ := pb.vs.Ping(pb.currentView.Viewnum)
	pb.updateBackup(newView)
	pb.currentView = &newView


}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.currentView = &viewservice.View{}
	pb.keyValStore = make(map[string]string)
	pb.hashVals = make(map[int64]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				//fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
