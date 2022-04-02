package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	//Hint: add field(s) to ViewServer to keep track of the current view.
	currentView *View

	//if primary is not Acked, and other service is pinging, we can only sent previousView Back
	previousView *View
	recentHeard  map[string]time.Time

	//Hint: you’ll need to keep track of whether the primary for the current view
	//has acknowledged it (in PingArgs.Viewnum).
	primary_acked bool

	//In other words, readers don't have to wait for each other. They only have to wait for writers holding the lock.
	//A sync.RWMutex is thus preferable for data that is mostly read,
	//and the resource that is saved compared to a sync.Mutex is time.
	idleServer map[string]bool
	rwlock     sync.RWMutex
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	//fmt.Println("ViewService: Now View:", vs.currentView )

	// Your code here.
	//Hint: you’ll want to add field(s) to ViewServer in server.go in order to
	//keep track of the most recent time at which the viewservice has heard a Ping from each server.
	//Perhaps a map from server names to time.Time. You can find the current time with time.Now().
	vs.rwlock.Lock()
	//fmt.Println("locking")
	defer vs.rwlock.Unlock()
	vs.recentHeard[args.Me] = time.Now()

	if vs.currentView == nil {
		//fmt.Println("currentView nil")
		//must use this line to initialize?
		vs.currentView = &View{0, "", ""}
		vs.previousView = &View{0, "", ""}
		vs.currentView.Viewnum = args.Viewnum + 1
		vs.currentView.Primary = args.Me
		//fmt.Println("currentView nil function ends")
	}
	if args.Me == vs.currentView.Primary {
		fmt.Println(args.Viewnum, vs.currentView.Viewnum)
		if args.Viewnum >= vs.previousView.Viewnum {
			fmt.Println("Primary ACKed current view")

			vs.primary_acked = true
			// fmt.Println("Past previous viewnum", vs.previousView.Viewnum)
			vs.previousView = vs.currentView
			// fmt.Println("Now previous viewnum", vs.previousView.Viewnum)
			// fmt.Println("Now Primary:", vs.currentView.Primary)
			// fmt.Println("Now Backup:", vs.currentView.Backup)
		}
		if args.Viewnum == 0 && vs.currentView.Viewnum > 1 {
			vs.currentView.Primary = vs.currentView.Backup
			vs.currentView.Backup = ""
			vs.promoteIdleTobackup()
			vs.currentView.Viewnum += 1
			vs.primary_acked = false
			//fmt.Println("Primary dead, backup become primary, now viewnum is:", vs.currentView.Viewnum)

		}

	} else if args.Me == vs.currentView.Backup {
		if args.Viewnum == 0 {
			vs.currentView.Backup = ""
			vs.promoteIdleTobackup()
			vs.currentView.Viewnum += 1

			vs.primary_acked = false
		}
		//fmt.Println("got ACK from backup")
	} else {
		if vs.previousView.Viewnum+1 > vs.currentView.Viewnum {
			if vs.currentView.Primary == "" {
				vs.currentView.Primary = args.Me
				vs.currentView.Viewnum += 1
				vs.primary_acked = false
			} else if vs.currentView.Backup == "" {
				vs.currentView.Backup = args.Me
				vs.currentView.Viewnum += 1
				vs.primary_acked = false

			} else {
				vs.idleServer[args.Me] = true
			}
		}
	}
	if args.Me == vs.currentView.Primary || vs.primary_acked {
		reply.View = *vs.currentView
	} else {
		reply.View = *vs.previousView
	}
	//The viewservice proceeds to a new view when
	//either it hasn’t received a Ping from the primary or backup for DeadPings PingIntervals,
	// or if the primary or backup crashed and restarted, or if there is no backup and there’s an idle server
	//(a server that’s been Pinging but is neither the primary nor the backup).

	//Hint: there may be more than two servers sending Pings.
	//The extra ones (beyond primary and backup) are volunteering to be backup if needed.
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.rwlock.Lock()

	//why use defer? https://stackoverflow.com/questions/47607955/use-of-defer-in-go
	defer vs.rwlock.Unlock()
	if vs.currentView != nil && vs.primary_acked {
		//fmt.Println("reply current view", vs.currentView)
		reply.View = *vs.currentView
	} else if vs.currentView != nil && !vs.primary_acked {
		//fmt.Println("reply previous view", vs.previousView)
		reply.View = *vs.previousView
	}

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//

func (vs *ViewServer) tick() {

	// Your code here.
	//Hint: your viewservice needs to make periodic decisions,
	//for example to promote the backup if the viewservice has missed DeadPings pings from the primary.
	// Add this code to the tick() function, which is called once per PingInterval.

	//check all alive servers, if didn't respond after PingInterval*DeadPings, then remove it from aliveServer
	//if the dead server is primary, replace it with backup, if dead server is backup, remove it from view
	vs.rwlock.Lock()
	defer vs.rwlock.Unlock()
	for key, val := range vs.recentHeard {
		if time.Now().After(val.Add(PingInterval * DeadPings)) {
			if key == vs.currentView.Primary {
				fmt.Println("Primary with key crashed:", key)
				if vs.previousView.Viewnum+1 > vs.currentView.Viewnum && vs.primary_acked {
					if vs.currentView.Backup != "" {
						vs.currentView.Primary = vs.currentView.Backup
						//fmt.Println("New Primary:", vs.currentView.Primary)
						vs.currentView.Backup = ""
						vs.promoteIdleTobackup()
						vs.currentView.Viewnum += 1
						vs.primary_acked = false
						//fmt.Println("Current new view is:", vs.currentView)
					}
				} else {
					fmt.Println("There is no backup to replace failed primary...")
				}
			} else if key == vs.currentView.Backup {
				fmt.Println("Backup crashed with key", key)
				vs.promoteIdleTobackup()
				vs.currentView.Viewnum += 1
				vs.primary_acked = false
				//fmt.Println("Current new view is:", vs.currentView)
			}
			delete(vs.recentHeard, key)
		}
	}
}
func (vs *ViewServer) promoteIdleTobackup() {
	for key, _ := range vs.idleServer {
		vs.currentView.Backup = key
		delete(vs.idleServer, key)
		break

	}

}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.dead = false
	vs.primary_acked = false
	vs.recentHeard = make(map[string]time.Time)
	vs.idleServer = make(map[string]bool)
	vs.currentView = nil
	vs.previousView = nil
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
