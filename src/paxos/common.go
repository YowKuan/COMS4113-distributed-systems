package paxos

type Err string


type PrepareArgs struct {
	Seq         int
	N           int
}

type PrepareReply struct {
	Err         Err
	N           int
	N_a         int
	V_a         interface{}
	Z_i         int
	Higher_N    int
}

type AcceptArgs struct {
	Seq         int
	N           int
	V_p         interface{} // v prime
}

type AcceptReply struct {
	Err         Err
	N           int
	Z_i         int
}

type DecidedArgs struct {
	Seq           int
	N             int
	V_p           interface{}
}

type DecidedReply struct {
	Err         Err
}