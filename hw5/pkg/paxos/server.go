package paxos

import (
	"coms4113/hw5/pkg/base"
	//"fmt"
)

const (
	Propose = "propose"
	Accept  = "accept"
	Decide  = "decide"
)

type Proposer struct {
	N             int
	Phase         string
	N_a_max       int
	V             interface{}
	SuccessCount  int
	ResponseCount int
	// To indicate if response from peer is received, should be initialized as []bool of len(server.peers)
	Responses []bool
	// Use this field to check if a message is latest.
	SessionId int

	// in case node will propose again - restore initial value
	InitialValue interface{}
}

type ServerAttribute struct {
	peers []base.Address
	me    int

	// Paxos parameter
	n_p int
	n_a int
	v_a interface{}

	// final result
	agreedValue interface{}

	// Propose parameter
	proposer Proposer

	// retry
	timeout *TimeoutTimer
}

type Server struct {
	base.CoreNode
	ServerAttribute
}

func NewServer(peers []base.Address, me int, proposedValue interface{}) *Server {
	response := make([]bool, len(peers))
	return &Server{
		CoreNode: base.CoreNode{},
		ServerAttribute: ServerAttribute{
			peers: peers,
			me:    me,
			proposer: Proposer{
				InitialValue: proposedValue,
				Responses:    response,
			},
			timeout: &TimeoutTimer{},
		},
	}
}

func (server *Server) IndexSearch(query base.Address) int {
	for i, peer := range server.peers{
		if peer == query{
			return i
		}
	}
	return -1
}

func (server *Server) MessageHandler(message base.Message) []base.Node {
	from := message.From()
	to := message.To()

	// fmt.Println("to server:", to)

	okie := false
	//N_p_response := -1

	newNodes := make([]base.Node, 0, 3)
	newNode := server.copy()

	actualMessage0, ok0 := message.(*ProposeRequest)
	if ok0{
		// fmt.Println("-------------")
		// fmt.Println("propose request")
		// fmt.Println(from, to)
		// fmt.Println("message.N", actualMessage.N)
		// fmt.Println("newNode.ServerAttribute.n_p", newNode.ServerAttribute.n_p)
		// fmt.Println("-------------")


		if actualMessage0.N > newNode.ServerAttribute.n_p{
			newNode.n_p = actualMessage0.N
			okie = true
		}
		response := &ProposeResponse{
			CoreMessage: base.MakeCoreMessage(to, from),
			Ok  : okie,
			N_p : newNode.ServerAttribute.n_p,
			N_a : newNode.ServerAttribute.n_a,
			V_a : newNode.ServerAttribute.v_a,
			SessionId:   actualMessage0.SessionId,
		}
		responses := make([]base.Message, 0)
		responses = append(responses, response)
		newNode.SetResponse(responses)
		newNodes = append(newNodes, newNode)
	}


	actualMessage1, ok1 := message.(*ProposeResponse)
	if ok1{
		//fmt.Println("propose response", newNode.me)
		if actualMessage1.SessionId == newNode.proposer.SessionId && newNode.proposer.Phase == Propose{
			index := newNode.IndexSearch(from)

			//If responseCount Reached full, and I didn't get majority and the agreed value does not set, then retry
			if newNode.proposer.ResponseCount == len(newNode.peers) && newNode.proposer.SuccessCount <= len(newNode.peers) / 2{
				//fmt.Println("added retry")
				retryNode := newNode.copy()
				retryNode.AttemptPropose(Propose, newNode.n_p + 1, newNode.proposer.V, newNode.proposer.SessionId+1)
				newNodes = append(newNodes, retryNode)
			}

			if actualMessage1.Ok{
				if newNode.proposer.Responses[index] == false{
					newNode.proposer.Responses[index] = true
					newNode.proposer.ResponseCount += 1
					newNode.proposer.SuccessCount += 1
					if actualMessage1.N_a > newNode.proposer.N_a_max {
						newNode.proposer.N_a_max = actualMessage1.N_a
						newNode.proposer.V = actualMessage1.V_a
					}
				}	
				//wait
				//fmt.Println("added wait")
				waitNode := newNode.copy()
				newNodes = append(newNodes, waitNode)

				if newNode.proposer.SuccessCount > len(newNode.peers) / 2{
					accPhaseNode := newNode.copy()
					accPhaseNode.ProposerInit(Accept, newNode.proposer.N, newNode.proposer.V, newNode.proposer.SessionId)
					messages := make([]base.Message, 0)
					for _,  peer := range accPhaseNode.peers{
						accmessage := &AcceptRequest{
							CoreMessage: base.MakeCoreMessage(to, peer),
							N:           accPhaseNode.proposer.N,
							V:			 accPhaseNode.proposer.V,
							SessionId:   accPhaseNode.proposer.SessionId,
						}
						messages = append(messages, accmessage)
					}
					//accept
					//fmt.Println("added accept")
					accPhaseNode.SetResponse(messages)
					newNodes = append(newNodes, accPhaseNode)

				}
				//retry
				// fmt.Println("added retry1")
				// retryNode1 := newNode.copy()
				// retryNode1.AttemptPropose(Propose, retryNode1.n_p + 1, retryNode1.proposer.V, retryNode1.proposer.SessionId+1)
				// newNodes = append(newNodes, retryNode1)

			}else{
				//wait
				//fmt.Println("added wait1")
				if newNode.proposer.Responses[index] == false{
					newNode.proposer.Responses[index] = true
					newNode.proposer.ResponseCount += 1
				}
				waitNode1 := newNode.copy()
				newNodes = append(newNodes, waitNode1)

				//retry
				//fmt.Println("added retry2")
				retryNode2 := newNode.copy()
				retryNode2.AttemptPropose(Propose, retryNode2.n_p + 1, retryNode2.proposer.V, retryNode2.proposer.SessionId+1)
				newNodes = append(newNodes, retryNode2)

			}
		}else{
			waitNode2 := newNode.copy()
			newNodes = append(newNodes, waitNode2)

		}

	
	}

	actualMessage2, ok2 := message.(*AcceptRequest)
	if ok2{
        if actualMessage2.N >= newNode.ServerAttribute.n_p{
			newNode.ServerAttribute.n_p = actualMessage2.N
			newNode.ServerAttribute.n_a = actualMessage2.N
			newNode.ServerAttribute.v_a = actualMessage2.V
			okie = true
		}
		response := &AcceptResponse{
			CoreMessage: base.MakeCoreMessage(to, from),
			Ok:          okie,
			N_p:         newNode.ServerAttribute.n_p,
			SessionId:   actualMessage2.SessionId,
		}
		responses := make([]base.Message, 0)
		responses = append(responses, response)
		newNode.SetResponse(responses)
		newNodes = append(newNodes, newNode)
	}

	actualMessage3, ok3 := message.(*AcceptResponse)
	if ok3{
		//fmt.Println("accept response", newNode.me)
		if actualMessage3.SessionId == newNode.proposer.SessionId && newNode.proposer.Phase == Accept{
			//If responseCount Reached full, and I didn't get majority and the agreed value does not set, then retry
			if newNode.proposer.ResponseCount == len(newNode.peers) && newNode.proposer.SuccessCount <= len(newNode.peers) / 2{
				//fmt.Println("added retry ")
				retryNode := newNode.copy()
				retryNode.AttemptPropose(Propose, newNode.n_p + 1, newNode.proposer.V ,newNode.proposer.SessionId+1)
				newNodes = append(newNodes, retryNode)
			}

			index := newNode.IndexSearch(from)

			if actualMessage3.Ok{
				if newNode.proposer.Responses[index] == false{
					newNode.proposer.ResponseCount += 1
					newNode.proposer.Responses[index] = true
					newNode.proposer.SuccessCount += 1
				}
				//wait
				//fmt.Println("added wait")
				waitNode := newNode.copy()
				newNodes = append(newNodes, waitNode)


				if newNode.proposer.SuccessCount > len(newNode.peers) / 2{
					//set myself to decided
					newNode.agreedValue = newNode.proposer.V
		
					decPhaseNode := newNode.copy()
					//fmt.Println("acc phase me", newNode.proposer.V)
					decPhaseNode.ProposerInit(Decide, newNode.proposer.N, newNode.proposer.V, newNode.proposer.SessionId)
		
					//fmt.Println(decPhaseNode.peers[decPhaseNode.me]==to)
					messages := make([]base.Message, 0)
					for _,  peer := range decPhaseNode.peers{
						decmessage := &DecideRequest{
							CoreMessage: base.MakeCoreMessage(decPhaseNode.peers[decPhaseNode.me], peer),
							V:			 decPhaseNode.proposer.V,
							SessionId:   decPhaseNode.proposer.SessionId,
						}
						messages = append(messages, decmessage)
					}
					//fmt.Println("added decidedReq")
					decPhaseNode.SetResponse(messages)
					newNodes = append(newNodes, decPhaseNode)
		
				}

				

			}else{
				//fmt.Println("added wait1")
				if newNode.proposer.Responses[index] == false{
					newNode.proposer.Responses[index] = true
					newNode.proposer.ResponseCount += 1
				}
				waitNode1 := newNode.copy()
				newNodes = append(newNodes, waitNode1)



				//fmt.Println("added retry1 ")
				// retryNode1 := newNode.copy()
				// retryNode1.AttemptPropose(Propose, retryNode1.n_p + 1, retryNode1.proposer.V ,retryNode1.proposer.SessionId+1)
				// newNodes = append(newNodes, retryNode1)

			}
		}
			

	}

	actualMessage4, ok4 := message.(*DecideRequest)
	if ok4{
		newNode.agreedValue = actualMessage4.V
		//newNode.SetSingleResponse(response)
		newNodes = append(newNodes, newNode)
	}

	return newNodes
}

func (server *Server) ProposerInit(phase string, N int, V interface{}, SessionId int){
	server.proposer.N = N
	//server.proposer.Phase
	server.proposer.V = V
	server.proposer.SuccessCount = 0
	server.proposer.Phase = phase
	server.proposer.ResponseCount = 0
	server.proposer.Responses = make([]bool, len(server.peers))
	server.proposer.SessionId = SessionId

}

func (server *Server) AttemptPropose(phase string, N int, V interface{}, SessionId int){
	server.ProposerInit(phase, N, V ,SessionId)
	messages := make([]base.Message, 0)
	for _,  peer := range server.peers{
		message := &ProposeRequest{
			CoreMessage: base.MakeCoreMessage(server.peers[server.ServerAttribute.me], peer),
			N:           server.proposer.N,
			SessionId:   server.proposer.SessionId,
		}
		messages = append(messages, message)
	}
	server.SetResponse(messages)

}

// To start a new round of Paxos.
func (server *Server) StartPropose(){
	//TODO: implement it
	//Initialize proposer values
	server.AttemptPropose(Propose, 1+server.me, server.proposer.InitialValue ,1)
}

// Returns a deep copy of server node
func (server *Server) copy() *Server {
	response := make([]bool, len(server.peers))
	for i, flag := range server.proposer.Responses {
		response[i] = flag
	}

	var copyServer Server
	copyServer.me = server.me
	// shallow copy is enough, assuming it won't change
	copyServer.peers = server.peers
	copyServer.n_a = server.n_a
	copyServer.n_p = server.n_p
	copyServer.v_a = server.v_a
	copyServer.agreedValue = server.agreedValue
	copyServer.proposer = Proposer{
		N:             server.proposer.N,
		Phase:         server.proposer.Phase,
		N_a_max:       server.proposer.N_a_max,
		V:             server.proposer.V,
		SuccessCount:  server.proposer.SuccessCount,
		ResponseCount: server.proposer.ResponseCount,
		Responses:     response,
		InitialValue:  server.proposer.InitialValue,
		SessionId:     server.proposer.SessionId,
	}

	// doesn't matter, timeout timer is state-less
	copyServer.timeout = server.timeout

	return &copyServer
}

func (server *Server) NextTimer() base.Timer {
	return server.timeout
}

// A TimeoutTimer tick simulates the situation where a proposal procedure times out.
// It will close the current Paxos round and start a new one if no consensus reached so far,
// i.e. the server after timer tick will reset and restart from the first phase if Paxos not decided.
// The timer will not be activated if an agreed value is set.
func (server *Server) TriggerTimer() []base.Node {
	if server.timeout == nil {
		return nil
	}

	subNode := server.copy()
	subNode.StartPropose()

	return []base.Node{subNode}
}

func (server *Server) Attribute() interface{} {
	return server.ServerAttribute
}

func (server *Server) Copy() base.Node {
	return server.copy()
}

func (server *Server) Hash() uint64 {
	return base.Hash("paxos", server.ServerAttribute)
}

func (server *Server) Equals(other base.Node) bool {
	otherServer, ok := other.(*Server)

	if !ok || server.me != otherServer.me ||
		server.n_p != otherServer.n_p || server.n_a != otherServer.n_a || server.v_a != otherServer.v_a ||
		(server.timeout == nil) != (otherServer.timeout == nil) {
		return false
	}

	if server.proposer.N != otherServer.proposer.N || server.proposer.V != otherServer.proposer.V ||
		server.proposer.N_a_max != otherServer.proposer.N_a_max || server.proposer.Phase != otherServer.proposer.Phase ||
		server.proposer.InitialValue != otherServer.proposer.InitialValue ||
		server.proposer.SuccessCount != otherServer.proposer.SuccessCount ||
		server.proposer.ResponseCount != otherServer.proposer.ResponseCount {
		return false
	}

	for i, response := range server.proposer.Responses {
		if response != otherServer.proposer.Responses[i] {
			return false
		}
	}

	return true
}

func (server *Server) Address() base.Address {
	return server.peers[server.me]
}
