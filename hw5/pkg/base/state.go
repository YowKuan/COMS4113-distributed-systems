package base

import (
	"encoding/binary"
	"hash/fnv"
	"math/rand"
	//"fmt"
)

type State struct {
	nodes      map[Address]Node
	addresses  []Address
	blockLists map[Address][]Address
	Network    []Message
	Depth      int

	isDropOff   bool
	isDuplicate bool

	// inheritance
	Prev  *State
	Event Event

	// auxiliary information
	nodeHash    uint64
	networkHash uint64
	// If the network is sorted by hash, then no need to do it again
	// Thus, use this indicator to record if it has been sorted
	hashSorted bool
}

func NewState(depth int, isDropOff, isDuplicate bool) *State {
	return &State{
		nodes:       map[Address]Node{},
		blockLists:  map[Address][]Address{},
		Network:     make([]Message, 0, 8),
		Depth:       depth,
		isDropOff:   isDropOff,
		isDuplicate: isDuplicate,
		nodeHash:    0,
		networkHash: 0,
		hashSorted:  false,
	}
}

func (s *State) AddNode(address Address, node Node, blockList []Address) {
	if old, ok := s.nodes[address]; ok {
		s.nodeHash -= old.Hash()
	} else {
		s.addresses = append(s.addresses, address)
	}

	s.nodes[address] = node
	s.blockLists[address] = blockList
	s.nodeHash += node.Hash()
	return
}

func (s *State) UpdateNode(address Address, node Node) {
	if old, ok := s.nodes[address]; ok {
		s.nodeHash -= old.Hash()
	} else {
		panic("node does not exist")
	}

	s.nodes[address] = node
	s.nodeHash += node.Hash()
	s.Receive(node.HandlerResponse())
}

func (s *State) Nodes() map[Address]Node {
	return s.nodes
}

func (s *State) GetNode(address Address) Node {
	return s.nodes[address]
}

func (s *State) Send(meg Message) {
	s.Network = append(s.Network, meg)
	return
}

func (s *State) Clone() *State {
	newState := NewState(s.Depth+1, s.isDropOff, s.isDuplicate)
	for address, node := range s.nodes {
		newState.nodes[address] = node
	}

	// Assume these fields are identical among every state and their children
	newState.addresses = s.addresses
	newState.blockLists = s.blockLists

	for _, message := range s.Network {
		newState.Network = append(newState.Network, message)
	}

	newState.nodeHash = s.nodeHash
	newState.networkHash = s.networkHash
	newState.hashSorted = s.hashSorted

	return newState
}

func (s *State) Inherit(event Event) *State {
	newState := s.Clone()
	newState.Prev = s
	newState.Event = event
	return newState
}

// blockList will not be compared in the Equal operation
func (s *State) Equals(other *State) bool {
	if other == nil {
		return false
	}

	if len(s.nodes) != len(other.nodes) || len(s.Network) != len(other.Network) ||
		s.nodeHash != other.nodeHash || s.networkHash != other.networkHash {
		return false
	}

	for address, node := range s.nodes {
		otherNode, ok := other.nodes[address]
		if !ok || !node.Equals(otherNode) {
			return false
		}
	}

	if !s.hashSorted {
		hashSort(s.Network)
		s.hashSorted = true
	}

	if !other.hashSorted {
		hashSort(other.Network)
		other.hashSorted = true
	}

	for i, message := range s.Network {
		if !message.Equals(other.Network[i]) {
			return false
		}
	}

	return true
}

func isBlocked(blockList []Address, candidate Address) bool {
	if blockList == nil {
		return false
	}

	for _, addr := range blockList {
		if addr == candidate {
			return true
		}
	}

	return false
}

func (s *State) isLocalCall(index int) bool {
	message := s.Network[index]
	return message.From() == message.To()
}

func (s *State) isMessageReachable(index int) (bool, *State) {
	message := s.Network[index]
	to := message.To()

	_, ok := s.nodes[to]
	if !ok {
		newState := s.Inherit(UnknownDestinationEvent(message))
		newState.DeleteMessage(index)
		return false, newState
	}

	if isBlocked(s.blockLists[to], message.From()) {
		newState := s.Inherit(PartitionEvent(message))
		newState.DeleteMessage(index)
		return false, newState
	}

	return true, nil
}

func (s *State) HandleMessage(index int, deleteMessage bool) (result []*State) {
	//TODO: implement it
	newStates := make([]*State, 0, 4)
	message := s.Network[index]
	destination := message.To()
	handler := s.nodes[destination]

	newNodes := handler.MessageHandler(message)

	for _, node := range newNodes{
		newState := s.Inherit(HandleEvent(message))

		newState.UpdateNode(destination, node)
		if deleteMessage{
			newState.DeleteMessage(index)
		}
		newStates = append(newStates, newState)
	}

	//panic("implement me")
	return newStates
}

func (s *State) DeleteMessage(index int) {
	// Remove the i-th message
	message := s.Network[index]
	s.Network[index] = s.Network[len(s.Network)-1]
	s.Network = s.Network[:len(s.Network)-1]

	// remove from the hash
	s.networkHash -= message.Hash()
	s.hashSorted = false
}

func (s *State) Receive(messages []Message) {
	for _, message := range messages {
		s.Network = append(s.Network, message)
		s.networkHash += message.Hash()
		s.hashSorted = false
	}
}

func (s *State) NextStates() []*State {
	nextStates := make([]*State, 0, 4)

	for i := range s.Network {
		//fmt.Println("in next state handling", i)
		// check if it is a local call
		if s.isLocalCall(i) {
			//fmt.Println("is local call", i)
			newStates := s.HandleMessage(i, true)
			nextStates = append(nextStates, newStates...)
			continue
		}

		// check Network Partition
		reachable, newState := s.isMessageReachable(i)
		if !reachable {
			//fmt.Println("is not reachable", i)
			nextStates = append(nextStates, newState)
			continue
		}

		// TODO: Drop off a message
		if s.isDropOff {
			//fmt.Println("is drop off", i)
			newState := s.Inherit(DropOffEvent(s.Network[i]))
			newState.DeleteMessage(i)
			nextStates = append(nextStates, newState)
			//fmt.Println(nextStates)
		}

		// TODO: Message arrives Normally. (use HandleMessage)
		newStates := s.HandleMessage(i, true)
		//fmt.Println("newState normal", newStates)
		for _, state := range newStates{
			nextStates = append(nextStates, state)
		}
		//fmt.Println("after normally", nextStates)


		// TODO: Message arrives but the message is duplicated. The same message may come later again
		// (use HandleMessage)
		if s.isDuplicate {
			newStates := s.HandleMessage(i, false)
			for _, state := range newStates{
				nextStates = append(nextStates, state)
			}
			//fmt.Println("after duplicate", nextStates)
		}

	}
	//fmt.Println("nextStates out of for loop", nextStates)
	// You must iterate through the addresses, because every iteration on map is random...
	// Weird feature in Go
	for _, address := range s.addresses {
		node := s.nodes[address]

		//TODO: call the timer (use TriggerNodeTimer)
		triggeredState := s.TriggerNodeTimer(address, node)
		for _, state := range triggeredState{
			nextStates = append(nextStates, state)
		}
	}
	//fmt.Println("nextStates after calling timer", nextStates)

	return nextStates
}

func (s *State) TriggerNodeTimer(address Address, node Node) []*State {
	newStates := make([]*State, 0, 4)

	newNodes := node.TriggerTimer()

	for _, node := range newNodes{
		newTimer := node.NextTimer()
		newState := s.Inherit(TriggerEvent(address, newTimer))

		newState.UpdateNode(address, node)

		newStates = append(newStates, newState)

	}

	//TODO: implement it
	//panic("implement me")
	return newStates

}

func (s *State) RandomNextState() *State {
	timerAddresses := make([]Address, 0, len(s.nodes))
	for addr, node := range s.nodes {
		if IsNil(node.NextTimer()) {
			continue
		}
		timerAddresses = append(timerAddresses, addr)
	}
	if len(s.Network) + len(timerAddresses) == 0 {
		newState := s.Inherit(EmptyEvent())
		return newState
	}
	roll := rand.Intn(len(s.Network) + len(timerAddresses))

	if roll < len(s.Network) {
		// check Network Partition
		reachable, newState := s.isMessageReachable(roll)
		if !reachable {
			return newState
		}

		//TODO: handle message and return one state
		newStates := s.HandleMessage(roll, true)
		//fmt.Println("newStates in message", newStates)
		if len(newStates) > 0{
			return newStates[0]
		}else{
			return nil
		}
		
	}

	// TODO: trigger timer and return one state
	address := timerAddresses[roll-len(s.Network)]
	node := s.nodes[address]

	newStates := s.TriggerNodeTimer(address, node)
	//fmt.Println("newStates in trigger", newStates)
	if len(newStates) > 0{
		return newStates[0]
	}else{
		return nil
	}
	


}

// Calculate the hash function of a State based on its nodeHash and networkHash.
// It doesn't consider the group information because we assume the group information does not change
// during the evaluation.
func (s *State) Hash() uint64 {
	b := make([]byte, 8)
	h := fnv.New64()

	binary.BigEndian.PutUint64(b, s.nodeHash)
	_, _ = h.Write(b)

	binary.BigEndian.PutUint64(b, s.networkHash)
	_, _ = h.Write(b)

	return h.Sum64()
}
