package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
)

// Fill in the function to lead the program to a state where A2 rejects the Accept Request of P1
func ToA2RejectP1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Propose
		if valid {
			fmt.Println("... p1 entered Propose phase")
		}
		return valid
	}
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept
	}
	p1AcceptRejBys2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		valid := false
		otheroks := 0
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s2.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s2")
		}
		return valid
	}
	return []func(s *base.State) bool{
		p1PreparePhase,
		p1AcceptPhase,
		p1AcceptRejBys2,
	}
	// panic("fill me in")
}

// Fill in the function to lead the program to a state where a consensus is reached in Server 3.
func ToConsensusCase5() []func(s *base.State) bool {
	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Propose && s3.proposer.V == "v3"
		if valid {
			fmt.Println("... p3 entered Propose phase with proposed value v3")
		}
		return valid
	}
	p3LargerNP := func(s *base.State) bool {
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Propose && s3.proposer.N > s2.n_p
		if valid{
			fmt.Println("... p3 has larger N than s2's n_p")
		}
		return valid
	}
	s2Acceptp3Propose := func(s *base.State) bool {
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		otheroks := 0
		valid := false
		if s3.proposer.Phase == Propose && s3.proposer.V == "v3" {
			for _, m := range s.Network {
				resp, ok := m.(*ProposeResponse)
				if ok && resp.Ok && m.To() == s3.Address() && m.From() == s2.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid{
			fmt.Println("... p3 get propose ok from s2")
		}
		return valid
	}
	p3AcceptBys2 := func(s *base.State) bool {
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		otheroks := 0
		if s3.proposer.Phase == Accept && s3.proposer.V == "v3" {
			//fmt.Println("s3:v3")
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				
				if ok && resp.Ok && m.To() == s3.Address() && m.From() == s2.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p3 entered Accept phase with proposed value v3, also accepted by s2")
		}
		return valid
	}
	s3KnowConsensus := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.agreedValue == "v3" && s2.agreedValue == nil && s1.agreedValue == nil
		if valid {
			fmt.Println("s3 is the first to reach consensus")
		}
		return valid
	}
	return []func(s *base.State) bool{
		p3PreparePhase,
		p3LargerNP,
		s2Acceptp3Propose,
		p3AcceptBys2,
		s3KnowConsensus,
	}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected
func NotTerminate1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Propose
		if valid {
			fmt.Println("... p1 entered Propose phase")
		}
		return valid
	}
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept
		if valid{
			fmt.Println("... p1 reached accept phase")
		}
		
		return valid
		
	}
	p3LargerNP := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept && s3.n_p > s1.proposer.N
		if valid{
			fmt.Println("... s3 has larger n_p than p1's N")
		}
		return valid
	}
	p3p2p1LargerNP := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s1.proposer.Phase == Accept && s3.n_p > s1.proposer.N  && s2.n_p > s1.proposer.N && s1.n_p > s1.proposer.N
		if valid{
			fmt.Println("... s3 s2 s1 has larger n_p than p1's N")
		}
		return valid
	}
	p1AcceptRejBys2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		valid := false
		otheroks := 0
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s2.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s2")
		}
		return valid
	}
	p1AcceptRejBys3 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		otheroks := 0
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s3.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s3")
		}
		return valid
	}
	p1AcceptRejBys1 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := false
		otheroks := 0
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s1.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s1")
		}
		return valid
	}
	p1AcceptRejBys2s3 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		s2Rej := false
		s3Rej := false
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s2.Address() {
					s2Rej = true
				}
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s3.Address() {
					s3Rej = true
				}
			}
			if s2Rej && s3Rej{
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s2,s3")
		}
		return valid
	}
	p1AcceptRejBys1s2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		s1Rej := false
		s2Rej := false
		s3Rej := false
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s1.Address() {
					s1Rej = true
				}
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s2.Address() {
					s2Rej = true
				}
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s3.Address() {
					s3Rej = true
				}
			}
			if s1Rej && s2Rej && s3Rej{
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s1,s2")
		}
		return valid
	}

	p1AcceptRejByAll := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		// s2 := s.Nodes()["s2"].(*Server)
		// s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s1.proposer.Phase == Accept && s1.proposer.SuccessCount == 0{
			if s1.proposer.ResponseCount == 3{
				valid = true
				fmt.Println("... s1 rejected by everyone")

			}

		}

		return valid
	}
	return []func(s *base.State) bool{
		p1PreparePhase,
		p1AcceptPhase,
		p1AcceptRejBys2,
		p1AcceptRejBys3,
		p1AcceptRejBys1,
		p1AcceptRejBys2s3,
		p1AcceptRejBys1s2,
		p3LargerNP, 
		p3p2p1LargerNP,
		p1AcceptRejByAll ,

	}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P3 are rejected
func NotTerminate2() []func(s *base.State) bool {
	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Propose
		if valid {
			fmt.Println("... p3 entered Propose phase")
		}
		return valid
	}
	p3LargeNpInPropose := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Propose && s3.n_p < s3.proposer.N  && s2.n_p < s3.proposer.N && s1.n_p < s3.proposer.N
		if valid{
			fmt.Println("... s3 s2 s1 has smaller n_p than p3's N")
		}
		return valid
	}
	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept
		if valid{
			fmt.Println("... p3 reached accept phase")
		}
		
		return valid
		
	}
	s2s1LargerNP := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept && s2.n_p > s3.proposer.N && s1.n_p > s3.proposer.N
		if valid{
			fmt.Println("... s2 s1 has larger n_p than p3's N")
		}
		return valid
	}
	s3LargerNP := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept && s3.n_p > s3.proposer.N 
		if valid{
			fmt.Println("... s3 has larger n_p than p3's N")
		}
		return valid
	}

	p3AcceptRejBys1 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		otheroks := 0
		if s1.proposer.Phase == Accept && s3.proposer.V == "v3" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s3.Address() && m.From() == s1.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p3 entered Accept phase with proposed value v3, but rejected by s1")
		}
		return valid
	}
	p3AcceptRejBys1s2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		s1Rej := false
		s2Rej := false
		if s3.proposer.Phase == Accept && s3.proposer.V == "v3" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s3.Address() && m.From() == s2.Address() {
					s2Rej = true
				}
				if ok && !resp.Ok && m.To() == s3.Address() && m.From() == s1.Address() {
					s1Rej = true
				}
			}
			if s1Rej && s2Rej{
				valid = true
			}
		}
		if valid {
			fmt.Println("... p3 entered Accept phase with proposed value v3, but rejected by s1,s2")
		}
		return valid
	}
	p3AcceptRejBys1s2s3 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		s1Rej := false
		s2Rej := false
		s3Rej := false
		if s3.proposer.Phase == Accept && s3.proposer.V == "v3" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s3.Address() && m.From() == s3.Address() {
					s3Rej = true
				}
				if ok && !resp.Ok && m.To() == s3.Address() && m.From() == s2.Address() {
					s2Rej = true
				}
				if ok && !resp.Ok && m.To() == s3.Address() && m.From() == s1.Address() {
					s1Rej = true
				}
			}
			if s1Rej && s2Rej && s3Rej{
				valid = true
			}
		}
		if valid {
			fmt.Println("... p3 entered Accept phase with proposed value v3, but rejected by s1,s2, s3")
		}
		return valid
	}
	

	p3AcceptRejByTwo := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		// s2 := s.Nodes()["s2"].(*Server)
		// s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s3.proposer.Phase == Accept && s3.proposer.SuccessCount == 0{
			if s3.proposer.ResponseCount == 2{
				valid = true
				fmt.Println("... s3 rejected by Two")

			}

		}

		return valid
	}
	p3AcceptRejByAll := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		// s2 := s.Nodes()["s2"].(*Server)
		// s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s3.proposer.Phase == Accept && s3.proposer.SuccessCount == 0{
			if s3.proposer.ResponseCount == 3{
				valid = true
				fmt.Println("... s3 rejected by all")

			}

		}

		return valid
	}
	return []func(s *base.State) bool{
		p3PreparePhase,
		p3LargeNpInPropose ,
		p3AcceptPhase,
		p3AcceptRejBys1,
		p3AcceptRejBys1s2,
		p3AcceptRejBys1s2s3,
		s2s1LargerNP,
		s3LargerNP,
		p3AcceptRejByTwo ,
		p3AcceptRejByAll,

	}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected again.
func NotTerminate3() []func(s *base.State) bool {
	//panic("fill me in")
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Propose
		if valid {
			fmt.Println("... p1 entered Propose phase")
		}
		return valid
	}
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept
		if valid{
			fmt.Println("... p1 reached accept phase")
		}
		
		return valid
		
	}
	p3LargerNP := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept && s3.n_p > s1.proposer.N
		if valid{
			fmt.Println("... s3 has larger n_p than p1's N")
		}
		return valid
	}
	p3p2p1LargerNP := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept && s3.n_p > s1.proposer.N  && s2.n_p > s1.proposer.N && s1.n_p > s1.proposer.N
		if valid{
			fmt.Println("... s3 s2 s1 has larger n_p than p1's N")
		}
		return valid
	}
	p1AcceptRejBys2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		valid := false
		otheroks := 0
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s2.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s2")
		}
		return valid
	}
	p1AcceptRejBys3 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		otheroks := 0
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s3.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s3")
		}
		return valid
	}
	p1AcceptRejBys1 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := false
		otheroks := 0
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s1.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s1")
		}
		return valid
	}
	p1AcceptRejBys2s3 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		s2Rej := false
		s3Rej := false
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s2.Address() {
					s2Rej = true
				}
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s3.Address() {
					s3Rej = true
				}
			}
			if s2Rej && s3Rej{
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s2,s3")
		}
		return valid
	}
	p1AcceptRejBys1s2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		s1Rej := false
		s2Rej := false
		s3Rej := false
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s1.Address() {
					s1Rej = true
				}
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s2.Address() {
					s2Rej = true
				}
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s3.Address() {
					s3Rej = true
				}
			}
			if s1Rej && s2Rej && s3Rej{
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s1,s2")
		}
		return valid
	}

	p1AcceptRejByAll := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		// s2 := s.Nodes()["s2"].(*Server)
		// s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s1.proposer.Phase == Accept && s1.proposer.SuccessCount == 0{
			if s1.proposer.ResponseCount == 3{
				valid = true
				fmt.Println("... s1 rejected by everyone")

			}

		}

		return valid
	}
	return []func(s *base.State) bool{
		p1PreparePhase,
		p1AcceptPhase,
		p1AcceptRejBys2,
		p1AcceptRejBys3,
		p1AcceptRejBys1,
		p1AcceptRejBys2s3,
		p1AcceptRejBys1s2,
		p3LargerNP, 
		p3p2p1LargerNP,
		p1AcceptRejByAll ,

	}
}

// Fill in the function to lead the program to make P1 propose first, then P3 proposes, but P1 get rejects in
// Accept phase
func concurrentProposer1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Propose
		if valid {
			fmt.Println("... p1 entered Propose phase")
		}
		return valid
	}
	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Propose
		if valid {
			fmt.Println("... p3 entered Propose phase")
		}
		return valid
	}
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept
		if valid{
			fmt.Println("... p1 reached accept phase")
		}
		
		return valid
		
	}
	p3LargerNP := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept && s3.n_p > s1.proposer.N
		if valid{
			fmt.Println("... s3 has larger n_p than p1's N")
		}
		return valid
	}
	p3p2p1LargerNP := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept && s3.n_p > s1.proposer.N  && s2.n_p > s1.proposer.N && s1.n_p > s1.proposer.N
		if valid{
			fmt.Println("... s3 s2 s1 has larger n_p than p1's N")
		}
		return valid
	}
	p1AcceptRejBys2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		valid := false
		otheroks := 0
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s2.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s2")
		}
		return valid
	}
	p1AcceptRejBys3 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		otheroks := 0
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s3.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s3")
		}
		return valid
	}
	p1AcceptRejBys1 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := false
		otheroks := 0
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s1.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s1")
		}
		return valid
	}
	p1AcceptRejBys2s3 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		s2Rej := false
		s3Rej := false
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s2.Address() {
					s2Rej = true
				}
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s3.Address() {
					s3Rej = true
				}
			}
			if s2Rej && s3Rej{
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s2,s3")
		}
		return valid
	}
	p1AcceptRejBys1s2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		s1Rej := false
		s2Rej := false
		s3Rej := false
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s1.Address() {
					s1Rej = true
				}
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s2.Address() {
					s2Rej = true
				}
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s3.Address() {
					s3Rej = true
				}
			}
			if s1Rej && s2Rej && s3Rej{
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1, but rejected by s1,s2")
		}
		return valid
	}

	p1AcceptRejByAll := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		// s2 := s.Nodes()["s2"].(*Server)
		// s3 := s.Nodes()["s3"].(*Server)
		valid := false
		if s1.proposer.Phase == Accept && s1.proposer.SuccessCount == 0{
			if s1.proposer.ResponseCount == 3{
				valid = true
				fmt.Println("... s1 rejected by everyone")

			}

		}

		return valid
	}
	return []func(s *base.State) bool{
		p1PreparePhase,
		p3PreparePhase,
		p1AcceptPhase,
		p1AcceptRejBys2,
		p1AcceptRejBys3,
		p1AcceptRejBys1,
		p1AcceptRejBys2s3,
		p1AcceptRejBys1s2,
		p3LargerNP, 
		p3p2p1LargerNP,
		p1AcceptRejByAll ,

	}
}

// Fill in the function to lead the program continue  P3's proposal  and reaches consensus at the value of "v3".
func concurrentProposer2() []func(s *base.State) bool {
	p3LargerNP := func(s *base.State) bool {
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Propose && s3.proposer.N > s2.n_p
		if valid{
			fmt.Println("... p3 has larger N than s2's n_p")
		}
		return valid
	}
	s2Acceptp3Propose := func(s *base.State) bool {
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		otheroks := 0
		valid := false
		if s3.proposer.Phase == Propose && s3.proposer.V == "v3" {
			for _, m := range s.Network {
				resp, ok := m.(*ProposeResponse)
				if ok && resp.Ok && m.To() == s3.Address() && m.From() == s2.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid{
			fmt.Println("... p3 get propose ok from s2")
		}
		return valid
	}
	p3AcceptBys2 := func(s *base.State) bool {
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		otheroks := 0
		if s3.proposer.Phase == Accept && s3.proposer.V == "v3" {
			//fmt.Println("s3:v3")
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				
				if ok && resp.Ok && m.To() == s3.Address() && m.From() == s2.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p3 entered Accept phase with proposed value v3, also accepted by s2")
		}
		return valid
	}
	s3KnowConsensus := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.agreedValue == "v3" && s2.agreedValue == nil && s1.agreedValue == nil
		if valid {
			fmt.Println("s3 is the first to reach consensus")
		}
		return valid
	}
	return []func(s *base.State) bool{
		p3LargerNP,
		s2Acceptp3Propose,
		p3AcceptBys2,
		s3KnowConsensus,
	}
}
