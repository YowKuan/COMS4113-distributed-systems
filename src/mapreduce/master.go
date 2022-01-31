package mapreduce

import (
	"container/list"
	"fmt"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if !ok {
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) getAvailableWorkers() {
	//fmt.Println("get available workers")
	for worker := range mr.registerChannel {
		mr.Workers[worker] = &WorkerInfo{address: worker}
		mr.availableWorkers <- worker
	}
}

func (mr *MapReduce) getJobs(jobType string, jobAmount int) {
	if jobType == "map" {
		for i := 0; i < jobAmount; i++ {

			mr.mapJobsToDo <- i
		}
	} else if jobType == "reduce" {
		for i := 0; i < jobAmount; i++ {

			mr.reduceJobsToDo <- i
		}

	}

}

func (mr *MapReduce) assignMapJobs() {
	for job := range mr.mapJobsToDo {
		worker := <-mr.availableWorkers
		go func(job int, worker string) {
			args := &DoJobArgs{
				File:          mr.file,
				Operation:     Map,
				JobNumber:     job,
				NumOtherPhase: mr.nReduce,
			}
			var reply DoJobReply
			ok := call(worker, "Worker.DoJob", args, &reply)
			if ok {
				fmt.Println("map job id completed:", job)
				mr.mapCompleted += 1
				fmt.Println("cur mapCompleted", mr.mapCompleted)
				mr.availableWorkers <- worker

			} else {
				mr.mapJobsToDo <- job
			}

		}(job, worker)

	}

}

func (mr *MapReduce) trackMapJob() {
	for {
		if mr.mapCompleted == mr.nMap {
			mr.mapDone = true
			break
		}
	}
}

func (mr *MapReduce) assignReduceJobs() {
	for {
		if mr.reduceDone {
			break
		}
		if mr.mapDone {
			for job := range mr.reduceJobsToDo {
				//fmt.Println("doing reduce job", job)
				worker := <-mr.availableWorkers
				go func(job int, worker string) {
					args := &DoJobArgs{
						File:          mr.file,
						Operation:     Reduce,
						JobNumber:     job,
						NumOtherPhase: mr.nMap,
					}
					var reply DoJobReply
					ok := call(worker, "Worker.DoJob", args, &reply)
					if ok {
						mr.reduceCompleted += 1
						mr.availableWorkers <- worker
					} else {
						mr.reduceJobsToDo <- job
					}

				}(job, worker)
			}

		}

	}

}

func (mr *MapReduce) trackReduceJob() {
	for {
		if mr.reduceCompleted == mr.nReduce {
			mr.reduceDone = true
			mr.allComplete = true
			break
		}
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	go mr.getAvailableWorkers()
	go mr.getJobs("map", mr.nMap)

	go mr.trackMapJob()
	go mr.assignMapJobs()

	go mr.getJobs("reduce", mr.nReduce)
	go mr.trackReduceJob()
	go mr.assignReduceJobs()
	for {
		if mr.allComplete {
			return mr.KillWorkers()
		}

	}

}
