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

func (mr *MapReduce) getJobs() {
	for i := 0; i < mr.nMap; i++ {
		mr.mapJobsToDo <- i
	}
	for i := 0; i < mr.nReduce; i++ {
		mr.reduceJobsToDo <- i
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
				//fmt.Println("map job id completed:", job)
				mr.mapJobsCompleted <- true
				mr.availableWorkers <- worker

			} else {
				mr.mapJobsToDo <- job
				//mr.availableWorkers <- worker
			}

		}(job, worker)

	}

}

func (mr *MapReduce) assignReduceJobs() {
	for job := range mr.reduceJobsToDo {
		//fmt.Println("doing reduce job", job)
		worker := <-mr.availableWorkers
		//fmt.Println("using worker:", worker)

		go func(job int, worker string) {
			//waitgroup.Add(1)
			args := &DoJobArgs{
				File:          mr.file,
				Operation:     Reduce,
				JobNumber:     job,
				NumOtherPhase: mr.nMap,
			}
			var reply DoJobReply
			ok := call(worker, "Worker.DoJob", args, &reply)
			if ok {
				mr.reduceJobsCompleted <- true
				mr.availableWorkers <- worker
			} else {
				fmt.Println("current reduce failed:", job)
				mr.reduceJobsToDo <- job
			}

		}(job, worker)
	}

}

func (mr *MapReduce) trackMapJob() {
	cnt := 0
	for range mr.mapJobsCompleted {
		cnt += 1
		if cnt == mr.nMap {
			break
		}
	}
	close(mr.mapJobsToDo)
	mr.mapDone <- true
}

func (mr *MapReduce) trackReduceJob() {
	cnt := 0
	for range mr.reduceJobsCompleted {
		cnt += 1
		if cnt == mr.nReduce {
			break
		}
	}
	close(mr.reduceJobsToDo)
	mr.reduceDone <- true
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	go mr.getAvailableWorkers()
	go mr.getJobs()

	go mr.trackMapJob()
	mr.assignMapJobs()
	<-mr.mapDone

	go mr.trackReduceJob()
	mr.assignReduceJobs()
	<-mr.reduceDone
	return mr.KillWorkers()

}
