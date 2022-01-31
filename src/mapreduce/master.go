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
		if ok == false {
			//fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) getAvailableWorkers() {
	//fmt.Println("get available workers")
	for worker := range mr.registerChannel {
		//fmt.Println("get worker:", worker)
		mr.Workers[worker] = &WorkerInfo{address: worker}
		mr.availableWorkers <- worker
	}
}

func (mr *MapReduce) getJobs(jobType string, jobAmount int) {
	if jobType == "map" {
		for i := 0; i < jobAmount; i++ {
			//fmt.Println("get map job id:", i)

			mr.mapJobsToDo <- i
		}
	} else if jobType == "reduce" {
		for i := 0; i < jobAmount; i++ {
			//fmt.Println("get reduce job id:", i)

			mr.reduceJobsToDo <- i
		}

	}

}

func (mr *MapReduce) assignMapJobs() {
	for job := range mr.mapJobsToDo {
		//fmt.Println("doing map job", job)
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
				//fmt.Printf("DoMap: RPC %s do job failure! reassign the  job id %v  \n", worker, job)
				mr.mapJobsToDo <- job
			}

		}(job, worker)

	}

}

func (mr *MapReduce) trackMapJob() {
	for {
		if mr.mapCompleted == mr.nMap {
			mr.mapDone = true
			//fmt.Println("map process complete, start doing reduce")
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
						//fmt.Println("reduce job id completed:", job)
						mr.reduceCompleted += 1
						mr.availableWorkers <- worker

						//fmt.Println("cur reduceCompleted", mr.reduceCompleted)
					} else {
						//fmt.Printf("DoReduce: RPC %s do job failure! reassign the job...\n", worker)
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
			//fmt.Println("map process complete, start doing reduce")
			break
		}
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	go mr.getAvailableWorkers()
	//fmt.Println("map total jobs", mr.nMap)
	go mr.getJobs("map", mr.nMap)

	go mr.trackMapJob()
	go mr.assignMapJobs()

	//fmt.Println("reduce total jobs", mr.nReduce)
	go mr.getJobs("reduce", mr.nReduce)
	go mr.trackReduceJob()
	go mr.assignReduceJobs()
	for {
		if mr.allComplete {
			return mr.KillWorkers()
		}

	}

}
