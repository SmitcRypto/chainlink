package services

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/smartcontractkit/chainlink/adapters"
	"github.com/smartcontractkit/chainlink/logger"
	"github.com/smartcontractkit/chainlink/store"
	"github.com/smartcontractkit/chainlink/store/models"
	"github.com/smartcontractkit/chainlink/utils"
	"go.uber.org/multierr"
)

// JobRunner safely handles coordinating job runs.
type JobRunner interface {
	Start() error
	Stop()
	resumeSleepingRuns() error
	channelForRun(string) chan<- struct{}
	workerCount() int
}

type jobRunner struct {
	started              bool
	done                 chan struct{}
	bootMutex            sync.Mutex
	store                *store.Store
	workerMutex          sync.RWMutex
	workers              map[string]chan struct{}
	workersWg            sync.WaitGroup
	demultiplexStopperWg sync.WaitGroup
}

// NewJobRunner initializes a JobRunner.
func NewJobRunner(str *store.Store) JobRunner {
	return &jobRunner{
		store:   str,
		workers: make(map[string]chan struct{}),
	}
}

// Start reinitializes runs and starts the execution of the store's runs.
func (rm *jobRunner) Start() error {
	rm.bootMutex.Lock()
	defer rm.bootMutex.Unlock()

	if rm.started {
		return errors.New("JobRunner already started")
	}
	rm.done = make(chan struct{})
	rm.started = true

	var starterWg sync.WaitGroup
	starterWg.Add(1)
	go rm.demultiplexRuns(&starterWg)
	starterWg.Wait()

	rm.demultiplexStopperWg.Add(1)
	return rm.resumeSleepingRuns()
}

// Stop closes all open worker channels.
func (rm *jobRunner) Stop() {
	rm.bootMutex.Lock()
	defer rm.bootMutex.Unlock()

	if !rm.started {
		return
	}
	close(rm.done)
	rm.started = false
	rm.demultiplexStopperWg.Wait()
}

func (rm *jobRunner) resumeSleepingRuns() error {
	pendingRuns, err := rm.store.JobRunsWithStatus(models.RunStatusPendingSleep)
	if err != nil {
		return err
	}
	for _, run := range pendingRuns {
		rm.store.RunChannel.Send(run.ID)
	}
	return nil
}

func (rm *jobRunner) demultiplexRuns(starterWg *sync.WaitGroup) {
	starterWg.Done()
	defer rm.demultiplexStopperWg.Done()
	for {
		select {
		case <-rm.done:
			logger.Debug("JobRunner demultiplexing of job runs finished")
			rm.workersWg.Wait()
			return
		case rr, ok := <-rm.store.RunChannel.Receive():
			if !ok {
				logger.Panic("RunChannel closed before JobRunner, can no longer demultiplexing job runs")
				return
			}
			rm.channelForRun(rr.ID) <- struct{}{}
		}
	}
}

func (rm *jobRunner) channelForRun(runID string) chan<- struct{} {
	rm.workerMutex.Lock()
	defer rm.workerMutex.Unlock()

	workerChannel, present := rm.workers[runID]
	if !present {
		workerChannel = make(chan struct{}, 1000)
		rm.workers[runID] = workerChannel
		rm.workersWg.Add(1)

		go func() {
			rm.workerLoop(runID, workerChannel)

			rm.workerMutex.Lock()
			delete(rm.workers, runID)
			rm.workersWg.Done()
			rm.workerMutex.Unlock()

			logger.Debug("Worker finished for ", runID)
		}()
	}
	return workerChannel
}

func (rm *jobRunner) workerLoop(runID string, workerChannel chan struct{}) {
	for {
		select {
		case <-workerChannel:
			run, err := rm.store.FindJobRun(runID)
			if err != nil {
				logger.Errorw(fmt.Sprint("Application Run Channel Executor: error finding run ", runID), run.ForLogger("error", err)...)
			}
			if err = executeRun(&run, rm.store); err != nil {
				logger.Errorw(fmt.Sprint("Application Run Channel Executor: error executing run ", runID), run.ForLogger("error", err)...)
			}

			if run.Status.Finished() {
				return
			}

			if run.Status.Runnable() {
				logger.Infow("Adding next task to job run queue", []interface{}{"run", run.ID}...)
				rm.store.RunChannel.Send(run.ID)
			}

		case <-rm.done:
			logger.Debug("JobRunner worker loop for ", runID, " finished")
			return
		}
	}
}

func (rm *jobRunner) workerCount() int {
	rm.workerMutex.RLock()
	defer rm.workerMutex.RUnlock()

	return len(rm.workers)
}

// BuildRun checks to ensure the given job has not started or ended before
// creating a new run for the job.
func BuildRun(
	job models.JobSpec,
	i models.Initiator,
	store *store.Store,
	input models.RunResult,
	currentBlockHeight *models.IndexableBlockNumber,
) (models.JobRun, error) {
	now := store.Clock.Now()
	if !job.Started(now) {
		return models.JobRun{}, RecurringScheduleJobError{
			msg: fmt.Sprintf("Job runner: Job %v unstarted: %v before job's start time %v", job.ID, now, job.EndAt),
		}
	}
	if job.Ended(now) {
		return models.JobRun{}, RecurringScheduleJobError{
			msg: fmt.Sprintf("Job runner: Job %v ended: %v past job's end time %v", job.ID, now, job.EndAt),
		}
	}

	run := job.NewRun(i)
	run.Overrides = input

	if currentBlockHeight != nil {
		run.CreationHeight = &currentBlockHeight.Number
	}

	if input.Amount != nil {
		paymentValid, err := ValidateMinimumContractPayment(store, job, *input.Amount)

		if err != nil {
			err = fmt.Errorf(
				"Rejecting job %s error validating contract payment: %v",
				job.ID,
				err,
			)
		} else if !paymentValid {
			err = fmt.Errorf(
				"Rejecting job %s with payment %s below minimum threshold (%s)",
				job.ID,
				input.Amount,
				store.Config.MinimumContractPayment.Text(10))
		}

		if err != nil {
			run.ApplyResult(input.WithError(err))
			return run, multierr.Append(err, store.Save(&run))
		}
	}

	return run, nil
}

// EnqueueRun creates a run and wakes up the Job Runner to process it
func EnqueueRun(
	job models.JobSpec,
	initr models.Initiator,
	input models.RunResult,
	store *store.Store,
	currentBlockHeight *models.IndexableBlockNumber,
) (models.JobRun, error) {
	run, err := BuildRun(job, initr, store, input, currentBlockHeight)
	if err != nil {
		return run, err
	}

	logger.Debugw("New run created", []interface{}{
		"run", run.ID,
		"input", input.Data,
	}...)

	nextTaskRun := run.NextTaskRun()
	nextTaskRun.Result, err = run.Overrides.Merge(input)
	if err != nil {
		return run, err
	}
	nextTaskRun.Result.JobRunID = run.ID

	if err = prepareJobRun(&run, store, currentBlockHeight); err != nil {
		return run, err
	}

	if err = store.Save(&run); err != nil {
		return run, err
	}

	if run.Status == models.RunStatusInProgress {
		logger.Debugw("New run is ready to start, adding to queue", []interface{}{"run", run.ID}...)
		return run, store.RunChannel.Send(run.ID)
	}
	return run, nil
}

// WakeConfirmingTask checks if a task's minimum confirmations have been met
// and if so, tell the job runner to get executing
func WakeConfirmingTask(
	run *models.JobRun,
	store *store.Store,
	currentBlockHeight *models.IndexableBlockNumber,
) error {
	logger.Debugw("New head waking up job", []interface{}{
		"run", run.ID,
		"blockHeight", currentBlockHeight.Number,
	}...)

	if err := prepareJobRun(run, store, currentBlockHeight); err != nil {
		return err
	}

	if err := store.Save(run); err != nil {
		return err
	}

	if run.Status == models.RunStatusInProgress {
		return store.RunChannel.Send(run.ID)
	}
	return nil
}

// WakeBridgePendingTask takes the body provided from an external adapter,
// saves it for the next task to process, then tells the job runner to execute
// it
func WakeBridgePendingTask(
	run *models.JobRun,
	store *store.Store,
	input models.RunResult,
) error {

	logger.Debugw("External adapter waking up job", []interface{}{
		"run", run.ID,
		"input", input.Data,
		"result", input.Status,
		"status", run.Status,
	}...)

	if !run.Status.Pending() {
		log.Panic("WakeBridgePendingTask for non pending task!")
	}

	// If the input coming from the Bridge contains an error, mark this run as errored
	if input.Status.Errored() {
		logger.Debugw("External adapter reported error, marking job as failed", []interface{}{
			"run", run.ID,
			"result", input.Status,
		}...)
		run.ApplyResult(input)
		return store.Save(run)
	}

	var err error
	run.Overrides, err = run.Overrides.Merge(input)
	if err != nil {
		run.ApplyResult(run.Result.WithError(err))
		return store.Save(run)
	}

	//if nextTaskRun.Task.Params, err = run.Overrides.Data.Merge(input.Data); err != nil {
	//nextTaskRun.ApplyResult(nextTaskRun.Result.WithError(err))
	//return store.Save(run)
	//}

	nextTaskRun := run.NextTaskRun()
	//nextTaskRun.Result, err = input.Merge(run.Overrides)
	nextTaskRun.Result, err = run.Overrides.Merge(input)
	if err != nil {
		return err
	}
	//nextTaskRun.Result.JobRunID = run.ID

	if err := prepareJobRun(run, store, nil); err != nil {
		return err
	}

	if err := store.Save(run); err != nil {
		return err
	}

	if run.Status == models.RunStatusInProgress {
		return store.RunChannel.Send(run.ID)
	}
	return nil
}

func executeRun(run *models.JobRun, store *store.Store) error {
	logger.Infow("Job beginning execution", run.ForLogger()...)

	statuses := []models.RunStatus{}
	for _, task := range run.TaskRuns {
		statuses = append(statuses, task.Status)
	}
	logger.Debugw("Tasks", []interface{}{"status", statuses}...)

	if !run.TasksRemain() {
		// TODO: I suspect this probably will because of job subscribers
		log.Panicf("Job triggered when no tasks left to run")
	}

	nextTaskRun := run.NextTaskRun()

	var err error

	if nextTaskRun.Task.Params, err = run.Overrides.Data.Merge(nextTaskRun.Task.Params); err != nil {
		nextTaskRun.ApplyResult(nextTaskRun.Result.WithError(err))
		run.ApplyResult(nextTaskRun.Result)
		return store.Save(run)
	}

	logger.Debugw("Task using params", []interface{}{"task", nextTaskRun.ID, "params", nextTaskRun.Task.Params}...)

	adapter, err := adapters.For(nextTaskRun.Task, store)
	if err != nil {
		nextTaskRun.ApplyResult(nextTaskRun.Result.WithError(err))
		run.ApplyResult(nextTaskRun.Result)
		return store.Save(run)
	}

	logger.Debugw("Starting task", []interface{}{"task", nextTaskRun.ID, "adapter", nextTaskRun.Task.Type}...)

	//fmt.Println("nextTaskRun", nextTaskRun, nextTaskRun.Result.JobRunID)
	result := adapter.Perform(nextTaskRun.Result, store)
	logger.Debugw("Task execution complete", []interface{}{
		"task", nextTaskRun.ID,
		"result", result.Error(),
		"response", result.Data,
		"status", result.Status,
	}...)

	nextTaskRun.ApplyResult(result)
	if nextTaskRun.Status.Runnable() {
		logger.Debugw("Marking task as completed", []interface{}{"task", nextTaskRun.ID}...)
		nextTaskRun.MarkCompleted()
	} else {
		logger.Debugw("Task is not runnable, setting job state", []interface{}{"run", run.ID, "task", nextTaskRun.ID, "state", nextTaskRun.Result.Status}...)
		run.ApplyResult(nextTaskRun.Result)
		return store.Save(run)
	}

	// If there are more tasks, save the result of this task as their input params
	if run.TasksRemain() {
		futureRun := run.NextTaskRun()

		futureRun.Result.Data = result.Data

		adapter, err := adapters.For(futureRun.Task, store)
		if err != nil {
			futureRun.ApplyResult(futureRun.Result.WithError(err))
			return store.Save(run)
		}

		// If the minimum confirmations for this task are greater than 0, put this
		// job run in pending status confirmations, only the head tracker / run log
		// should wake it up again
		minConfs := utils.MaxUint64(
			store.Config.MinIncomingConfirmations,
			futureRun.Task.Confirmations,
			adapter.MinConfs())
		if minConfs > 0 {
			run.Status = models.RunStatusPendingConfirmations
		}

		logger.Debugw("Future task run", []interface{}{
			"task", futureRun.ID,
			"params", futureRun.Result.Data,
			"minimum_confirmations", minConfs,
		}...)
	} else if nextTaskRun.Status.Completed() {
		logger.Debugw("All tasks completed, marking job as completed", []interface{}{"run", run.ID, "task", nextTaskRun.ID}...)
		run.ApplyResult(nextTaskRun.Result)
	}

	if err = store.Save(run); err != nil {
		return wrapExecuteRunError(run, err)
	}
	logger.Infow("Finished current job run execution", run.ForLogger()...)

	statuses = []models.RunStatus{}
	for _, task := range run.TaskRuns {
		statuses = append(statuses, task.Status)
	}
	logger.Debugw("Tasks", []interface{}{"status", statuses}...)

	return nil
}

// prepareJobRun is responsible for putting a job into a running state provided
// it meets starting conditions: not errored, sufficient confirmations, sleep
// period elapsed etc.
func prepareJobRun(
	run *models.JobRun,
	store *store.Store,
	currentBlockHeight *models.IndexableBlockNumber) error {

	if !run.Status.CanStart() {
		return fmt.Errorf("Unable to start with status %v", run.Status)
	}

	//if run.Status.Pending() && currentBlockHeight != nil {
	if currentBlockHeight != nil {
		tr := run.NextTaskRun()

		// TODO: This is only needed to get the adapter.MinConfs, but is otherwise
		// discarded and then used again in an other spot, could we make this more
		// efficient?
		adapter, err := adapters.For(tr.Task, store)
		if err != nil {
			tr.ApplyResult(tr.Result.WithError(err))
			return nil
		}

		minConfs := utils.MaxUint64(
			store.Config.MinIncomingConfirmations,
			tr.Task.Confirmations,
			adapter.MinConfs())

		if !run.Runnable(currentBlockHeight.Number.ToInt().Uint64(), minConfs) {
			logger.Debugw("Insufficient confirmations to wake job", []interface{}{
				"run", run.ID,
				"observed_height", currentBlockHeight.Number.ToInt(),
				"expected_height", minConfs,
			}...)
			run.Status = models.RunStatusPendingConfirmations
			return nil
		}

		logger.Debugw("Minimum confirmations met", []interface{}{
			"run", run.ID,
			"observed_height", currentBlockHeight.Number.ToInt(),
			"expected_height", minConfs,
		}...)
	}

	if run.Result.HasError() {
		logger.Debugw("Error preparing job run", []interface{}{
			"run", run.ID,
			"result", run.Result.Error(),
		}...)
		return run.Result
	}

	run.Status = models.RunStatusInProgress
	return nil
}

func logTaskResult(lr models.TaskRun, tr models.TaskRun, i int) {
	logger.Debugw("Produced task run", "taskRun", lr)
	logger.Debugw(fmt.Sprintf("Next task run %v %v", tr.Task.Type, tr.Result.Status), tr.ForLogger("taskidx", i, "result", lr.Result)...)
}

func markCompletedIfRunnable(tr models.TaskRun) models.TaskRun {
	return tr
}

func wrapExecuteRunError(run *models.JobRun, err error) error {
	return fmt.Errorf("executeRun: Job#%v: %v", run.JobID, err)
}

// RecurringScheduleJobError contains the field for the error message.
type RecurringScheduleJobError struct {
	msg string
}

// Error returns the error message for the run.
func (err RecurringScheduleJobError) Error() string {
	return err.msg
}
