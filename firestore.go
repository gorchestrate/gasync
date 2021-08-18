package gasync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/gorchestrate/async"
)

type FirestoreEngine struct {
	Scheduler  *GTasksScheduler
	DB         *firestore.Client
	Collection string
	Workflows  map[string]func() async.WorkflowState
}

type DBWorkflow struct {
	Meta     async.State
	State    interface{} // json body of workflow state
	LockTill time.Time   // optimistic locking
}

func logTime(section string) func() {
	start := time.Now()
	return func() {
		log.Printf("%v took %v ms", section, time.Since(start))
	}
}

func (fs FirestoreEngine) Lock(ctx context.Context, id string) (DBWorkflow, error) {
	defer logTime("lock")()
	for i := 0; ; i++ {
		doc, err := fs.DB.Collection(fs.Collection).Doc(id).Get(ctx)
		if err != nil {
			return DBWorkflow{}, err
		}
		var wf DBWorkflow
		err = doc.DataTo(&wf)
		if err != nil {
			return DBWorkflow{}, fmt.Errorf("err unmarshaling workflow: %v", err)
		}
		if time.Since(wf.LockTill) < 0 {
			if i > 50 {
				return DBWorkflow{}, fmt.Errorf("workflow is locked. can't unlock with 50 retries")
			} else {
				log.Printf("workflow is locked, waiting and trying again...")
				time.Sleep(time.Millisecond * 100 * time.Duration(i))
				continue
			}
		}
		_, err = fs.DB.Collection(fs.Collection).Doc(id).Update(ctx,
			[]firestore.Update{
				{
					Path:  "LockTill",
					Value: time.Now().Add(time.Minute),
				},
			},
			firestore.LastUpdateTime(doc.UpdateTime),
		)
		if err != nil && strings.Contains(err.Error(), "FailedPrecondition") {
			log.Printf("workflow was locked concurrently, waiting and trying again...")
			continue
		}
		if err != nil {
			return DBWorkflow{}, fmt.Errorf("err locking workflow: %v", err)
		}
		return wf, nil
	}
}

func (fs FirestoreEngine) Unlock(ctx context.Context, id string) error {
	defer logTime("unlock")()
	// always unlock, even if previous err != nil
	_, unlockErr := fs.DB.Collection(fs.Collection).Doc(id).Update(ctx,
		[]firestore.Update{
			{
				Path:  "LockTill",
				Value: time.Time{},
			},
		},
	)
	if unlockErr != nil {
		return fmt.Errorf("err unlocking workflow: %v", unlockErr)
	}
	return nil
}

type DBWorkflowLog struct {
	Meta         async.State
	State        interface{} // json body of workflow state
	Time         time.Time
	ExecDuration time.Duration
	Input        interface{}
	Output       interface{}
	Callback     *async.CallbackRequest
}

func pjson(in interface{}) interface{} {
	d, ok := in.([]byte)
	if ok {
		var i interface{}
		_ = json.Unmarshal(d, &i)
		return i
	}
	d, ok = in.(json.RawMessage)
	if ok {
		var i interface{}
		_ = json.Unmarshal(d, &i)
		return i
	}
	return in
}

func (fs FirestoreEngine) Save(ctx context.Context, wf *DBWorkflow, s *async.WorkflowState, unlock bool) error {
	defer logTime("save")()
	updates := []firestore.Update{
		{
			Path:  "Meta",
			Value: wf.Meta,
		},
		{
			Path:  "State",
			Value: *s,
		},
	}
	if unlock {
		updates = append(updates, firestore.Update{
			Path:  "LockTill",
			Value: time.Time{},
		})
	}
	b := fs.DB.Batch()
	b.Update(fs.DB.Collection(fs.Collection).Doc(wf.Meta.ID), updates)
	_, err := b.Commit(ctx)
	return err
}

// func (fs FirestoreEngine) Checkpoint(ctx context.Context, wf *DBWorkflow, s *async.WorkflowState, cb *async.CallbackRequest, input, output interface{}) func(bool) error {
// 	defer logTime("checkpoint func")()
// 	start := time.Now()
// 	return func(resume bool) error {
// 		b := fs.DB.Batch()
// 		b.Update(fs.DB.Collection(fs.Collection).Doc(wf.Meta.ID), []firestore.Update{
// 			{
// 				Path:  "Meta",
// 				Value: wf.Meta,
// 			},
// 			{
// 				Path:  "State",
// 				Value: *s,
// 			},
// 		})
// 		in, ok := input.([]byte)
// 		if ok {
// 			input = in
// 		}

// 		b.Set(fs.DB.Collection(fs.Collection+"_log").Doc(fmt.Sprintf("%v_%v", wf.Meta.ID, wf.Meta.PC)), DBWorkflowLog{
// 			Meta:         wf.Meta,
// 			State:        wf.State,
// 			Time:         time.Now(),
// 			ExecDuration: time.Since(start),
// 			Input:        pjson(input),
// 			Output:       pjson(output),
// 			Callback:     cb,
// 		})
// 		_, err := b.Commit(ctx)
// 		return err
// 	}
// }

func (fs FirestoreEngine) HandleCallback(ctx context.Context, id string, cb async.CallbackRequest, input interface{}) (interface{}, error) {
	wf, err := fs.Lock(ctx, id)
	if err != nil {
		return nil, err
	}
	w, ok := fs.Workflows[wf.Meta.Workflow]
	if !ok {
		_ = fs.Unlock(ctx, id)
		return nil, fmt.Errorf("workflow not found: %v", wf.Meta.Workflow)
	}
	state := w()
	d, err := json.Marshal(wf.State)
	if err != nil {
		_ = fs.Unlock(ctx, id)
		return nil, err
	}
	err = json.Unmarshal(d, &state)
	if err != nil {
		_ = fs.Unlock(ctx, id)
		return nil, err
	}
	out, err := async.HandleCallback(ctx, cb, state, &wf.Meta, input)
	if err != nil {
		_ = fs.Unlock(ctx, id)
		return out, fmt.Errorf("err during workflow processing: %w", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := fs.Scheduler.Schedule(ctx, wf.Meta.ID, 0)
		if err != nil {
			log.Printf("err scheduling")
		}
	}()
	err = fs.Save(ctx, &wf, &state, true)
	if err != nil {
		return out, fmt.Errorf("err during workflow saving: %w", err)
	}
	wg.Wait()
	return out, nil
}

func (fs FirestoreEngine) HandleEvent(ctx context.Context, id string, name string, input interface{}) (interface{}, error) {
	defer logTime("handle event")()
	wf, err := fs.Lock(ctx, id)
	if err != nil {
		return nil, err
	}
	w, ok := fs.Workflows[wf.Meta.Workflow]
	if !ok {
		_ = fs.Unlock(ctx, id)
		return nil, fmt.Errorf("workflow not found: %v", wf.Meta.Workflow)
	}
	state := w()
	d, err := json.Marshal(wf.State)
	if err != nil {
		_ = fs.Unlock(ctx, id)
		return nil, err
	}
	err = json.Unmarshal(d, &state)
	if err != nil {
		_ = fs.Unlock(ctx, id)
		return nil, err
	}
	out, err := async.HandleEvent(ctx, name, state, &wf.Meta, input)
	if err != nil {
		_ = fs.Unlock(ctx, id)
		return out, fmt.Errorf("err during workflow processing: %w", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := fs.Scheduler.Schedule(ctx, wf.Meta.ID, 0)
		if err != nil {
			log.Printf("err scheduling")
		}
	}()
	err = fs.Save(ctx, &wf, &state, true)
	if err != nil {
		return out, fmt.Errorf("err during workflow saving: %w", err)
	}
	wg.Wait()
	// _, err = async.Resume(context.Background(), state, &wf.Meta)
	// if err != nil {
	// 	return out, fmt.Errorf("err during workflow resuming: %w", err)
	// }
	// err = fs.Save(context.Background(), &wf, &state, true)
	// if err != nil {
	// 	return out, fmt.Errorf("err during workflow saving after resume: %w", err)
	// }
	return out, nil
}

func (fs FirestoreEngine) Resume(ctx context.Context, id string) error {
	defer logTime("resume func")()
	wf, err := fs.Lock(ctx, id)
	if err != nil {
		return err
	}
	w, ok := fs.Workflows[wf.Meta.Workflow]
	if !ok {
		_ = fs.Unlock(ctx, id)
		return fmt.Errorf("workflow not found: %v", err)
	}
	state := w()
	d, err := json.Marshal(wf.State)
	if err != nil {
		_ = fs.Unlock(ctx, id)
		return err
	}
	err = json.Unmarshal(d, &state)
	if err != nil {
		_ = fs.Unlock(ctx, id)
		return err
	}
	s := logTime("resume")
	_, err = async.Resume(ctx, state, &wf.Meta)
	if err != nil {
		_ = fs.Unlock(ctx, id)
		return fmt.Errorf("err during workflow processing: %w", err)
	}
	s()
	s = logTime("checkpoint")
	err = fs.Save(ctx, &wf, &state, true)
	if err != nil {
		return err
	}
	s()
	return nil
}

func (fs FirestoreEngine) Get(ctx context.Context, id string) (*DBWorkflow, error) {
	defer logTime("get")()
	d, err := fs.DB.Collection(fs.Collection).Doc(id).Get(ctx)
	if err != nil {
		return nil, err
	}
	var wf DBWorkflow
	err = d.DataTo(&wf)
	return &wf, err
}

func (fs FirestoreEngine) ScheduleAndCreate(ctx context.Context, id, name string, state interface{}) error {
	defer logTime("schedule and create")()
	wf := DBWorkflow{
		Meta:  async.NewState(id, name),
		State: state,
	}
	w, ok := fs.Workflows[wf.Meta.Workflow]
	if !ok {
		_ = fs.Unlock(ctx, id)
		return fmt.Errorf("workflow not found: %v", wf.Meta.Workflow)
	}
	s := w()
	_, err := async.Resume(ctx, s, &wf.Meta)
	if err != nil {
		_ = fs.Unlock(ctx, id)
		return fmt.Errorf("err during workflow processing: %w", err)
	}
	_, err = fs.DB.Collection(fs.Collection).Doc(id).Create(ctx, wf)
	if err != nil {
		return err
	}
	return nil
}
