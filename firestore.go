package gasync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
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

func (fs FirestoreEngine) Lock(ctx context.Context, id string) (DBWorkflow, error) {
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

func (fs FirestoreEngine) Checkpoint(ctx context.Context, wf *DBWorkflow, s *async.WorkflowState, cb *async.CallbackRequest, input, output interface{}) func(bool) error {
	start := time.Now()
	return func(resume bool) error {
		if resume {
			err := fs.Scheduler.Schedule(ctx, wf.Meta.ID)
			if err != nil {
				return err
			}
		}
		b := fs.DB.Batch()
		b.Update(fs.DB.Collection(fs.Collection).Doc(wf.Meta.ID), []firestore.Update{
			{
				Path:  "Meta",
				Value: wf.Meta,
			},
			{
				Path:  "State",
				Value: *s,
			},
		})
		in, ok := input.([]byte)
		if ok {
			input = in
		}

		b.Set(fs.DB.Collection(fs.Collection+"_log").Doc(fmt.Sprintf("%v_%v", wf.Meta.ID, wf.Meta.PC)), DBWorkflowLog{
			Meta:         wf.Meta,
			State:        wf.State,
			Time:         time.Now(),
			ExecDuration: time.Since(start),
			Input:        pjson(input),
			Output:       pjson(output),
			Callback:     cb,
		})
		_, err := b.Commit(ctx)
		return err
	}
}

func (fs FirestoreEngine) HandleCallback(ctx context.Context, id string, cb async.CallbackRequest, input interface{}) (interface{}, error) {
	wf, err := fs.Lock(ctx, id)
	if err != nil {
		return nil, err
	}
	defer fs.Unlock(ctx, id)
	w, ok := fs.Workflows[wf.Meta.Workflow]
	if !ok {
		return nil, fmt.Errorf("workflow not found: %v", err)
	}
	state := w()
	d, err := json.Marshal(wf.State)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(d, &state)
	if err != nil {
		return nil, err
	}
	out, err := async.HandleCallback(ctx, cb, state, &wf.Meta, input)
	if err != nil {
		return out, fmt.Errorf("err during workflow processing: %v", err)
	}
	err = fs.Checkpoint(ctx, &wf, &state, &cb, input, out)(true)
	if err != nil {
		return out, fmt.Errorf("err during workflow saving: %v", err)
	}
	return out, nil
}

func (fs FirestoreEngine) HandleEvent(ctx context.Context, id string, name string, input interface{}) (interface{}, error) {
	wf, err := fs.Lock(ctx, id)
	if err != nil {
		return nil, err
	}
	defer fs.Unlock(ctx, id)
	w, ok := fs.Workflows[wf.Meta.Workflow]
	if !ok {
		return nil, fmt.Errorf("workflow not found: %v", wf.Meta.Workflow)
	}
	state := w()
	d, err := json.Marshal(wf.State)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(d, &state)
	if err != nil {
		return nil, err
	}
	out, err := async.HandleEvent(ctx, name, state, &wf.Meta, input)
	if err != nil {
		return out, fmt.Errorf("err during workflow processing: %v", err)
	}
	err = fs.Checkpoint(ctx, &wf, &state, &async.CallbackRequest{Name: name}, input, out)(true)
	if err != nil {
		return out, fmt.Errorf("err during workflow saving: %v", err)
	}
	return out, nil
}

func (fs FirestoreEngine) Resume(ctx context.Context, id string) error {
	wf, err := fs.Lock(ctx, id)
	if err != nil {
		return err
	}
	defer fs.Unlock(ctx, id)
	w, ok := fs.Workflows[wf.Meta.Workflow]
	if !ok {
		return fmt.Errorf("workflow not found: %v", err)
	}
	state := w()
	d, err := json.Marshal(wf.State)
	if err != nil {
		return err
	}
	err = json.Unmarshal(d, &state)
	if err != nil {
		return err
	}
	err = async.Resume(ctx, state, &wf.Meta, fs.Checkpoint(ctx, &wf, &state, nil, nil, nil))
	if err != nil {
		return fmt.Errorf("err during workflow processing: %v", err)
	}
	return nil
}

func (fs FirestoreEngine) Get(ctx context.Context, id string) (*DBWorkflow, error) {
	d, err := fs.DB.Collection(fs.Collection).Doc(id).Get(ctx)
	if err != nil {
		return nil, err
	}
	var wf DBWorkflow
	err = d.DataTo(&wf)
	return &wf, err
}

func (fs FirestoreEngine) ScheduleAndCreate(ctx context.Context, id, name string, state interface{}) error {
	err := fs.Scheduler.Schedule(ctx, id)
	if err != nil {
		return err
	}
	wf := DBWorkflow{
		Meta:  async.NewState(id, name),
		State: state,
	}
	_, err = fs.DB.Collection(fs.Collection).Doc(id).Create(ctx, wf)
	return err
}
