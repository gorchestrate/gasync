package gasync

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorchestrate/async"
	cloudtasks "google.golang.org/api/cloudtasks/v2beta3"
)

type GTasksScheduler struct {
	Engine      *FirestoreEngine
	C           *cloudtasks.Service
	Collection  string
	ProjectID   string
	LocationID  string
	QueueName   string
	ResumeURL   string
	CallbackURL string
}

type ResumeRequest struct {
	ID string
}

func (mgr *GTasksScheduler) ResumeHandler(w http.ResponseWriter, r *http.Request) {
	var req ResumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		log.Printf("err: %v", err)
		return
	}

	err = mgr.Engine.Resume(r.Context(), req.ID)
	if err != nil {
		log.Printf("err: %v", err)
		w.WriteHeader(500)
		return
	}
}

// in this demo we resume workflows right inside the http handler.
// we use this scheduler only for redundancy in case resume will fail for some reason in http handler.
func (mgr *GTasksScheduler) Schedule(ctx context.Context, id string) error {
	body, err := json.Marshal(ResumeRequest{
		ID: id,
	})
	if err != nil {
		panic(err)
	}
	sTime := time.Now().Add(time.Millisecond * 100).Format(time.RFC3339)
	_, err = mgr.C.Projects.Locations.Queues.Tasks.Create(
		fmt.Sprintf("projects/%v/locations/%v/queues/%v",
			mgr.ProjectID, mgr.LocationID, mgr.QueueName),
		&cloudtasks.CreateTaskRequest{
			Task: &cloudtasks.Task{
				ScheduleTime: sTime,
				HttpRequest: &cloudtasks.HttpRequest{
					Url:        mgr.ResumeURL,
					HttpMethod: "POST",
					Body:       base64.StdEncoding.EncodeToString(body),
				},
			},
		}).Context(ctx).Do()
	return err
}

func (t *GTasksScheduler) Timeout(dur time.Duration) *TimeoutHandler {
	return &TimeoutHandler{
		Duration:  dur,
		scheduler: t,
	}
}

type TimeoutHandler struct {
	Duration  time.Duration
	scheduler *GTasksScheduler
}

func (s TimeoutHandler) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type     string
		Duration string
	}{
		Type:     "timeout",
		Duration: fmt.Sprintf("%v sec", s.Duration.Seconds()),
	})
}

func (t *TimeoutHandler) Handle(ctx context.Context, req async.CallbackRequest, input interface{}) (interface{}, error) {
	return nil, nil
}

func (t *TimeoutHandler) Setup(ctx context.Context, req async.CallbackRequest) (string, error) {
	return t.scheduler.Setup(ctx, req, t.Duration)
}

func (t *TimeoutHandler) Teardown(ctx context.Context, req async.CallbackRequest, handled bool) error {
	return t.scheduler.Teardown(ctx, req, handled)
}

func (mgr *GTasksScheduler) TimeoutHandler(w http.ResponseWriter, r *http.Request) {
	var req async.CallbackRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		log.Printf("err: %v", err)
		return
	}
	_, err = mgr.Engine.HandleCallback(r.Context(), req.WorkflowID, req, nil)
	if err != nil {
		log.Printf("err: %v", err)
		w.WriteHeader(500)
		return
	}
}

type GTasksSchedulerData struct {
	ID string
}

func (mgr *GTasksScheduler) Setup(ctx context.Context, req async.CallbackRequest, del time.Duration) (string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		panic(err)
	}
	sTime := time.Now().Add(del).Format(time.RFC3339)
	resp, err := mgr.C.Projects.Locations.Queues.Tasks.Create(
		fmt.Sprintf("projects/%v/locations/%v/queues/%v",
			mgr.ProjectID, mgr.LocationID, mgr.QueueName),
		&cloudtasks.CreateTaskRequest{
			Task: &cloudtasks.Task{
				ScheduleTime: sTime,
				HttpRequest: &cloudtasks.HttpRequest{
					Url:        mgr.CallbackURL,
					HttpMethod: "POST",
					Body:       base64.StdEncoding.EncodeToString(body),
				},
			},
		}).Do()
	if err != nil {
		return "", err
	}
	d, err := json.Marshal(GTasksSchedulerData{
		ID: resp.Name,
	})
	return string(d), err
}

func (mgr *GTasksScheduler) Teardown(ctx context.Context, req async.CallbackRequest, handled bool) error {
	if handled {
		log.Printf("skipping teardown for task that was already handled")
		return nil
	}
	var data GTasksSchedulerData
	err := json.Unmarshal([]byte(req.SetupData), &data)
	if err != nil {
		return err
	}
	_, err = mgr.C.Projects.Locations.Queues.Tasks.Delete(data.ID).Do()
	if err != nil {
		log.Printf("delete task err: %v", err)
	}
	return nil
}
