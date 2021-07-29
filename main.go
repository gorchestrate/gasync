package gasync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/goccy/go-graphviz"
	"github.com/rs/cors"

	"cloud.google.com/go/firestore"
	"github.com/alecthomas/jsonschema"
	"github.com/gorchestrate/async"
	"github.com/gorilla/mux"
	cloudtasks "google.golang.org/api/cloudtasks/v2beta3"
)

type Config struct {
	GCloudProjectID      string
	GCloudLocationID     string
	GCloudTasksQueueName string
	BasePublicURL        string
	CORS                 bool
	Collection           string
}

type Server struct {
	Engine *FirestoreEngine
	Router *mux.Router
}

func NewServer(cfg Config, workflows map[string]func() async.WorkflowState) (*Server, error) {
	jsonschema.Version = ""
	rand.Seed(time.Now().Unix())
	ctx := context.Background()
	db, err := firestore.NewClient(ctx, cfg.GCloudProjectID)
	if err != nil {
		panic(err)
	}
	cTasks, err := cloudtasks.NewService(ctx)
	if err != nil {
		panic(err)
	}

	mr := mux.NewRouter()
	if cfg.CORS {
		c := cors.New(cors.Options{
			AllowedOrigins: []string{"*"},
			AllowedMethods: []string{"GET", "POST"},
		})
		mr.Use(c.Handler)
	}

	engine := &FirestoreEngine{
		DB:         db,
		Collection: cfg.Collection,
		Workflows:  workflows,
	}

	s := &GTasksScheduler{
		Engine:     engine,
		C:          cTasks,
		ProjectID:  cfg.GCloudProjectID,
		LocationID: cfg.GCloudLocationID,
		QueueName:  cfg.GCloudTasksQueueName,
		ResumeURL:  cfg.BasePublicURL + "/resume",
	}
	mr.HandleFunc("/resume", s.ResumeHandler)

	engine.Scheduler = s
	gTaskMgr := &GTasksScheduler{
		Engine:      engine,
		C:           cTasks,
		ProjectID:   cfg.GCloudProjectID,
		LocationID:  cfg.GCloudLocationID,
		QueueName:   cfg.GCloudTasksQueueName,
		CallbackURL: cfg.BasePublicURL + "/callback/timeout",
	}
	mr.HandleFunc("/callback/timeout", gTaskMgr.TimeoutHandler)

	mr.HandleFunc("/new/{id}", func(w http.ResponseWriter, r *http.Request) {
		wfName := mux.Vars(r)["id"]
		wf, ok := workflows[wfName]
		if !ok {
			if err != nil {
				fmt.Fprintf(w, " workflow  %v not found", wfName)
				return
			}
		}
		err := engine.ScheduleAndCreate(r.Context(), mux.Vars(r)["id"], "pizzaOrder", wf()) // TODO: how to create workflow with params!?
		if err != nil {
			w.WriteHeader(400)
			fmt.Fprintf(w, err.Error())
			return
		}
		// after callback is handled - we wait for resume process
		err = engine.Resume(r.Context(), mux.Vars(r)["id"])
		if err != nil {
			log.Printf("resume err: %v", err)
			w.WriteHeader(500)
			return
		}
	})
	mr.HandleFunc("/status/{id}", func(w http.ResponseWriter, r *http.Request) {
		wf, err := engine.Get(r.Context(), mux.Vars(r)["id"])
		if err != nil {
			w.WriteHeader(400)
			fmt.Fprintf(w, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(wf)
	})
	mr.HandleFunc("/graph/{id}", func(w http.ResponseWriter, r *http.Request) {
		wfName := mux.Vars(r)["id"]
		wf, ok := workflows[wfName]
		if !ok {
			if err != nil {
				fmt.Fprintf(w, " workflow  %v not found", wfName)
				return
			}
		}
		g := Grapher{}
		def := g.Dot(wf().Definition())
		gv := graphviz.New()
		gd, err := graphviz.ParseBytes([]byte(def))
		if err != nil {
			fmt.Fprintf(w, " %v \n %v", def, err)
			return
		}
		w.Header().Add("Content-Type", "image/jpg")
		gv.Render(gd, graphviz.JPG, w)
	})

	mr.HandleFunc("/swagger", func(w http.ResponseWriter, r *http.Request) {
		docs, err := SwaggerDocs(workflows)
		if err != nil {
			fmt.Fprintf(w, "%v ", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		e := json.NewEncoder(w)
		e.SetIndent("", " ")
		_ = e.Encode(docs)
	})
	ret := &Server{
		Router: mr,
		Engine: engine,
	}
	mr.HandleFunc("/event/{id}/{event}", ret.SimpleEventHandler)
	return ret, nil
}
