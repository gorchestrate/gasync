package gasync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"

	"github.com/alecthomas/jsonschema"
	"github.com/gorchestrate/async"
	"github.com/gorilla/mux"
	"github.com/xeipuuv/gojsonschema"
)

type Empty struct {
}

func Event(role, name string, handler interface{}, ss ...async.Stmt) async.Event {
	return async.On(name, &ReflectEvent{
		Role:    role,
		Handler: handler,
	}, ss...)
}

// This is an example of how to create your custom events
type ReflectEvent struct {
	Role    string
	Handler interface{}
}

func (h ReflectEvent) InputSchema() ([]byte, error) {
	fv := reflect.ValueOf(h.Handler)
	ft := fv.Type()
	if ft.NumIn() != 1 {
		return nil, fmt.Errorf("async http handler should have 1 input") // TODO: ctx support?
	}
	if ft.In(0).Kind() != reflect.Struct {
		return nil, fmt.Errorf(("input param is not a struct"))
	}
	return json.Marshal(jsonschema.ReflectFromType(ft.In(0)))
}

func (h ReflectEvent) Schemas() (in *jsonschema.Schema, out *jsonschema.Schema, err error) {
	fv := reflect.ValueOf(h.Handler)
	ft := fv.Type()
	if ft.NumOut() != 2 {
		return nil, nil, fmt.Errorf("async http handler should have 2 outputs")
	}
	if ft.Out(0).Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf(("input param is not a struct"))
	}
	if ft.NumIn() != 1 {
		return nil, nil, fmt.Errorf("async http handler should have 1 input") // TODO: ctx support?
	}
	if ft.In(0).Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf(("input param is not a struct"))
	}
	r := jsonschema.Reflector{
		FullyQualifyTypeNames: true,
	}
	return r.ReflectFromType(ft.In(0)), r.ReflectFromType(ft.Out(0)), nil
}

func (h ReflectEvent) MarshalJSON() ([]byte, error) {
	fv := reflect.ValueOf(h.Handler)
	ft := fv.Type()
	if ft.NumIn() != 1 {
		return nil, fmt.Errorf("async http handler should have 1 input") // TODO: ctx support?
	}
	if ft.NumOut() != 2 {
		return nil, fmt.Errorf("async http handler should have 2 outputs")
	}
	if ft.In(0).Kind() != reflect.Struct {
		return nil, fmt.Errorf(("input param is not a struct"))
	}
	if ft.Out(0).Kind() != reflect.Struct {
		return nil, fmt.Errorf(("first output param is not a struct"))
	}
	r := jsonschema.Reflector{
		FullyQualifyTypeNames: true,
	}
	in := r.ReflectFromType(ft.In(0))
	out := r.ReflectFromType(ft.Out(0))
	return json.Marshal(struct {
		Type   string
		Input  *jsonschema.Schema
		Output *jsonschema.Schema
		Role   string
	}{
		Type:   "handler",
		Input:  in,
		Output: out,
		Role:   h.Role,
	})
}

// code that will be executed when event is received
func (h *ReflectEvent) Handle(ctx context.Context, req async.CallbackRequest, input interface{}) (interface{}, error) {
	in, err := h.InputSchema()
	if err != nil {
		return nil, fmt.Errorf("input schema: %v", err)
	}
	vRes, err := gojsonschema.Validate(gojsonschema.NewBytesLoader(in), gojsonschema.NewBytesLoader(input.([]byte)))
	if err != nil {
		return nil, fmt.Errorf("jsonschema validate failure: %v using %v", err, string(in))
	}
	if !vRes.Valid() {
		for _, e := range vRes.Errors() {
			return nil, ValidateErr(e.Field(), e.String())
		}
		return nil, fmt.Errorf("jsonschema validate: %v", vRes.Errors())
	}
	fv := reflect.ValueOf(h.Handler)
	ft := fv.Type()
	if ft.NumIn() != 1 {
		return nil, fmt.Errorf("async http handler should have 1 input") // TODO: ctx support?
	}
	if ft.NumOut() != 2 {
		return nil, fmt.Errorf("async http handler should have 2 outputs")
	}
	if ft.In(0).Kind() != reflect.Struct {
		return nil, fmt.Errorf(("input param is not a struct"))
	}
	if ft.Out(0).Kind() != reflect.Struct {
		return nil, fmt.Errorf(("first output param is not a struct"))
	}
	dstInput := reflect.New(ft.In(0))
	err = json.Unmarshal(input.([]byte), dstInput.Interface())
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal input: %v", err)
	}
	res := fv.Call([]reflect.Value{dstInput.Elem()})
	if res[1].Interface() != nil {
		outErr, ok := res[1].Interface().(error)
		if !ok {
			return nil, fmt.Errorf("second output param is not an error")
		}
		if outErr != nil {
			return nil, fmt.Errorf("err in handler: %w", outErr)
		}
	}
	d, err := json.Marshal(res[0].Interface())
	if err != nil {
		return nil, fmt.Errorf("err marshaling output: %v", err)
	}
	return json.RawMessage(d), nil
}

// when we will start listening for this event - Setup() will be called for us to setup this event on external services
func (t *ReflectEvent) Setup(ctx context.Context, req async.CallbackRequest) (string, error) {
	// we will receive event via http call, no setup is needed
	return "", nil
}

// when we will stop listening for this event - Teardown() will be called for us to remove this event on external services
func (t *ReflectEvent) Teardown(ctx context.Context, req async.CallbackRequest, handled bool) error {
	// we will receive event via http call, no teardown is needed
	return nil
}

// Receive event and forward it to workflow engine
func (s Server) SimpleEventHandler(w http.ResponseWriter, r *http.Request) {
	d, err := ioutil.ReadAll(r.Body)
	if err != nil {
		jsonErr(w, err, 500)
		return
	}
	out, err := s.Engine.HandleEvent(r.Context(), mux.Vars(r)["id"], mux.Vars(r)["event"], d)
	if err != nil {
		jsonErr(w, err, 400)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

type ErrValidate struct {
	Path    string
	Message string
}

func (e ErrValidate) Error() string {
	return fmt.Sprintf("validation failed: %v %v", e.Path, e.Message)
}

func ValidateErr(path, format string, params ...interface{}) ErrValidate {
	return ErrValidate{
		Path:    path,
		Message: fmt.Sprintf(format, params...),
	}
}

func jsonErr(w http.ResponseWriter, err error, code int) {
	w.WriteHeader(code)
	var valErr ErrValidate
	e := struct {
		Msg  string
		Type string
		Path string
	}{
		Msg:  err.Error(),
		Type: "general",
	}

	if errors.As(err, &valErr) {
		e.Msg = valErr.Message
		e.Type = "validate"
		e.Path = valErr.Path
	}

	_ = json.NewEncoder(w).Encode(e)
	log.Printf("%v", e)
}
