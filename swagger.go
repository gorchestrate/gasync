package gasync

import (
	"fmt"
	"net/url"

	"github.com/gorchestrate/async"
)

func SwaggerDoc(host string, wfName string, wf func() async.WorkflowState) (interface{}, error) {
	url, err := url.Parse(host)
	if err != nil {
		return nil, err
	}
	definitions := map[string]interface{}{}
	endpoints := map[string]interface{}{}
	docs := map[string]interface{}{
		"definitions": definitions,
		"swagger":     "2.0",
		"info": map[string]interface{}{
			"title":   wfName,
			"version": "0.0.1",
		},
		"host":     url.Host,
		"basePath": "/",
		"schemes":  []string{url.Scheme},
		"paths":    endpoints,
	}
	endpoints["/wf/"+wfName+"/{id}"] = map[string]interface{}{
		"post": map[string]interface{}{
			"consumes":    []string{"application/json"},
			"produces":    []string{"application/json"},
			"tags":        []string{wfName},
			"description": `<img src="https://pizzaapp-ffs2ro4uxq-uc.a.run.app/graph/pizza" style="width:400px;" />`,
			"parameters": []map[string]interface{}{
				{
					"name":        "id",
					"in":          "path",
					"description": "workflow id",
					"required":    true,
					"type":        "string",
				},
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "success",
				},
			},
		},
	}
	var oErr error
	_, err = async.Walk(wf().Definition(), func(s async.Stmt) bool {
		switch x := s.(type) {
		case async.WaitEventsStmt:
			for _, v := range x.Cases {
				h, ok := v.Handler.(*ReflectEvent)
				if !ok {
					continue
				}
				in, out, err := h.Schemas()
				if err != nil {
					oErr = err
					panic(err)
				}
				for name, def := range in.Definitions {
					definitions[name] = def
				}
				for name, def := range out.Definitions {
					definitions[name] = def
				}
				endpoints["/wf/"+wfName+"/{id}/"+v.Callback.Name] = map[string]interface{}{
					"post": map[string]interface{}{
						"consumes": []string{"application/json"},
						"produces": []string{"application/json"},
						"tags":     []string{wfName},
						"parameters": []map[string]interface{}{
							{
								"name":        "id",
								"in":          "path",
								"description": "workflow id",
								"required":    true,
								"type":        "string",
							},
							{
								"name":        "body",
								"in":          "body",
								"description": "event data",
								"required":    true,
								"schema": map[string]interface{}{
									"$ref": in.Ref,
								},
							},
						},
						"responses": map[string]interface{}{
							"200": map[string]interface{}{
								"description": "success",
								"schema": map[string]interface{}{
									"$ref": out.Ref,
								},
							},
						},
					},
				}
			}
		}
		return false
	})
	if err != nil {
		return nil, fmt.Errorf("err swaggering workflow: %v", wfName)
	}
	if oErr != nil {
		return nil, fmt.Errorf("err during swaggering workflow: %v", wfName)
	}
	return docs, nil
}
