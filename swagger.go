package gasync

import (
	"fmt"

	"github.com/gorchestrate/async"
)

func SwaggerDocs(workflows map[string]func() async.WorkflowState) (interface{}, error) {
	definitions := map[string]interface{}{}
	endpoints := map[string]interface{}{}
	docs := map[string]interface{}{
		"definitions": definitions,
		"swagger":     "2.0",
		"info": map[string]interface{}{
			"title":   "Pizza Service",
			"version": "0.0.1",
		},
		"host":     "pizzaapp-ffs2ro4uxq-uc.a.run.app",
		"basePath": "/",
		"schemes":  []string{"https"},
		"paths":    endpoints,
	}
	for wfName, wf := range workflows {
		endpoints["/"+wfName+"/new/{id}"] = map[string]interface{}{
			"post": map[string]interface{}{
				"consumes": []string{"application/json"},
				"produces": []string{"application/json"},
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
		_, err := async.Walk(wf().Definition(), func(s async.Stmt) bool {
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
					endpoints["/"+wfName+"/event/{id}/"+v.Callback.Name] = map[string]interface{}{
						"post": map[string]interface{}{
							"consumes": []string{"application/json"},
							"produces": []string{"application/json"},
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
	}
	return docs, nil
}
