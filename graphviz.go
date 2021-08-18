package gasync

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/awalterschulze/gographviz"

	"github.com/gorchestrate/async"
)

type Grapher struct {
	g *gographviz.Graph
}

func (g *Grapher) Dot(s async.Stmt) string {
	g.g = gographviz.NewGraph()
	g.g.Directed = true
	ctx := GraphCtx{}
	start := ctx.node(g, "start", "start", "circle")
	end := ctx.node(g, "", "end", "circle")
	ctx.Prev = []string{start}
	octx := g.Walk(s, ctx)
	g.AddEdges(octx.Prev, end)
	return g.g.String()
}

func (g *Grapher) AddEdges(from []string, to string) {
	for _, v := range from {
		_ = g.g.AddEdge(v, to, true, nil)
	}
}

func (g *Grapher) AddEdge(from string, to string) {
	if from == "" || to == "" {
		return
	}
	_ = g.g.AddEdge(from, to, true, nil)
}

type GraphCtx struct {
	Parent string
	Prev   []string
	Break  []string
}

var ncount int

func (ctx *GraphCtx) node(g *Grapher, id, name string, shape string) string {
	if id == "" {
		ncount++
		id = fmt.Sprint(ncount)
	} else {
		id = strconv.Quote(id)
	}
	_ = g.g.AddNode("", id, map[string]string{
		"label": strconv.Quote(name),
		"shape": shape,
	})
	return id
}

func (g *Grapher) Walk(s async.Stmt, ctx GraphCtx) GraphCtx {
	switch x := s.(type) {
	case nil:
		return GraphCtx{}
	case async.ReturnStmt:
		n := ctx.node(g, "", "end", "circle")
		g.AddEdges(ctx.Prev, n)
		return GraphCtx{}
	case async.BreakStmt:
		return GraphCtx{Break: ctx.Prev}
	case async.ContinueStmt:
		return GraphCtx{}
	case async.StmtStep:
		id := ctx.node(g, x.Name, "⚙️ "+x.Name+"  ", "box")
		g.AddEdges(ctx.Prev, id)
		return GraphCtx{Prev: []string{id}}
	case async.WaitCondStmt:
		id := ctx.node(g, x.Name, "⏸ wait for "+x.Name, "hexagon")
		g.AddEdges(ctx.Prev, id)
		return GraphCtx{Prev: []string{id}}
	case async.WaitEventsStmt:
		id := ctx.node(g, x.Name, "⏸ wait "+x.Name, "hexagon")
		g.AddEdges(ctx.Prev, id)
		prev := []string{}
		breaks := []string{}
		for _, v := range x.Cases {
			var cid string
			_, ok := v.Handler.(*ReflectEvent)
			_, ok2 := v.Handler.(*TimeoutHandler)
			if ok {
				cid = ctx.node(g, v.Callback.Name, "▶️ /"+v.Callback.Name+"  ", "component")
			} else if ok2 {
				cid = ctx.node(g, v.Callback.Name, "🕑"+v.Callback.Name+"  ", "component")
			} else {
				cid = ctx.node(g, v.Callback.Name, "⚡"+v.Callback.Name+"  ", "component")
			}
			_ = g.g.AddEdge(id, cid, true, nil)
			octx := g.Walk(v.Stmt, GraphCtx{
				Prev: []string{cid},
			})
			prev = append(prev, octx.Prev...)
			breaks = append(breaks, octx.Break...)
		}
		return GraphCtx{Prev: prev}
	case *async.GoStmt:
		id := ctx.node(g, x.Name, x.Name, "ellipse")

		for _, v := range ctx.Prev {
			_ = g.g.AddEdge(v, id, true, map[string]string{
				"style": "dashed",
				"label": "parallel",
			})
		}
		_ = g.Walk(x.Stmt, GraphCtx{Prev: []string{id}})
		return GraphCtx{Prev: ctx.Prev}
	case async.ForStmt:
		id := ctx.node(g, x.Name, "↺ while "+x.Name, "hexagon")
		g.AddEdges(ctx.Prev, id)
		breaks := []string{}
		curCtx := GraphCtx{Prev: []string{id}}
		for _, v := range x.Section {
			curCtx = g.Walk(v, GraphCtx{
				Prev:   curCtx.Prev,
				Parent: "sub",
			})
			breaks = append(breaks, curCtx.Break...)
		}
		g.AddEdges(curCtx.Prev, id)
		return GraphCtx{Prev: append(breaks, id)}
	case *async.SwitchStmt:
		for _, v := range x.Cases {
			g.Walk(v.Stmt, ctx)
		}
		return GraphCtx{}
	case async.Section:
		curCtx := ctx
		breaks := []string{}
		for _, v := range x {
			curCtx = g.Walk(v, GraphCtx{
				Prev:   curCtx.Prev,
				Parent: ctx.Parent,
			})
			breaks = append(breaks, curCtx.Break...)
		}
		return GraphCtx{Prev: curCtx.Prev, Break: breaks}
	default:
		panic(reflect.TypeOf(s))
	}
}
