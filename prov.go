// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"
)

// ProvContext is the JSON-LD context of PROV output: PROV-O terms under
// prov:, xkv's own terms under xkv:.
var ProvContext = map[string]string{
	"prov": "http://www.w3.org/ns/prov#",
	"rdfs": "http://www.w3.org/2000/01/rdf-schema#",
	"xkv":  "https://xkv.org/ns#",
	"xsd":  "http://www.w3.org/2001/XMLSchema#",
}

// RevisionIRI names a revision of a store: urn:xkv:STOREID:rev:REV. The same
// revision gets the same name in every tree exported from that store, so
// graphs from one store merge.
func RevisionIRI(store StoreID, rev uint64) string {
	return fmt.Sprintf("urn:xkv:%s:rev:%d", store, rev)
}

// TransactionIRI names a transaction of a store: urn:xkv:STOREID:tx:TX.
func TransactionIRI(store StoreID, tx uint64) string {
	return fmt.Sprintf("urn:xkv:%s:tx:%d", store, tx)
}

// AgentIRI names an actor: urn:xkv:actor: and the actor, percent-encoded.
// It doesn't depend on the store, so one actor is one agent everywhere.
func AgentIRI(actor string) string {
	return "urn:xkv:actor:" + url.PathEscape(actor)
}

// PROV returns the tree as W3C PROV-O in JSON-LD, for store (the store the
// tree came from; see DB.StoreID). Each revision is a prov:Entity generated
// by its transaction, a prov:Activity associated with the actor, a
// prov:Agent. A revision's causes are used by its transaction and are
// entities it was derived from; the key's previous revision is the entity it
// is a revision of. Reason, ref and the rest are xkv: terms. Nodes appear
// once each, in the order the tree first reaches them, so the output for a
// tree is always the same bytes.
func (n *WhyNode) PROV(store StoreID) ([]byte, error) {
	if n == nil {
		return nil, fmt.Errorf("xkv: no tree: %w", ErrInvalid)
	}
	g := provGraph{store: store, entities: map[uint64]*provNode{}, activities: map[uint64]*provNode{}, agents: map[string]*provNode{}, known: map[uint64]*Revision{}, inTree: map[uint64]bool{}}
	g.collect(n)
	g.walk(n)
	nodes := make([]any, 0, len(g.order)+len(g.agentOrder))
	for _, x := range g.order {
		nodes = append(nodes, x)
	}
	sort.Strings(g.agentOrder)
	for _, a := range g.agentOrder {
		nodes = append(nodes, g.agents[a])
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	err := enc.Encode(struct {
		Context map[string]string `json:"@context"`
		Graph   []any             `json:"@graph"`
	}{ProvContext, nodes})
	return buf.Bytes(), err
}

// provNode is one JSON-LD node object with its properties in a fixed order.
type provNode struct {
	keys   []string
	values map[string]any
}

func newProvNode(id, typ string) *provNode {
	p := &provNode{values: map[string]any{}}
	p.set("@id", id)
	p.set("@type", typ)
	return p
}

func (p *provNode) set(k string, v any) {
	if _, ok := p.values[k]; !ok {
		p.keys = append(p.keys, k)
	}
	p.values[k] = v
}

// add appends an {"@id": id} reference to a list property, once.
func (p *provNode) add(k, id string) {
	ref := map[string]string{"@id": id}
	list, _ := p.values[k].([]any)
	for _, x := range list {
		if x.(map[string]string)["@id"] == id {
			return
		}
	}
	p.set(k, append(list, ref))
}

func (p *provNode) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, k := range p.keys {
		if i > 0 {
			b.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		b.Write(kb)
		b.WriteByte(':')
		var vb bytes.Buffer
		enc := json.NewEncoder(&vb)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(p.values[k]); err != nil {
			return nil, err
		}
		b.Write(bytes.TrimRight(vb.Bytes(), "\n"))
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

type provGraph struct {
	store      StoreID
	entities   map[uint64]*provNode
	activities map[uint64]*provNode
	agents     map[string]*provNode
	order      []*provNode
	agentOrder []string
	known      map[uint64]*Revision // every revision the tree holds in full
	inTree     map[uint64]bool      // revisions that are nodes of the tree, not only a Prev
}

func (g *provGraph) collect(n *WhyNode) {
	g.known[n.Rev] = &n.Revision
	g.inTree[n.Rev] = true
	if n.Prev != nil {
		if _, ok := g.known[n.Prev.Rev]; !ok {
			g.known[n.Prev.Rev] = n.Prev
		}
	}
	for _, c := range n.Causes {
		g.collect(c)
	}
}

func typed(v, typ string) map[string]string { return map[string]string{"@value": v, "@type": typ} }

func (g *provGraph) agent(actor string) string {
	id := AgentIRI(actor)
	if _, ok := g.agents[actor]; !ok {
		a := newProvNode(id, "prov:Agent")
		a.set("rdfs:label", actor)
		g.agents[actor] = a
		g.agentOrder = append(g.agentOrder, actor)
	}
	return id
}

// entity adds a revision and its transaction, once, and returns both nodes.
func (g *provGraph) entity(r *Revision) (*provNode, *provNode) {
	e, ok := g.entities[r.Rev]
	if !ok {
		e = newProvNode(RevisionIRI(g.store, r.Rev), "prov:Entity")
		e.set("rdfs:label", fmt.Sprintf("%s@%d", r.Key, r.Rev))
		e.set("xkv:key", r.Key)
		e.set("xkv:rev", typed(strconv.FormatUint(r.Rev, 10), "xsd:integer"))
		switch {
		case r.Deleted:
			e.set("xkv:deleted", true)
		case utf8.Valid(r.Value):
			e.set("prov:value", string(r.Value))
		default:
			e.set("prov:value", base64.StdEncoding.EncodeToString(r.Value))
			e.set("xkv:valueEncoding", "base64")
		}
		if r.Reason != "" {
			e.set("xkv:reason", r.Reason)
		}
		if r.Ref != "" {
			e.set("xkv:ref", r.Ref)
		}
		if r.Origin != "" {
			e.set("xkv:origin", r.Origin)
		}
		if r.History {
			e.set("xkv:history", true)
		}
		when := typed(r.Time.UTC().Format(time.RFC3339Nano), "xsd:dateTime")
		e.set("prov:generatedAtTime", when)
		e.set("prov:wasGeneratedBy", map[string]string{"@id": TransactionIRI(g.store, r.Tx)})
		e.set("prov:wasAttributedTo", map[string]string{"@id": g.agent(r.Actor)})
		g.entities[r.Rev] = e
		g.order = append(g.order, e)
	}
	a, ok := g.activities[r.Tx]
	if !ok {
		a = newProvNode(TransactionIRI(g.store, r.Tx), "prov:Activity")
		a.set("xkv:tx", typed(strconv.FormatUint(r.Tx, 10), "xsd:integer"))
		a.set("prov:endedAtTime", typed(r.Time.UTC().Format(time.RFC3339Nano), "xsd:dateTime"))
		g.activities[r.Tx] = a
		g.order = append(g.order, a)
	}
	// Writes in one transaction can name different actors: each is
	// associated with it.
	a.add("prov:wasAssociatedWith", g.agent(r.Actor))
	return e, a
}

func (g *provGraph) walk(n *WhyNode) {
	e, a := g.entity(&n.Revision)
	if n.Prev != nil {
		p, _ := g.entity(n.Prev)
		if !g.inTree[n.Prev.Rev] {
			// A previous value is shown without its own causes.
			p.set("xkv:causesNotShown", true)
		}
		e.set("prov:wasRevisionOf", map[string]string{"@id": RevisionIRI(g.store, n.Prev.Rev)})
	}
	if n.PrevCompacted {
		e.set("xkv:prevCompacted", true)
	}
	if n.Truncated {
		e.set("xkv:truncated", true)
	}
	// Causes are named even where the tree stops (truncated or repeat): the
	// links are known, only the causes' own causes are not shown there.
	for _, c := range n.Causes {
		g.walk(c)
	}
	for _, c := range n.Revision.Causes {
		id := RevisionIRI(g.store, c.Rev)
		if full, ok := g.known[c.Rev]; ok {
			g.entity(full)
		} else if _, ok := g.entities[c.Rev]; !ok {
			// A cause the depth limit didn't reach: a bare entity, so the
			// reference resolves.
			x := newProvNode(id, "prov:Entity")
			x.set("xkv:key", c.Key)
			x.set("xkv:rev", typed(strconv.FormatUint(c.Rev, 10), "xsd:integer"))
			x.set("xkv:unexplored", true)
			g.entities[c.Rev] = x
			g.order = append(g.order, x)
		}
		// A cause written in the same transaction was generated by this
		// activity, not used by it; the derivation still holds.
		if full, ok := g.known[c.Rev]; !ok || full.Tx != n.Tx {
			a.add("prov:used", id)
		}
		e.add("prov:wasDerivedFrom", id)
	}
}
