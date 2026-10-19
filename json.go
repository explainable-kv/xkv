// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0

package xkv

import (
	"encoding/base64"
	"encoding/json"
	"time"
	"unicode/utf8"
)

type revisionJSON struct {
	Rev      uint64     `json:"rev"`
	Tx       uint64     `json:"tx"`
	Key      string     `json:"key"`
	Value    *string    `json:"value,omitempty"`
	Encoding string     `json:"encoding,omitempty"`
	Deleted  bool       `json:"deleted,omitempty"`
	Actor    string     `json:"actor"`
	Reason   string     `json:"reason"`
	Ref      string     `json:"ref,omitempty"`
	Causes   []CauseRef `json:"causes"`
	Time     time.Time  `json:"time"`
}

// MarshalJSON writes the value as a string when it is valid UTF-8, and as
// base64 with "encoding": "base64" when it isn't.
func (r Revision) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.toJSON())
}

func (r Revision) toJSON() revisionJSON {
	j := revisionJSON{
		Rev: r.Rev, Tx: r.Tx, Key: r.Key, Deleted: r.Deleted,
		Actor: r.Actor, Reason: r.Reason, Ref: r.Ref, Causes: r.Causes, Time: r.Time,
	}
	if j.Causes == nil {
		j.Causes = []CauseRef{}
	}
	if !r.Deleted {
		var v string
		if utf8.Valid(r.Value) {
			v = string(r.Value)
		} else {
			v = base64.StdEncoding.EncodeToString(r.Value)
			j.Encoding = "base64"
		}
		j.Value = &v
	}
	return j
}

// UnmarshalJSON reads what MarshalJSON writes.
func (r *Revision) UnmarshalJSON(b []byte) error {
	var j revisionJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	*r = Revision{
		Rev: j.Rev, Tx: j.Tx, Key: j.Key, Deleted: j.Deleted,
		Actor: j.Actor, Reason: j.Reason, Ref: j.Ref, Causes: j.Causes, Time: j.Time,
	}
	if len(r.Causes) == 0 {
		r.Causes = nil
	}
	if j.Value != nil {
		if j.Encoding == "base64" {
			v, err := base64.StdEncoding.DecodeString(*j.Value)
			if err != nil {
				return err
			}
			r.Value = v
		} else {
			r.Value = []byte(*j.Value)
		}
	}
	return nil
}

type whyJSON struct {
	Revision      revisionJSON  `json:"revision"`
	Prev          *revisionJSON `json:"prev,omitempty"`
	PrevCompacted bool          `json:"prev_compacted,omitempty"`
	Causes        []*whyJSON    `json:"causes"`
	Truncated     bool          `json:"truncated,omitempty"`
	Repeat        bool          `json:"repeat,omitempty"`
}

// MarshalJSON writes the node as {"revision", "prev", "causes", ...}, with
// causes as nested nodes. The tree is converted to plain structs first and
// encoded in one pass, so deep trees cost linear time.
func (n WhyNode) MarshalJSON() ([]byte, error) {
	return json.Marshal(n.toJSON())
}

func (n *WhyNode) toJSON() *whyJSON {
	j := &whyJSON{Revision: n.Revision.toJSON(), PrevCompacted: n.PrevCompacted, Causes: make([]*whyJSON, 0, len(n.Causes)), Truncated: n.Truncated, Repeat: n.Repeat}
	if n.Prev != nil {
		p := n.Prev.toJSON()
		j.Prev = &p
	}
	for _, c := range n.Causes {
		j.Causes = append(j.Causes, c.toJSON())
	}
	return j
}

// UnmarshalJSON reads what MarshalJSON writes.
func (n *WhyNode) UnmarshalJSON(b []byte) error {
	var j whyJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	return n.fromJSON(&j)
}

func (r *Revision) fromJSON(j revisionJSON) error {
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return r.UnmarshalJSON(b)
}

func (n *WhyNode) fromJSON(j *whyJSON) error {
	*n = WhyNode{PrevCompacted: j.PrevCompacted, Truncated: j.Truncated, Repeat: j.Repeat}
	if err := n.Revision.fromJSON(j.Revision); err != nil {
		return err
	}
	if j.Prev != nil {
		var p Revision
		if err := p.fromJSON(*j.Prev); err != nil {
			return err
		}
		n.Prev = &p
	}
	for _, c := range j.Causes {
		var cn WhyNode
		if err := cn.fromJSON(c); err != nil {
			return err
		}
		n.Causes = append(n.Causes, &cn)
	}
	return nil
}
