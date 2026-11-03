# Why trees as W3C PROV

Since 0.4.0 the tree of causes behind a value can be had as
[W3C PROV-O](https://www.w3.org/TR/prov-o/) in JSON-LD, the provenance
vocabulary that lineage, audit and knowledge-graph tools read:

```sh
xkv why settings.xkv config/db/timeout --prov
curl 'localhost:7070/v1/why/config/db/timeout?format=prov'
curl 'localhost:7070/v1/revs/12/why?format=prov&depth=4'
```

In Go, `(*WhyNode).PROV(storeID)` gives the same bytes; `DB.StoreID` gives
the id. The answer's content type is `application/ld+json`. A store still
in file format 1 has no store id and is refused (409 over HTTP); upgrade it
first.

## The mapping

| xkv | PROV |
|---|---|
| a revision | a `prov:Entity` named `urn:xkv:STOREID:rev:REV` |
| its transaction | a `prov:Activity` named `urn:xkv:STOREID:tx:TX`; the entity `prov:wasGeneratedBy` it |
| the actor | a `prov:Agent` named `urn:xkv:actor:` plus the actor, percent-encoded; the entity `prov:wasAttributedTo` it and the activity `prov:wasAssociatedWith` it (an activity has one agent for each actor that wrote in it) |
| a cause | the entity `prov:wasDerivedFrom` it, and the activity `prov:used` it unless the same transaction wrote it |
| the key's previous revision | the entity `prov:wasRevisionOf` it |
| the value | `prov:value`, base64 with `xkv:valueEncoding "base64"` when it isn't UTF-8; a delete has `xkv:deleted true` instead |
| the write time | `prov:generatedAtTime` on the entity and `prov:endedAtTime` on the activity, as `xsd:dateTime` |
| key, revision, transaction number | `xkv:key`, `xkv:rev`, `xkv:tx` (`xsd:integer`) |
| reason, ref | `xkv:reason`, `xkv:ref` |
| an imported revision's origin, a history-only revision | `xkv:origin` (`STOREID:REV` where it was first written), `xkv:history true` |
| `truncated`, `prev_compacted` in the JSON tree | `xkv:truncated true`, `xkv:prevCompacted true` on the entity |

`rdfs:label` gives each entity as `KEY@REV` and each agent its actor name.
The `xkv:` prefix is `https://xkv.org/ns#`. A cause the depth limit didn't
reach still appears, as an entity with only its key and revision and
`xkv:unexplored true`, so every reference in the graph names a node in it. A previous value is
shown without its own causes, so unless it is also in the tree in full it
carries `xkv:causesNotShown true`; ask for its own tree to see them.
A revision reached by two paths appears once. Revisions from one
transaction share one activity.

Names are fixed by the store id and the revision number, so trees exported
from one store at different times, or for different keys, merge into one
graph without duplicates. Agents are named by actor alone, so one actor is
one agent across stores.

## Example

```json
{
  "@context": {"prov": "http://www.w3.org/ns/prov#", "rdfs": "http://www.w3.org/2000/01/rdf-schema#", "xkv": "https://xkv.org/ns#", "xsd": "http://www.w3.org/2001/XMLSchema#"},
  "@graph": [
    {"@id": "urn:xkv:4f1c...:rev:3", "@type": "prov:Entity", "rdfs:label": "config/db/timeout@3",
     "xkv:key": "config/db/timeout", "xkv:rev": {"@value": "3", "@type": "xsd:integer"},
     "prov:value": "45s", "xkv:reason": "still short",
     "prov:generatedAtTime": {"@value": "2026-11-03T10:20:42.118Z", "@type": "xsd:dateTime"},
     "prov:wasGeneratedBy": {"@id": "urn:xkv:4f1c...:tx:3"},
     "prov:wasAttributedTo": {"@id": "urn:xkv:actor:sam"},
     "prov:wasRevisionOf": {"@id": "urn:xkv:4f1c...:rev:2"},
     "prov:wasDerivedFrom": [{"@id": "urn:xkv:4f1c...:rev:1"}]},
    {"@id": "urn:xkv:4f1c...:tx:3", "@type": "prov:Activity", "...": "..."},
    {"@id": "urn:xkv:actor:sam", "@type": "prov:Agent", "rdfs:label": "sam"}
  ]
}
```

## How it is checked

Go tests check the nodes, links, merging of shared transactions, base64
values, the depth limit's unexplored entities and that every reference
resolves. In CI, [scripts/check-prov.py](../scripts/check-prov.py) builds a
store with the xkv command, exports a tree and parses it with rdflib, a
JSON-LD and RDF library that shares no code with xkv, then checks the
expected triples, that every entity has a generating activity and every
activity an agent, and that every term under `prov:` is in rdflib's list of PROV-O terms.

PROV says what xkv recorded, not that it's true: reasons and causes are
whatever the writer gave.
