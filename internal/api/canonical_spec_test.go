package api

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// testdata/canonical-openapi.json is the document the canonical service publishes at /api-json,
// taken from the mainnet deployment of the release named in its info.version. `make
// canonical-spec` refreshes it; the canonical-spec workflow fails when the deployment moves on.
const canonicalSpecFile = "testdata/canonical-openapi.json"

// Canonical operations this service does not serve. Both need the consensus-layer validator set,
// which is not ingested here.
var canonicalNotServed = map[string]string{
	"get /v1/modules/{module_id}/validators/validator-exits-to-prepare/{operator_id}":      "no validator ingest",
	"get /v1/modules/{module_id}/validators/generate-unsigned-exit-messages/{operator_id}": "no validator ingest",
}

// Canonical response fields this service declares narrower, keyed "method path status field".
// Everything below the field is covered too.
var canonicalNarrowed = map[string]string{
	"get /v1/status 200 .clBlockSnapshot": "always null: no validator ingest",
}

func TestOurSpecCoversTheCanonicalSpec(t *testing.T) {
	canonical, ours := loadCanonicalSpec(t), loadOurSpec(t)
	narrowedUsed := map[string]bool{}

	for _, op := range operations(canonical) {
		item := pathItem(ours, op.path)
		ourOp, served := item[op.method].(map[string]any)

		if reason, excluded := canonicalNotServed[op.key()]; excluded {
			if served {
				t.Errorf("%s is listed as not served (%s), but our spec documents it: drop it from canonicalNotServed",
					op.key(), reason)
			}
			continue
		}
		if !served {
			t.Errorf("%s is in the canonical spec (%s) but not in openapi.yaml", op.key(), specVersion(canonical))
			continue
		}

		ourParams := parameters(ours, item, ourOp)
		for p := range parameters(canonical, pathItem(canonical, op.path), op.op) {
			if !ourParams[p] {
				t.Errorf("%s: canonical parameter %s is missing or differs", op.key(), p)
			}
		}

		if body := jsonSchema(canonical, op.op["requestBody"]); body != nil {
			compareShapes(t, op.key()+" request body", canonical, body, ours, jsonSchema(ours, ourOp["requestBody"]), nil)
		}

		ourResponses, _ := ourOp["responses"].(map[string]any)
		canonicalResponses, _ := op.op["responses"].(map[string]any)
		for _, code := range sortedKeys(canonicalResponses) {
			ourResponse, ok := ourResponses[code]
			if !ok {
				t.Errorf("%s: canonical status %s is not documented", op.key(), code)
				continue
			}
			schema := jsonSchema(canonical, canonicalResponses[code])
			if schema == nil {
				continue
			}
			prefix := op.key() + " " + code
			compareShapes(t, prefix, canonical, schema, ours, jsonSchema(ours, ourResponse), func(field string) bool {
				for k := range canonicalNarrowed {
					if base, ok := strings.CutPrefix(k, prefix+" "); ok && (field == base || strings.HasPrefix(field, base+".")) {
						narrowedUsed[k] = true
						return true
					}
				}
				return false
			})
		}
	}

	for k, reason := range canonicalNarrowed {
		if !narrowedUsed[k] {
			t.Errorf("canonicalNarrowed %q (%s) no longer differs from the canonical spec: drop it", k, reason)
		}
	}
}

// What a consumer can rely on: every field the canonical document declares is in ours with a
// compatible type. Ours may add fields and widen a type into a union; the live diff holds the
// /v1 bytes to the canonical ones, which a document cannot.
func compareShapes(t *testing.T, where string, cdoc, cschema, odoc, oschema map[string]any, narrowed func(string) bool) {
	t.Helper()
	want, got := map[string]string{}, map[string]string{}
	shape(cdoc, cschema, "", want, 0)
	if oschema != nil {
		shape(odoc, oschema, "", got, 0)
	}
	for _, field := range sortedKeys(want) {
		if compatible(want[field], got[field]) || (narrowed != nil && narrowed(field)) {
			continue
		}
		if got[field] == "" {
			t.Errorf("%s: canonical field %s (%s) is not documented", where, field, want[field])
		} else {
			t.Errorf("%s: field %s is %s in the canonical spec, %s in ours", where, field, want[field], got[field])
		}
	}
}

func compatible(want, got string) bool {
	if want == got {
		return true
	}
	members, ok := strings.CutPrefix(got, "oneOf:")
	return ok && strings.Contains(","+members+",", ","+want+",")
}

// shape flattens a schema into field path → type: ".data[].key" → "string".
func shape(doc, schema map[string]any, path string, out map[string]string, depth int) {
	schema = deref(doc, schema)
	if schema == nil || depth > 8 {
		return
	}
	field := path
	if field == "" {
		field = "."
	}
	if alts, ok := schema["oneOf"].([]any); ok {
		var types []string
		for _, a := range alts {
			types = append(types, typeOf(deref(doc, asMap(a))))
		}
		out[field] = "oneOf:" + strings.Join(types, ",")
		return
	}
	switch typ := typeOf(schema); typ {
	case "array":
		out[field] = typ
		shape(doc, asMap(schema["items"]), path+"[]", out, depth+1)
	case "object":
		out[field] = typ
		props, _ := schema["properties"].(map[string]any)
		for name, p := range props {
			shape(doc, asMap(p), path+"."+name, out, depth+1)
		}
	default:
		out[field] = typ
	}
}

// integer and number are one JSON type; an untyped schema with properties is an object.
func typeOf(schema map[string]any) string {
	typ, _ := schema["type"].(string)
	switch {
	case typ == "integer":
		return "number"
	case typ == "" && schema["properties"] != nil:
		return "object"
	case typ == "" && schema["items"] != nil:
		return "array"
	}
	return typ
}

// deref follows $ref and a single-element allOf, the shape both documents use for a nullable ref.
func deref(doc, schema map[string]any) map[string]any {
	for range 16 {
		if ref, ok := schema["$ref"].(string); ok {
			schema = resolve(doc, ref)
			continue
		}
		if all, ok := schema["allOf"].([]any); ok && len(all) == 1 {
			schema = asMap(all[0])
			continue
		}
		return schema
	}
	return schema
}

func resolve(doc map[string]any, ref string) map[string]any {
	node := doc
	for _, part := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
		node = asMap(node[part])
	}
	return node
}

// jsonSchema is the application/json schema of a request body or a response, either inline or by $ref.
func jsonSchema(doc map[string]any, bodyOrResponse any) map[string]any {
	m := deref(doc, asMap(bodyOrResponse))
	content := asMap(asMap(m["content"])["application/json"])
	if s, ok := content["schema"].(map[string]any); ok {
		return s
	}
	return nil
}

// parameters as "in:name required=bool", from the path item and the operation.
func parameters(doc, item, op map[string]any) map[string]bool {
	out := map[string]bool{}
	for _, list := range []any{item["parameters"], op["parameters"]} {
		ps, _ := list.([]any)
		for _, p := range ps {
			pm := deref(doc, asMap(p))
			required, _ := pm["required"].(bool)
			out[pm["in"].(string)+":"+pm["name"].(string)+" required="+map[bool]string{true: "true", false: "false"}[required]] = true
		}
	}
	return out
}

type operation struct {
	method, path string
	op           map[string]any
}

func (o operation) key() string { return o.method + " " + o.path }

func operations(doc map[string]any) []operation {
	var out []operation
	paths := asMap(doc["paths"])
	for _, path := range sortedKeys(paths) {
		item := asMap(paths[path])
		for _, method := range sortedKeys(item) {
			if op, ok := item[method].(map[string]any); ok && method != "parameters" {
				out = append(out, operation{method, path, op})
			}
		}
	}
	return out
}

func pathItem(doc map[string]any, path string) map[string]any {
	return asMap(asMap(doc["paths"])[path])
}

func specVersion(doc map[string]any) string {
	v, _ := asMap(doc["info"])["version"].(string)
	return "canonical " + v
}

// canonicalPathSet is every path the canonical service owns, served here or not.
func canonicalPathSet(t *testing.T) map[string]bool {
	out := map[string]bool{}
	for _, op := range operations(loadCanonicalSpec(t)) {
		out[op.path] = true
	}
	return out
}

func loadCanonicalSpec(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(canonicalSpecFile)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("%s: %v", canonicalSpecFile, err)
	}
	if n := len(asMap(doc["paths"])); n < 10 {
		t.Fatalf("%s has %d paths, which looks like a truncated download", canonicalSpecFile, n)
	}
	return doc
}

func loadOurSpec(t *testing.T) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := yaml.Unmarshal(openAPISpec, &doc); err != nil {
		t.Fatalf("openapi.yaml: %v", err)
	}
	return doc
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
