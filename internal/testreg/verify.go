package testreg

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// Verifier checks a response against the registry, streaming.
//
// Two things are checked per key and one at the end. Per key: the pubkey decodes to an
// identity, its hash tail matches, its module agrees with the moduleAddress it was
// served under, its salt is the one expected for that module, and its used/vetted
// flags are the ones the spec implies. At the end: every expected key appeared exactly
// once.
//
// Membership is a bitset per module rather than a checksum, because the two failures
// worth telling apart — a gap and a duplicate — are indistinguishable in a sum. A
// bitset for 500 000 keys is 62 KiB, and one key at a time is decoded from the body,
// so verifying a 220 MiB dump costs a few MiB.
type Verifier struct {
	spec     Spec
	salt     map[int]uint64
	modules  []int
	partial  bool
	seen     map[int][]uint64
	count    map[int]int
	problems []string
}

// NewVerifier expects every module of the spec at the spec's own salt.
func NewVerifier(s Spec) *Verifier {
	v := &Verifier{
		spec:    s,
		salt:    map[int]uint64{},
		modules: s.ModuleIDs(),
		seen:    map[int][]uint64{},
		count:   map[int]int{},
	}
	for _, id := range v.modules {
		v.salt[id] = s.Salt
		v.seen[id] = make([]uint64, (s.KeysPerModule()+63)/64)
	}
	return v
}

// Expect overrides the salt a module's keys must carry. This is what a torn-read test
// asserts on: one module at the old salt and another at the new one in the same
// response is a dump stitched from two versions.
func (v *Verifier) Expect(moduleID int, salt uint64) *Verifier {
	v.salt[moduleID] = salt
	return v
}

// Only narrows the response to the named modules, for a module-scoped endpoint.
func (v *Verifier) Only(moduleIDs ...int) *Verifier {
	v.modules = moduleIDs
	return v
}

// Partial stops Done from requiring completeness, for a filtered response where not
// every key is expected. Per-key checks still apply.
func (v *Verifier) Partial() *Verifier {
	v.partial = true
	return v
}

// Key checks one key and records that it was seen.
func (v *Verifier) Key(k *model.Key) error {
	id, ok := Decode(k.PubkeyBin)
	if !ok {
		return v.fail("key at module %s index %d: pubkey %s is not one this registry generated",
			k.ModuleAddress, k.Index, model.EncodeHex(k.PubkeyBin))
	}

	servedBy, ok := ModuleIDFromAddress(k.ModuleAddress)
	if !ok {
		return v.fail("%s: served under moduleAddress %q, which is not a module of this registry",
			id, k.ModuleAddress)
	}
	if servedBy != id.Module {
		return v.fail("%s: served under module %d — the key and the module it was served with disagree",
			id, servedBy)
	}
	if want, known := v.salt[id.Module]; known && id.Salt != want {
		return v.fail("%s: expected salt %d for module %d — this key is from another version of it",
			id, want, id.Module)
	}
	if id.Operator != int(k.OperatorIndex) || id.Index != int(k.Index) {
		return v.fail("%s: served as operator %d index %d", id, k.OperatorIndex, k.Index)
	}

	expected := v.spec.WithSalt(id.Salt).Key(id.Module, id.Operator, id.Index)
	if model.EncodeHex(k.SignatureBin) != model.EncodeHex(expected.SignatureBin) {
		return v.fail("%s: signature does not belong to this pubkey", id)
	}
	if k.Used != v.spec.used(id.Module, id.Operator, id.Index) {
		return v.fail("%s: used=%t, want %t", id, k.Used, !k.Used)
	}
	if !k.Vetted {
		return v.fail("%s: vetted=false, want true", id)
	}

	return v.mark(id)
}

// mark records membership and catches a key served twice.
func (v *Verifier) mark(id Identity) error {
	bits, tracked := v.seen[id.Module]
	if !tracked {
		return v.fail("%s: module %d is not part of this expectation", id, id.Module)
	}
	ordinal := id.Operator*v.spec.KeysPerOperator + id.Index
	if ordinal < 0 || ordinal/64 >= len(bits) {
		return v.fail("%s: outside the registry's shape", id)
	}
	word, bit := ordinal/64, uint(ordinal%64)
	if bits[word]&(1<<bit) != 0 {
		return v.fail("%s: served twice", id)
	}
	bits[word] |= 1 << bit
	v.count[id.Module]++
	return nil
}

// Done reports what the response was missing, or nothing.
func (v *Verifier) Done() error {
	if len(v.problems) > 0 {
		return errors.New(v.summary())
	}
	if v.partial {
		return nil
	}

	perModule := v.spec.KeysPerModule()
	for _, id := range v.modules {
		if got := v.count[id]; got != perModule {
			v.fail("module %d: %d keys, want %d — missing %v",
				id, got, perModule, v.firstMissing(id, 5))
		}
	}
	if len(v.problems) > 0 {
		return errors.New(v.summary())
	}
	return nil
}

// Keys is how many keys passed, for a test that wants to assert the count itself.
func (v *Verifier) Keys() int {
	total := 0
	for _, n := range v.count {
		total += n
	}
	return total
}

// firstMissing names the gaps, because "1 key short of 60 000" is not actionable and
// "operator 3 index 17 is missing" is.
func (v *Verifier) firstMissing(moduleID, limit int) []Identity {
	var out []Identity
	bits := v.seen[moduleID]
	for ordinal := 0; ordinal < v.spec.KeysPerModule() && len(out) < limit; ordinal++ {
		if bits[ordinal/64]&(1<<uint(ordinal%64)) == 0 {
			out = append(out, Identity{
				Module:   moduleID,
				Operator: ordinal / v.spec.KeysPerOperator,
				Index:    ordinal % v.spec.KeysPerOperator,
				Salt:     v.salt[moduleID],
			})
		}
	}
	return out
}

// fail records a problem and returns it. Recording rather than only returning means a
// streaming caller that stops at the first error still gets the full list from Done,
// and a caller that keeps going does not lose the earlier ones.
func (v *Verifier) fail(format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	if len(v.problems) < 20 {
		v.problems = append(v.problems, msg)
	}
	return errors.New(msg)
}

func (v *Verifier) summary() string {
	sorted := append([]string(nil), v.problems...)
	sort.Strings(sorted)
	out := fmt.Sprintf("%d problem(s):", len(v.problems))
	for _, p := range sorted {
		out += "\n  " + p
	}
	return out
}

// jsonKey is the served key, whose hex fields have to come back as bytes for the
// identity check.
type jsonKey struct {
	Key              string `json:"key"`
	DepositSignature string `json:"depositSignature"`
	OperatorIndex    int32  `json:"operatorIndex"`
	Used             bool   `json:"used"`
	ModuleAddress    string `json:"moduleAddress"`
	Index            int32  `json:"index"`
	Vetted           bool   `json:"vetted"`
}

func (j jsonKey) key() (*model.Key, error) {
	pubkey, err := model.DecodeHex(j.Key)
	if err != nil {
		return nil, fmt.Errorf("pubkey %q: %w", j.Key, err)
	}
	signature, err := model.DecodeHex(j.DepositSignature)
	if err != nil {
		return nil, fmt.Errorf("signature %q: %w", j.DepositSignature, err)
	}
	return &model.Key{
		PubkeyBin: pubkey, SignatureBin: signature,
		OperatorIndex: j.OperatorIndex, Index: j.Index, Used: j.Used,
		ModuleAddress: j.ModuleAddress, Vetted: j.Vetted,
	}, nil
}

// Response is what a verified response carried besides its keys.
type Response struct {
	Meta       *model.ELBlockSnapshot
	Pagination *Pagination
	Modules    []model.StakingModule
}

// Pagination is the paging block a paged response adds after its data.
type Pagination struct {
	Limit    int    `json:"limit"`
	Returned int    `json:"returned"`
	Snapshot int64  `json:"snapshot"`
	Cursor   string `json:"cursor"`
}

// V1Keys verifies a flat keys response: `{"meta": …, "data": [ … ]}`. It streams, so
// the body is never held whole, and it returns what the envelope carried.
//
// A truncated body — which is how an aborted stream arrives — surfaces here as an
// error from the decoder, and that is the signal a test asserts on: a response that
// parses cleanly to the end is a complete answer, whatever else was going on.
func (v *Verifier) V1Keys(r io.Reader) (Response, error) {
	return v.walk(r, func(dec *json.Decoder, field string, out *Response) error {
		switch field {
		case "data":
			return v.keyArray(dec)
		case "meta":
			var meta struct {
				ELBlockSnapshot *model.ELBlockSnapshot `json:"elBlockSnapshot"`
			}
			if err := dec.Decode(&meta); err != nil {
				return err
			}
			out.Meta = meta.ELBlockSnapshot
			return nil
		case "pagination":
			var page Pagination
			if err := dec.Decode(&page); err != nil {
				return err
			}
			out.Pagination = &page
			return nil
		default:
			return skip(dec)
		}
	})
}

// GroupedKeys verifies `/v1/modules/keys`: data is an array of `{keys: […], module: …}`.
// The grouped shape is worth its own path because it is the endpoint that used to
// build the whole response in memory, and because a stitch shows up between two
// groups rather than inside one array.
func (v *Verifier) GroupedKeys(r io.Reader) (Response, error) {
	return v.walk(r, func(dec *json.Decoder, field string, out *Response) error {
		switch field {
		case "data":
			return v.groupArray(dec, out)
		case "meta":
			var meta struct {
				ELBlockSnapshot *model.ELBlockSnapshot `json:"elBlockSnapshot"`
			}
			if err := dec.Decode(&meta); err != nil {
				return err
			}
			out.Meta = meta.ELBlockSnapshot
			return nil
		default:
			return skip(dec)
		}
	})
}

// walk reads a JSON object field by field, handing each to fn.
func (v *Verifier) walk(r io.Reader, fn func(*json.Decoder, string, *Response) error) (Response, error) {
	var out Response
	dec := json.NewDecoder(r)

	if tok, err := dec.Token(); err != nil {
		return out, fmt.Errorf("read the start of the response: %w", err)
	} else if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return out, fmt.Errorf("response starts with %v, want an object", tok)
	}

	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return out, fmt.Errorf("read a field name: %w", err)
		}
		field, ok := tok.(string)
		if !ok {
			return out, fmt.Errorf("field name is %v", tok)
		}
		if err := fn(dec, field, &out); err != nil {
			return out, err
		}
	}
	if _, err := dec.Token(); err != nil {
		return out, fmt.Errorf("read the end of the response: %w", err)
	}
	return out, nil
}

func (v *Verifier) keyArray(dec *json.Decoder) error {
	if tok, err := dec.Token(); err != nil {
		return fmt.Errorf("read the start of data: %w", err)
	} else if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("data starts with %v, want an array", tok)
	}
	for dec.More() {
		var jk jsonKey
		if err := dec.Decode(&jk); err != nil {
			return fmt.Errorf("decode a key: %w", err)
		}
		k, err := jk.key()
		if err != nil {
			return err
		}
		if err := v.Key(k); err != nil {
			return err
		}
	}
	_, err := dec.Token()
	return err
}

func (v *Verifier) groupArray(dec *json.Decoder, out *Response) error {
	if tok, err := dec.Token(); err != nil {
		return fmt.Errorf("read the start of data: %w", err)
	} else if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("data starts with %v, want an array", tok)
	}
	for dec.More() {
		var group struct {
			Keys   []jsonKey           `json:"keys"`
			Module model.StakingModule `json:"module"`
		}
		if err := dec.Decode(&group); err != nil {
			return fmt.Errorf("decode a module group: %w", err)
		}
		out.Modules = append(out.Modules, group.Module)
		for i := range group.Keys {
			k, err := group.Keys[i].key()
			if err != nil {
				return err
			}
			if err := v.Key(k); err != nil {
				return err
			}
		}
	}
	_, err := dec.Token()
	return err
}

// skip consumes one value, whatever it is.
func skip(dec *json.Decoder) error {
	var discard json.RawMessage
	return dec.Decode(&discard)
}
