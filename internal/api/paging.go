package api

import (
	"bufio"
	"net/http"
	"strconv"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// Paging is opt-in and additive: without `limit` the response is the canonical one,
// byte for byte. With `limit`, the same envelope carries a `pagination` object after
// the data array, so a consumer that asked for pages knows where it is and one that
// did not sees no change.
//
// The reason this is safe here and refused by the canonical service is the version.
// There, page two would query a table the registry job may have rewritten between
// pages, so keys could be skipped or repeated. Here every page reads the version the
// cursor names.

// maxPageLimit bounds one page. Above this a consumer should use the full dump,
// which is streamed and costs less per key than many round trips.
const maxPageLimit = 100_000

// pageRequest is the parsed paging part of a request.
type pageRequest struct {
	enabled bool
	limit   int
	after   *store.KeyCursor
}

// parsePage reads `limit` and `cursor`.
func parsePage(r *http.Request) (pageRequest, error) {
	q := r.URL.Query()
	var p pageRequest

	if v := q.Get("cursor"); v != "" {
		cursor, err := store.ParseKeyCursor(v)
		if err != nil {
			return p, errBadRequest{[]string{err.Error()}}
		}
		p.after = &cursor
		p.enabled = true
		// A cursor without a limit still pages: the consumer clearly wants to
		// continue a walk, and defaulting to the whole rest of the version would
		// undo the point of having stopped.
		p.limit = 10_000
	}

	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return p, errBadRequest{[]string{"limit must be a positive integer"}}
		}
		if n > maxPageLimit {
			return p, errBadRequest{[]string{"limit must not be greater than " + strconv.Itoa(maxPageLimit)}}
		}
		p.limit = n
		p.enabled = true
	}

	return p, nil
}

// pinFor returns the pin a paged request has to be served at: the version named by
// the cursor, unless the request pinned one explicitly.
//
// This is what makes a walk consistent without asking the consumer to do anything.
// Page one is served from whatever was current, the cursor carries that version, and
// every later page follows it even as new versions are published.
func (p pageRequest) pinFor(r *http.Request) string {
	if explicit := r.URL.Query().Get("snapshot"); explicit != "" {
		return explicit
	}
	if p.after != nil {
		return strconv.FormatInt(p.after.Block, 10)
	}
	return ""
}

// handleKeysPage serves one page of /v1/keys.
func (s *Server) handleKeysPage(w http.ResponseWriter, r *http.Request, filter model.KeysFilter, page pageRequest) {
	ctx := r.Context()

	err := s.snapshotAt(ctx, r, page.pinFor(r), func(snap store.Snapshot) error {
		paged, ok := snap.(store.PagedSnapshot)
		if !ok {
			return store.ErrNotImplemented{What: "pagination"}
		}

		markStale(w, snap)

		elMeta, err := snap.ELMeta(ctx)
		if err != nil {
			return err
		}
		if elMeta == nil {
			tooEarly(w)
			return nil
		}

		w.Header().Set("Content-Type", "application/json")
		bw := bufio.NewWriterSize(w, streamBufferSize)
		defer bw.Flush()

		bw.WriteString(`{ "meta": {"elBlockSnapshot":`)
		bw.Write(marshalCompact(elMeta))
		bw.WriteString(`}, "data": [`)

		var (
			first    = true
			returned int
		)
		next, err := paged.StreamKeysPage(ctx, filter, store.KeyPage{Limit: page.limit, After: page.after},
			func(k *model.Key) error {
				if !first {
					bw.WriteByte(',')
				}
				first = false
				writeKey(bw, k)
				returned++
				return nil
			})
		if err != nil {
			return err
		}

		bw.WriteString(`], "pagination": {"limit":`)
		writeInt(bw, int64(page.limit))
		bw.WriteString(`,"returned":`)
		writeInt(bw, int64(returned))
		bw.WriteString(`,"snapshot":`)
		// The version's own block, not the head in the meta: that is what the cursor
		// pins to, and the head moves on every quiet cycle.
		writeInt(bw, paged.PinBlock())
		bw.WriteString(`,"cursor":`)
		if next == nil {
			// The walk is finished. A null cursor is the end condition, so a consumer
			// never has to compare counts against the limit to know.
			bw.WriteString(`null`)
		} else {
			bw.WriteByte('"')
			bw.WriteString(next.String())
			bw.WriteByte('"')
		}
		bw.WriteString(`}}`)
		return nil
	})
	if err != nil && !writeStoreError(w, err) {
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}
