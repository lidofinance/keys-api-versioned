package api

import (
	"net/http"
	"strconv"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// Paging is safe here, unlike in the canonical service, because every page reads the version the
// cursor names rather than a table the registry job may rewrite between pages.

// Above this a consumer should use the streamed full dump.
const maxPageLimit = 100_000

type pageRequest struct {
	enabled bool
	limit   int
	after   *store.KeyCursor
}

func parsePage(r *http.Request) (pageRequest, error) {
	if !onExtSurface(r) {
		return pageRequest{}, nil
	}

	q := r.URL.Query()
	var p pageRequest

	if v := q.Get("cursor"); v != "" {
		cursor, err := store.ParseKeyCursor(v)
		if err != nil {
			return p, errBadRequest{[]string{err.Error()}}
		}
		// Refused, not resolved: re-sending ?snapshot=finalized with each cursor would mix versions as
		// finality advances mid-walk, silently. This fails on page two, deterministically.
		if q.Get("snapshot") != "" {
			return p, errBadRequest{[]string{
				"cursor and snapshot cannot be combined: the cursor names the version the " +
					"walk is on — pin with snapshot on the first page, then send the cursor alone"}}
		}
		p.after = &cursor
		p.enabled = true
		// A cursor without a limit still pages.
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

// pinFor: the cursor names page one's version by hash, so one replaced mid-walk answers 409.
func (p pageRequest) pinFor(r *http.Request) string {
	if p.after != nil {
		return p.after.Hash
	}
	return r.URL.Query().Get("snapshot")
}

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
		bw, out := s.newStreamWriter(ctx, w)
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
				return out.Err()
			})
		if err != nil {
			return err
		}

		bw.WriteString(`], "pagination": {"limit":`)
		writeInt(bw, int64(page.limit))
		bw.WriteString(`,"returned":`)
		writeInt(bw, int64(returned))
		bw.WriteString(`,"snapshot":`)
		// The version's own block, not the head in meta: the cursor pins to it.
		writeInt(bw, paged.PinBlock())
		bw.WriteString(`,"cursor":`)
		if next == nil {
			// A null cursor is the end condition.
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
		s.internalError(w, r, err)
	}
}
