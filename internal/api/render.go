package api

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// writeKey is the hot loop (once per key on a full dump): no reflection, no intermediate allocations.
func writeKey(w *bufio.Writer, k *model.Key) {
	w.WriteString(`{"key":"`)
	writeHexOrString(w, k.PubkeyBin, k.Key)
	w.WriteString(`","depositSignature":"`)
	writeHexOrString(w, k.SignatureBin, k.DepositSignature)
	w.WriteString(`","operatorIndex":`)
	writeInt(w, int64(k.OperatorIndex))
	w.WriteString(`,"used":`)
	writeBool(w, k.Used)
	w.WriteString(`,"moduleAddress":"`)
	w.WriteString(k.ModuleAddress)
	w.WriteString(`","index":`)
	writeInt(w, int64(k.Index))
	w.WriteString(`,"vetted":`)
	writeBool(w, k.Vetted)
	w.WriteByte('}')
}

// The stack scratch buffer fits the longest field (96-byte signature), so encoding allocates nothing.
func writeHexOrString(w *bufio.Writer, b []byte, s string) {
	if b == nil {
		w.WriteString(s)
		return
	}
	var scratch [192]byte
	n := hex.Encode(scratch[:], b)
	w.WriteString("0x")
	w.Write(scratch[:n])
}

func writeInt(w *bufio.Writer, v int64) {
	var buf [20]byte
	w.Write(strconv.AppendInt(buf[:0], v, 10))
}

func writeBool(w *bufio.Writer, v bool) {
	if v {
		w.WriteString("true")
		return
	}
	w.WriteString("false")
}

func marshalCompact(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		// Only fixed structs are passed here, so this cannot fail in practice.
		return []byte("null")
	}
	return b
}
