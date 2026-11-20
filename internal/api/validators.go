package api

import (
	"math"
	"math/big"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/lidofinance/keys-api-versioned/internal/ethaddr"
)

// registryDisabled is the canonical service's answer when it runs without its validators registry
// (VALIDATOR_REGISTRY_ENABLE=false). This service has no registry, so it always answers so.
const registryDisabled = "Validators Registry is disabled. Check environment variables"

// handleValidatorExits serves both exit routes. The canonical service validates module_id first,
// then the query, then operator_id, and reports only the first parameter that fails.
func (s *Server) handleValidatorExits(w http.ResponseWriter, r *http.Request) {
	if msg := moduleIDError(r.PathValue("module_id")); msg != "" {
		writeError(w, http.StatusBadRequest, []string{msg})
		return
	}
	var queryErrs []string
	for _, name := range []string{"max_amount", "percent"} {
		values, set := r.URL.Query()[name]
		if !set {
			continue
		}
		// A repeated parameter arrives as an array, and Number() of an array is NaN.
		v := math.NaN()
		if len(values) == 1 {
			if values[0] == "" {
				continue
			}
			v = jsNumber(values[0])
		}
		queryErrs = append(queryErrs, nonNegativeIntErrors(name, v)...)
	}
	if len(queryErrs) > 0 {
		writeError(w, http.StatusBadRequest, queryErrs)
		return
	}
	if errs := nonNegativeIntErrors("operator_id", jsNumber(r.PathValue("operator_id"))); len(errs) > 0 {
		writeError(w, http.StatusBadRequest, errs)
		return
	}
	writeError(w, http.StatusInternalServerError, registryDisabled)
}

var (
	moduleIndex   = regexp.MustCompile(`^(0|[1-9]\d*)$`)
	moduleAddress = regexp.MustCompile(`^(0x)?[0-9a-fA-F]{40}$`)
	mixedCase     = regexp.MustCompile(`[A-F].*[a-f]|[a-f].*[A-F]`)
)

// moduleIDError is the canonical ModuleIdPipe: an address as ethers v5 isAddress accepts it (0x
// optional, mixed case only with a valid checksum; ICAP is not supported here) or a plain index.
func moduleIDError(raw string) string {
	ref := jsTrim(raw)
	if moduleAddress.MatchString(ref) {
		with0x := ref
		if !strings.HasPrefix(ref, "0x") {
			with0x = "0x" + ref
		}
		if !mixedCase.MatchString(with0x[2:]) || ethaddr.IsChecksummed(with0x) {
			// The pipe compares against the zero address with its 0x, so 40 zeros without it pass.
			if strings.ToLower(ref) == "0x0000000000000000000000000000000000000000" {
				return "module_id cannot be the zero address"
			}
			return ""
		}
	}
	if moduleIndex.MatchString(ref) {
		return ""
	}
	return "module_id must be a contract address or numeric value"
}

// nonNegativeIntErrors is class-validator's @IsInt() @Min(0), in the order the canonical service
// reports them.
func nonNegativeIntErrors(name string, v float64) []string {
	var errs []string
	if !(v >= 0) {
		errs = append(errs, name+" must not be less than 0")
	}
	if math.IsNaN(v) || math.IsInf(v, 0) || v != math.Trunc(v) {
		errs = append(errs, name+" must be an integer number")
	}
	return errs
}

var (
	jsDecimal    = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$`)
	jsNonDecimal = regexp.MustCompile(`^0([xX][0-9a-fA-F]+|[oO][0-7]+|[bB][01]+)$`)
)

// jsNumber is JavaScript's Number(string): surrounding whitespace ignored, empty is 0, a sign only
// on decimals, 0x/0o/0b integers, Infinity, NaN for anything else.
func jsNumber(s string) float64 {
	s = jsTrim(s)
	switch {
	case s == "":
		return 0
	case s == "Infinity" || s == "+Infinity":
		return math.Inf(1)
	case s == "-Infinity":
		return math.Inf(-1)
	case jsDecimal.MatchString(s):
		v, err := strconv.ParseFloat(s, 64)
		if err != nil && !isRange(err) {
			return math.NaN()
		}
		return v
	case jsNonDecimal.MatchString(s):
		base := map[byte]int{'x': 16, 'o': 8, 'b': 2}[s[1]|0x20]
		n, _ := new(big.Int).SetString(s[2:], base)
		v, _ := new(big.Float).SetInt(n).Float64()
		return v
	}
	return math.NaN()
}

// ParseFloat reports overflow with ±Inf and underflow with 0, which is what Number() returns too.
func isRange(err error) bool {
	ne, ok := err.(*strconv.NumError)
	return ok && ne.Err == strconv.ErrRange
}

// jsTrim strips what String.prototype.trim strips: WhiteSpace and LineTerminator.
func jsTrim(s string) string {
	return strings.TrimFunc(s, func(r rune) bool {
		switch r {
		case '\t', '\n', '\v', '\f', '\r', ' ', 0xa0, 0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000, 0xfeff:
			return true
		}
		return r >= 0x2000 && r <= 0x200a
	})
}
