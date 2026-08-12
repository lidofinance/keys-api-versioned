package eth

import "strings"

// An endpoint can fail a request in two entirely different ways, and until this
// distinction existed the client treated both as the same thing: it returned the error
// to the caller and never tried the next endpoint in the list.
//
// That cost a day of downtime. A dRPC key hit its daily compute-unit limit and answered
// every call with `Your key daily limit exceeded`; an Alchemy endpoint was configured
// right behind it, healthy and untouched, and the service never sent it a single
// request — because a JSON-RPC error arrives in a *successful* HTTP response, and the
// fallback loop only advanced on transport failures.
//
// So the question a failed call has to answer is not "did it fail" but "is this about
// the endpoint or about the call":
//
//   - About the endpoint: a quota, a rate limit, a batch size this plan refuses, an
//     expired key, a method this node does not serve. Another endpoint would answer.
//     These fail over.
//   - About the call: `execution reverted`, `out of gas`, a bad parameter. Every
//     endpoint would answer the same, so failing over multiplies the requests and
//     changes nothing.
//
// The second group must stay a returned error for a reason beyond politeness: the
// Multicall3 path *depends* on seeing `out of gas`. It is how an aggregate that asked
// for too much gets bisected and the budget lowered — see splitAggregate. Failing over
// on it would turn one refused aggregate into a walk down the endpoint list and then a
// hard failure, losing the bisect that makes the aggregation work at all.

// callErrorPatterns are outcomes of executing the call. Checked first, because some of
// them contain words that also appear in endpoint limits — "gas limit", "exceeds" —
// and a call-level answer must never be mistaken for a sick endpoint.
var callErrorPatterns = []string{
	"execution reverted",
	"out of gas",
	"gas required exceeds",
	"intrinsic gas",
	"invalid opcode",
	"stack underflow",
	"nonce too low",
	"already known",
}

// endpointErrorPatterns say the endpoint will not serve this request, whoever asked.
// Matched on the message rather than only on the code because the codes are not
// standardised across providers: dRPC reports its daily limit as 8, Infura and Alchemy
// use -32005, and some gateways answer 429 with a JSON-RPC body.
var endpointErrorPatterns = []string{
	// Quotas and rate limits.
	"daily limit",
	"day limit",
	"monthly limit",
	"rate limit",
	"ratelimit",
	"too many requests",
	"exceeded the quota",
	"quota exceeded",
	"compute units",
	"capacity exceeded",
	"request limit",
	"throttl",
	// Plan restrictions. The batch one is dRPC's free plan refusing more than three
	// calls in a request, which another provider carries without complaint.
	"not allowed on free plan",
	"batch of more than",
	"batch size",
	"upgrade your plan",
	"payment required",
	// Credentials.
	"unauthorized",
	"forbidden",
	"invalid api key",
	"invalid key",
	"api key",
	"access denied",
	// Capability. A node without the method, or without the archive state this call
	// needs, is a routing problem: the fallback list usually has one that does.
	"method not found",
	"method not supported",
	"does not exist/is not available",
	"unsupported method",
	"missing trie node",
	"state is not available",
	"header not found",
	"block range",
	"query returned more than",
	"response size exceeded",
	"request entity too large",
	// The endpoint saying it is unwell.
	"internal error",
	"service unavailable",
	"temporarily unavailable",
	"try again",
	"timeout",
	"upstream",
}

// endpointErrorCodes are the codes that mean the endpoint refused, independent of
// wording. -32005 is "limit exceeded" as Infura and Alchemy send it; -32029 is the
// rate-limit code some gateways use; 429 and 503 appear when an HTTP status is
// reflected into the JSON-RPC error.
var endpointErrorCodes = map[int]bool{
	-32005: true,
	-32029: true,
	-32097: true,
	429:    true,
	503:    true,
}

// endpointFailure reports whether an error the endpoint returned is about the endpoint
// rather than about the call — and therefore whether the next endpoint is worth trying.
//
// Unknown errors are treated as call-level, which is the conservative direction: a
// wrongly-retried call spends requests on every endpoint and still fails, whereas a
// wrongly-returned one fails the cycle and is retried on the next tick anyway. The
// pattern lists are additive, and the cost of a missing entry is bounded.
func endpointFailure(code int, message string) bool {
	lower := strings.ToLower(message)
	for _, pattern := range callErrorPatterns {
		if strings.Contains(lower, pattern) {
			return false
		}
	}
	if endpointErrorCodes[code] {
		return true
	}
	for _, pattern := range endpointErrorPatterns {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}
