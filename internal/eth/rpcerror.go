package eth

import "strings"

// A JSON-RPC error arrives in a successful HTTP response. Endpoint errors fail over; call errors
// must stay returned: the multicall bisection depends on seeing "out of gas".

// Checked first: some call errors contain words of endpoint limits ("gas limit", "exceeds").
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

// Matched on the message, not only the code: codes are not standardised across providers.
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
	// Plan restrictions: a batch size one provider refuses, another carries.
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
	// Capability: a node without the method or archive state; another endpoint usually has it.
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

// -32005 is Infura/Alchemy "limit exceeded", -32029 a gateway rate limit, -32601 a method the node
// lacks, -32603 the node's own failure; HTTP statuses also come reflected into the error.
var endpointErrorCodes = map[int]bool{
	-32005: true,
	-32029: true,
	-32097: true,
	-32601: true,
	-32603: true,
	401:    true,
	402:    true,
	403:    true,
	429:    true,
	500:    true,
	502:    true,
	503:    true,
	504:    true,
}

// 3 is geth's "execution reverted" with revert data: the call's own outcome on any node.
var callErrorCodes = map[int]bool{3: true}

// Endpoint refusals that are about the request's size, so a smaller aggregate may pass.
var sizeRefusalPatterns = []string{
	"response size exceeded",
	"response too large",
	"request entity too large",
	"payload too large",
}

func sizeRefusal(message string) bool {
	lower := strings.ToLower(message)
	for _, pattern := range sizeRefusalPatterns {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}

// classify treats unknown errors as call-level: a wrongly-returned error just fails the cycle, a
// wrongly-retried one spends requests on every endpoint and still fails. known=false is counted
// and logged, so the lists can follow what providers actually send.
func classify(code int, message string) (endpoint, known bool) {
	lower := strings.ToLower(message)
	for _, pattern := range callErrorPatterns {
		if strings.Contains(lower, pattern) {
			return false, true
		}
	}
	if endpointErrorCodes[code] {
		return true, true
	}
	if callErrorCodes[code] {
		return false, true
	}
	for _, pattern := range endpointErrorPatterns {
		if strings.Contains(lower, pattern) {
			return true, true
		}
	}
	return false, false
}

func endpointFailure(code int, message string) bool {
	endpoint, _ := classify(code, message)
	return endpoint
}
