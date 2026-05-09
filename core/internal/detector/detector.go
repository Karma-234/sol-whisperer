package core

import (
	"github.com/karma-234/sol-whisperer/core"
	"github.com/karma-234/sol-whisperer/core/internal/types"
)

// SourceNameMap normalizes Helius source labels to our internal program names.
var SourceNameMap = map[string]string{
	"RAYDIUM":  "raydium_v4", // default to v4; inspect instructions for exact version
	"PUMP_AMM": "pump_fun",
	"JUPITER":  "jupiter",
	"ORCA":     "orca",
	"SOLEND":   "solend",
	"MARINADE": "marinade",
}

// DetectedProgram holds matched program info.
type DetectedProgram struct {
	Address string // on-chain program address
	Name    string // internal name from ProgramNames
}

// DetectProgramsFromInstructions walks all instructions (including inner)
// and returns matched program names and addresses from the webhook tx.
func DetectProgramsFromInstructions(tx types.HeliusEnhancedWebhookTx) []DetectedProgram {
	seen := make(map[string]struct{}) // dedup by address
	var result []DetectedProgram

	var walk func(ix types.HeliusInstruction)
	walk = func(ix types.HeliusInstruction) {
		if _, exists := seen[ix.ProgramID]; exists {
			return
		}
		seen[ix.ProgramID] = struct{}{}

		if name, ok := core.ProgramNames[ix.ProgramID]; ok {
			result = append(result, DetectedProgram{
				Address: ix.ProgramID,
				Name:    name,
			})
		}

		for _, inner := range ix.InnerInstructions {
			walk(inner)
		}
	}

	for _, ix := range tx.Instructions {
		walk(ix)
	}

	return result
}

// NormalizeSourceLabel maps the Helius source string to our internal name,
// falling back to the source string if no mapping exists.
func NormalizeSourceLabel(source string) string {
	if normalized, ok := SourceNameMap[source]; ok {
		return normalized
	}
	return source // passthrough if unknown
}

// SwapInfo is a normalized representation of a swap event.
type SwapInfo struct {
	Signature        string
	Timestamp        int64
	Source           string            // normalized internal name
	DetectedPrograms []DetectedProgram // matched ProgramID + name pairs
	TokenInputs      []types.HeliusTokenAmountEvent
	TokenOutputs     []types.HeliusTokenAmountEvent
	NativeInput      *types.HeliusNativeAmount
	NativeOutput     *types.HeliusNativeAmount
	Fee              uint64
	FeePayer         string
	Description      string
}

// ExtractSwapInfo normalizes a SWAP transaction into actionable struct.
func ExtractSwapInfo(tx types.HeliusEnhancedWebhookTx) *SwapInfo {
	if tx.Type != "SWAP" || tx.Events.Swap == nil {
		return nil
	}

	return &SwapInfo{
		Signature:        tx.Signature,
		Timestamp:        tx.Timestamp,
		Source:           NormalizeSourceLabel(tx.Source),
		DetectedPrograms: DetectProgramsFromInstructions(tx),
		TokenInputs:      tx.Events.Swap.TokenInputs,
		TokenOutputs:     tx.Events.Swap.TokenOutputs,
		NativeInput:      tx.Events.Swap.NativeInput,
		NativeOutput:     tx.Events.Swap.NativeOutput,
		Fee:              tx.Fee,
		FeePayer:         tx.FeePayer,
		Description:      tx.Description,
	}
}
