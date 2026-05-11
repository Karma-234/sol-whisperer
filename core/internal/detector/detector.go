package detector

import (
	"strconv"

	"github.com/karma-234/sol-whisperer/core"
	"github.com/karma-234/sol-whisperer/core/internal/filter"
	"github.com/karma-234/sol-whisperer/core/internal/metadata"
	"github.com/karma-234/sol-whisperer/core/internal/types"
)

// SourceNameMap normalizes Helius source labels to internal names.
var SourceNameMap = map[string]string{
	"RAYDIUM":  "raydium_v4",
	"PUMP_AMM": "pump_fun",
	"JUPITER":  "jupiter",
	"ORCA":     "orca",
	"SOLEND":   "solend",
	"MARINADE": "marinade",
}

type DetectedProgram struct {
	Address string
	Name    string
}

// SwapInfo returns the core fields you asked for:
// - Swapper: wallet address that performed the swap
// - OutputMint: token CA they swapped to
// - OutputAmount: raw amount received
type SwapInfo struct {
	Signature        string
	Timestamp        int64
	Source           string
	Swapper          string
	OutputMint       string
	OutputAmount     string
	OutputDecimals   uint8
	InputMint        string
	InputAmount      string
	AmountInSOL      uint64 // populated when InputMint == "SOL" (in lamports)
	MarketCap        uint64 // market cap in USD for OutputMint
	DetectedPrograms []DetectedProgram
	Fee              uint64
	FeePayer         string
	Description      string
}

func NormalizeSourceLabel(source string) string {
	if normalized, ok := SourceNameMap[source]; ok {
		return normalized
	}
	return source
}

// Non-recursive DFS. Faster and avoids missing inner instructions when parent program IDs repeat.
func DetectProgramsFromInstructions(tx types.HeliusEnhancedWebhookTx) []DetectedProgram {
	if len(tx.Instructions) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(tx.Instructions)*2)
	result := make([]DetectedProgram, 0, 4)

	stack := make([]types.HeliusInstruction, 0, len(tx.Instructions))
	stack = append(stack, tx.Instructions...)

	for len(stack) > 0 {
		last := len(stack) - 1
		ix := stack[last]
		stack = stack[:last]

		// Always traverse inner instructions, even if this program ID was seen before.
		if len(ix.InnerInstructions) > 0 {
			stack = append(stack, ix.InnerInstructions...)
		}

		if _, exists := seen[ix.ProgramID]; exists {
			continue
		}
		seen[ix.ProgramID] = struct{}{}

		if name, ok := core.ProgramNames[ix.ProgramID]; ok {
			result = append(result, DetectedProgram{
				Address: ix.ProgramID,
				Name:    name,
			})
		}
	}

	return result
}

// Default keeps old behavior.
func ExtractSwapInfo(tx types.HeliusEnhancedWebhookTx) *SwapInfo {
	return ExtractSwapInfoWithOptions(tx, true, nil, nil)
}

// Set detectPrograms=false on hot paths for lower latency.
// filter: market cap filter (optional, nil disables filtering)
// fetcher: metadata fetcher for market cap (optional, required if filter is set)
func ExtractSwapInfoWithOptions(tx types.HeliusEnhancedWebhookTx, detectPrograms bool, capFilter *filter.MarketCapFilter, fetcher *metadata.Fetcher) *SwapInfo {
	if tx.Type != "SWAP" || tx.Events.Swap == nil {
		return nil
	}

	swap := tx.Events.Swap

	var detected []DetectedProgram
	if detectPrograms {
		detected = DetectProgramsFromInstructions(tx)
	}

	info := &SwapInfo{
		Signature:        tx.Signature,
		Timestamp:        tx.Timestamp,
		Source:           NormalizeSourceLabel(tx.Source),
		Swapper:          tx.FeePayer, // best default for "who swapped"
		DetectedPrograms: detected,
		Fee:              tx.Fee,
		FeePayer:         tx.FeePayer,
		Description:      tx.Description,
	}

	// Input side.
	if len(swap.TokenInputs) > 0 {
		in := pickInputForSwapper(swap.TokenInputs, info.Swapper)
		info.InputMint = in.Mint
		info.InputAmount = in.RawTokenAmount.TokenAmount
		if info.Swapper == "" && in.UserAccount != "" {
			info.Swapper = in.UserAccount
		}
	} else if swap.NativeInput != nil {
		info.InputMint = "SOL"
		info.InputAmount = swap.NativeInput.Amount
		if amt, err := parseUint64(swap.NativeInput.Amount); err == nil {
			info.AmountInSOL = amt
		}
		if info.Swapper == "" {
			info.Swapper = swap.NativeInput.Account
		}
	}

	// Output side: token CA + amount swapped to.
	if len(swap.TokenOutputs) > 0 {
		out := pickOutputForSwapper(swap.TokenOutputs, info.Swapper)
		info.OutputMint = out.Mint
		if core.StablecoinMints[info.OutputMint] {
			return nil
		}
		info.OutputAmount = out.RawTokenAmount.TokenAmount
		info.OutputDecimals = out.RawTokenAmount.Decimals
		if info.Swapper == "" && out.UserAccount != "" {
			info.Swapper = out.UserAccount
		}
	} else if swap.NativeOutput != nil {
		// When output is native SOL.
		info.OutputMint = "SOL"
		info.OutputAmount = swap.NativeOutput.Amount
		info.OutputDecimals = 9
		if amt, err := parseUint64(swap.NativeOutput.Amount); err == nil {
			info.AmountInSOL = amt
		}
		if info.Swapper == "" {
			info.Swapper = swap.NativeOutput.Account
		}
	}

	// Fetch market cap for output token (for filtering and logging)
	if fetcher != nil && info.OutputMint != "" {
		if meta := fetcher.FetchTokenMetadata(info.OutputMint); meta != nil {
			info.MarketCap = meta.MarketCap
		}
	}

	// Skip if input volume is too small (minimum 0.3 SOL when buying with SOL)
	const minInputSOL = 3e8 // 0.3 SOL in lamports
	if info.AmountInSOL > 0 && info.AmountInSOL < minInputSOL {
		return nil
	}

	// Skip if market cap exceeds threshold (filter for memes only)
	if capFilter != nil && !capFilter.IsAllowed(info.MarketCap) {
		return nil
	}

	return info
}

func pickOutputForSwapper(outputs []types.HeliusTokenAmountEvent, swapper string) types.HeliusTokenAmountEvent {
	if len(outputs) == 0 {
		return types.HeliusTokenAmountEvent{}
	}

	if swapper != "" {
		for _, o := range outputs {
			if o.UserAccount == swapper {
				return o
			}
		}
	}

	return outputs[0]
}

func pickInputForSwapper(inputs []types.HeliusTokenAmountEvent, swapper string) types.HeliusTokenAmountEvent {
	if len(inputs) == 0 {
		return types.HeliusTokenAmountEvent{}
	}

	if swapper != "" {
		for _, in := range inputs {
			if in.UserAccount == swapper {
				return in
			}
		}
	}

	return inputs[0]
}

func parseUint64(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}
