package types

// Enhanced webhook payload is an array of transactions.
type HeliusEnhancedWebhookPayload []HeliusEnhancedWebhookTx

type HeliusEnhancedWebhookTx struct {
	AccountData     []HeliusAccountData    `json:"accountData"`
	Description     string                 `json:"description"`
	Events          HeliusEnhancedEvents   `json:"events"`
	Fee             uint64                 `json:"fee"`
	FeePayer        string                 `json:"feePayer"`
	Signature       string                 `json:"signature"`
	Slot            uint64                 `json:"slot"`
	Source          string                 `json:"source"`
	Timestamp       int64                  `json:"timestamp"`
	TokenTransfers  []HeliusTokenTransfer  `json:"tokenTransfers"`
	NativeTransfers []HeliusNativeTransfer `json:"nativeTransfers"`
	Instructions    []HeliusInstruction    `json:"instructions"`
	Type            string                 `json:"type"` // "SWAP" for swap events
}

type HeliusEnhancedEvents struct {
	Swap *HeliusSwapEvent `json:"swap,omitempty"`
	// Add other known event families as needed (nft, compressed, etc).
}

type HeliusSwapEvent struct {
	NativeInput  *HeliusNativeAmount      `json:"nativeInput,omitempty"`
	NativeOutput *HeliusNativeAmount      `json:"nativeOutput,omitempty"`
	TokenInputs  []HeliusTokenAmountEvent `json:"tokenInputs"`
	TokenOutputs []HeliusTokenAmountEvent `json:"tokenOutputs"`
	NativeFees   []HeliusNativeAmount     `json:"nativeFees"`
	TokenFees    []HeliusTokenAmountEvent `json:"tokenFees"`
	InnerSwaps   []HeliusInnerSwap        `json:"innerSwaps"`
}

type HeliusNativeAmount struct {
	Account string `json:"account"`
	Amount  string `json:"amount"` // docs/examples commonly emit numeric strings
}

type HeliusTokenAmountEvent struct {
	UserAccount    string               `json:"userAccount"`
	TokenAccount   string               `json:"tokenAccount"`
	Mint           string               `json:"mint"`
	RawTokenAmount HeliusRawTokenAmount `json:"rawTokenAmount"`
}

type HeliusRawTokenAmount struct {
	TokenAmount string `json:"tokenAmount"`
	Decimals    uint8  `json:"decimals"`
}

type HeliusInnerSwap struct {
	TokenInputs  []HeliusTokenAmountEvent `json:"tokenInputs"`
	TokenOutputs []HeliusTokenAmountEvent `json:"tokenOutputs"`
	TokenFees    []HeliusTokenAmountEvent `json:"tokenFees"`
	NativeFees   []HeliusNativeAmount     `json:"nativeFees"`
	ProgramInfo  map[string]any           `json:"programInfo,omitempty"`
}

type HeliusNativeTransfer struct {
	FromUserAccount string `json:"fromUserAccount"`
	ToUserAccount   string `json:"toUserAccount"`
	Amount          uint64 `json:"amount"`
}

type HeliusTokenTransfer struct {
	FromUserAccount  string  `json:"fromUserAccount"`
	ToUserAccount    string  `json:"toUserAccount"`
	FromTokenAccount string  `json:"fromTokenAccount,omitempty"`
	ToTokenAccount   string  `json:"toTokenAccount,omitempty"`
	TokenAmount      float64 `json:"tokenAmount"` // Changed from uint64
	Mint             string  `json:"mint"`
	TokenStandard    string  `json:"tokenStandard,omitempty"`
}

type HeliusAccountData struct {
	Account             string                     `json:"account"`
	NativeBalanceChange int64                      `json:"nativeBalanceChange"`
	TokenBalanceChanges []HeliusTokenBalanceChange `json:"tokenBalanceChanges"`
}

type HeliusTokenBalanceChange struct {
	UserAccount    string               `json:"userAccount"`
	TokenAccount   string               `json:"tokenAccount"`
	Mint           string               `json:"mint"`
	RawTokenAmount HeliusRawTokenAmount `json:"rawTokenAmount"`
}

type HeliusInstruction struct {
	Accounts          []string            `json:"accounts"`
	Data              string              `json:"data"`
	ProgramID         string              `json:"programId"`
	InnerInstructions []HeliusInstruction `json:"innerInstructions"`
}

// Webhooks API error envelope (management endpoints).
type HeliusWebhookAPIErrorResponse struct {
	JSONRPC string                `json:"jsonrpc"`
	Error   HeliusWebhookAPIError `json:"error"`
	ID      any                   `json:"id"`
}

type HeliusWebhookAPIError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Enhanced Transactions API error envelope.
type HeliusEnhancedTxAPIErrorResponse struct {
	Error string `json:"error"`
}
