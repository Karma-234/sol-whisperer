package ws

// Solana JSON-RPC types for logsSubscribe notifications

// JSONRPCRequest is a JSON-RPC 2.0 request
type JSONRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

// JSONRPCResponse is a JSON-RPC 2.0 response
type JSONRPCResponse struct {
	JSONRPC string        `json:"jsonrpc"`
	Result  any           `json:"result,omitempty"`
	Error   *JSONRPCError `json:"error,omitempty"`
	ID      any           `json:"id"`
}

// JSONRPCError is a JSON-RPC error object
type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data,omitempty"`
}

// ProgramNotification is a logsSubscribe notification from Solana
type ProgramNotification struct {
	JSONRPC string                     `json:"jsonrpc"`
	Method  string                     `json:"method,omitempty"`
	Params  *ProgramNotificationParams `json:"params,omitempty"`
	Error   *JSONRPCError              `json:"error,omitempty"`
}

// ProgramNotificationParams wraps logsNotification params.
type ProgramNotificationParams struct {
	Result       *ProgramNotificationResult `json:"result,omitempty"`
	Subscription int                        `json:"subscription"`
}

// ProgramNotificationResult contains context and value for logsNotification.
type ProgramNotificationResult struct {
	Context *RpcContext               `json:"context,omitempty"`
	Value   *ProgramNotificationValue `json:"value,omitempty"`
}

// ProgramNotificationValue is the transaction data in a logsNotification update.
type ProgramNotificationValue struct {
	Signature   string           `json:"signature"`
	Logs        []string         `json:"logs,omitempty"`
	Err         interface{}      `json:"err,omitempty"`
	Transaction *TransactionData `json:"transaction,omitempty"`
}

// RpcContext provides the context of the RPC response
type RpcContext struct {
	Slot uint64 `json:"slot"`
}

// TransactionData is the parsed transaction in a notification
type TransactionData struct {
	Message    Message  `json:"message"`
	Signatures []string `json:"signatures"`
}

// Message is the message part of a transaction
type Message struct {
	AccountKeys     []string      `json:"accountKeys"`
	Header          MessageHeader `json:"header"`
	Instructions    []Instruction `json:"instructions"`
	RecentBlockhash string        `json:"recentBlockhash"`
}

// MessageHeader describes the read-only/signed structure
type MessageHeader struct {
	NumRequiredSignatures       int `json:"numRequiredSignatures"`
	NumReadonlySignedAccounts   int `json:"numReadonlySignedAccounts"`
	NumReadonlyUnsignedAccounts int `json:"numReadonlyUnsignedAccounts"`
}

// Instruction is a single instruction in a transaction
type Instruction struct {
	ProgramIDIndex int    `json:"programIdIndex"`
	Accounts       []int  `json:"accounts"`
	Data           string `json:"data"`
}

// GetTransactionConfig is the config for getTransaction RPC calls
type GetTransactionConfig struct {
	Encoding                       string `json:"encoding"`
	MaxSupportedTransactionVersion int    `json:"maxSupportedTransactionVersion"`
}

// TransactionResponse is the response from getTransaction
type TransactionResponse struct {
	Slot        uint64           `json:"slot"`
	Transaction *FullTransaction `json:"transaction,omitempty"`
	Meta        *TransactionMeta `json:"meta,omitempty"`
	BlockTime   *int64           `json:"blockTime,omitempty"`
}

// FullTransaction is the full parsed transaction from getTransaction
type FullTransaction struct {
	Signatures []string `json:"signatures"`
	Message    Message  `json:"message"`
}

// TransactionMeta is the metadata for a confirmed transaction
type TransactionMeta struct {
	Err               interface{}       `json:"err"`
	Fee               uint64            `json:"fee"`
	PreTokenBalances  []TokenBalance    `json:"preTokenBalances"`
	PostTokenBalances []TokenBalance    `json:"postTokenBalances"`
	InnerInstructions []interface{}     `json:"innerInstructions,omitempty"`
	PreBalances       []uint64          `json:"preBalances"`
	PostBalances      []uint64          `json:"postBalances"`
	LogMessages       []string          `json:"logMessages"`
	Status            TransactionStatus `json:"status"`
}

// TokenBalance represents a token account balance
type TokenBalance struct {
	AccountIndex  int           `json:"accountIndex"`
	Mint          string        `json:"mint"`
	Owner         string        `json:"owner"`
	UiTokenAmount UiTokenAmount `json:"uiTokenAmount"`
}

// UiTokenAmount is the UI-formatted token amount
type UiTokenAmount struct {
	Amount         string  `json:"amount"`
	Decimals       int     `json:"decimals"`
	UIAmount       float64 `json:"uiAmount"`
	UIAmountString string  `json:"uiAmountString"`
}

// TransactionStatus is the transaction status
type TransactionStatus struct {
	Ok  any `json:"Ok,omitempty"`
	Err any `json:"Err,omitempty"`
}
