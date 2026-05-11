package filter

// MarketCapFilter filters tokens by market cap in USD.
type MarketCapFilter struct {
	maxCapUSD uint64
}

// NewMarketCapFilter creates a new market cap filter with the given threshold in USD.
func NewMarketCapFilter(maxCapUSD uint64) *MarketCapFilter {
	if maxCapUSD == 0 {
		maxCapUSD = 200_000 // default: $200K
	}
	return &MarketCapFilter{
		maxCapUSD: maxCapUSD,
	}
}

// IsAllowed returns true if the token's market cap is below the threshold.
func (f *MarketCapFilter) IsAllowed(marketCapUSD uint64) bool {
	if f == nil {
		return true // if no filter, allow all
	}
	return marketCapUSD < f.maxCapUSD
}

// MaxCap returns the current threshold in USD.
func (f *MarketCapFilter) MaxCap() uint64 {
	if f == nil {
		return 0
	}
	return f.maxCapUSD
}
