// Package healthcheck typically enables readiness or liveness probes within kubernetes.
// IMPORTANT: If you update this behavior, be sure to update internal/fullnode/pod_builder.go with the new
// cosmos operator image in the "healthcheck" container.
package healthcheck

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"

	"github.com/altuslabsxyz/cosmos-operator/internal/cosmos"
)

// Stature can query the Comet status endpoint.
type Stature interface {
	Status(ctx context.Context, rpcHost string) (cosmos.CometStatus, error)
}

type healthResponse struct {
	Address  string `json:"address"`
	InSync   bool   `json:"in_sync"`
	Error    string `json:"error,omitempty"`
	BlockAge string `json:"block_age,omitempty"`
}

// Comet checks the CometBFT status endpoint to determine if the node is in-sync or not.
type Comet struct {
	client      Stature
	lastStatus  int32
	logger      logr.Logger
	rpcHost     string
	timeout     time.Duration
	maxBlockAge time.Duration
}

func NewComet(logger logr.Logger, client Stature, rpcHost string, timeout time.Duration, maxBlockAge time.Duration) *Comet {
	return &Comet{
		client:      client,
		logger:      logger,
		rpcHost:     rpcHost,
		timeout:     timeout,
		maxBlockAge: maxBlockAge,
	}
}

// ServeHTTP implements http.Handler.
func (h *Comet) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var resp healthResponse
	resp.Address = h.rpcHost

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	status, err := h.client.Status(ctx, h.rpcHost)
	if err != nil {
		resp.Error = err.Error()
		h.writeResponse(http.StatusServiceUnavailable, w, resp)
		return
	}

	resp.InSync = !status.Result.SyncInfo.CatchingUp
	if !resp.InSync {
		h.writeResponse(http.StatusUnprocessableEntity, w, resp)
		return
	}

	// Check block age if maxBlockAge is configured
	if h.maxBlockAge > 0 {
		blockAge := time.Since(status.Result.SyncInfo.LatestBlockTime)
		resp.BlockAge = blockAge.Truncate(time.Second).String()

		if blockAge > h.maxBlockAge {
			resp.InSync = false
			resp.Error = fmt.Sprintf("block age %s exceeds max %s", blockAge.Truncate(time.Second), h.maxBlockAge)
			h.writeResponse(http.StatusServiceUnavailable, w, resp)
			return
		}
	}

	h.writeResponse(http.StatusOK, w, resp)
}

func (h *Comet) writeResponse(code int, w http.ResponseWriter, resp healthResponse) {
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	mustJSONEncode(resp, w)
	// Only log when status code changes, so we don't spam logs.
	if atomic.SwapInt32(&h.lastStatus, int32(code)) != int32(code) {
		h.logger.Info("Health state change", "statusCode", code, "response", resp)
	}
}
