package stuckheight

import (
	"testing"
	"time"

	cosmosv1 "github.com/b-harvest/cosmos-operator/api/v1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRateLimiter_CanAttemptRecovery(t *testing.T) {
	t.Run("first attempt - allowed", func(t *testing.T) {
		limiter := NewRateLimiter()
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				MaxRetries:      ptr(int32(3)),
				RateLimitWindow: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				RateLimitWindowStart: nil,
				RecoveryAttempts:     0,
			},
		}

		allowed, msg, err := limiter.CanAttemptRecovery(recovery)

		require.NoError(t, err)
		require.True(t, allowed)
		require.Empty(t, msg)
	})

	t.Run("within limit - allowed", func(t *testing.T) {
		limiter := NewRateLimiter()
		windowStart := metav1.NewTime(time.Now().Add(-2 * time.Minute))
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				MaxRetries:      ptr(int32(3)),
				RateLimitWindow: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				RateLimitWindowStart: &windowStart,
				RecoveryAttempts:     2,
			},
		}

		allowed, msg, err := limiter.CanAttemptRecovery(recovery)

		require.NoError(t, err)
		require.True(t, allowed)
		require.Empty(t, msg)
	})

	t.Run("at limit - not allowed", func(t *testing.T) {
		limiter := NewRateLimiter()
		windowStart := metav1.NewTime(time.Now().Add(-2 * time.Minute))
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				MaxRetries:      ptr(int32(3)),
				RateLimitWindow: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				RateLimitWindowStart: &windowStart,
				RecoveryAttempts:     3,
			},
		}

		allowed, msg, err := limiter.CanAttemptRecovery(recovery)

		require.NoError(t, err)
		require.False(t, allowed)
		require.Contains(t, msg, "Rate limit exceeded")
		require.Contains(t, msg, "3 attempts")
	})

	t.Run("window expired - allowed", func(t *testing.T) {
		limiter := NewRateLimiter()
		windowStart := metav1.NewTime(time.Now().Add(-10 * time.Minute))
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				MaxRetries:      ptr(int32(3)),
				RateLimitWindow: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				RateLimitWindowStart: &windowStart,
				RecoveryAttempts:     3,
			},
		}

		allowed, msg, err := limiter.CanAttemptRecovery(recovery)

		require.NoError(t, err)
		require.True(t, allowed)
		require.Empty(t, msg)
	})

	t.Run("default values", func(t *testing.T) {
		limiter := NewRateLimiter()
		windowStart := metav1.NewTime(time.Now().Add(-2 * time.Minute))
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				RateLimitWindowStart: &windowStart,
				RecoveryAttempts:     3,
			},
		}

		allowed, msg, err := limiter.CanAttemptRecovery(recovery)

		require.NoError(t, err)
		require.False(t, allowed)
		require.Contains(t, msg, "3 attempts")
	})

	t.Run("invalid rate limit window", func(t *testing.T) {
		limiter := NewRateLimiter()
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				RateLimitWindow: "invalid",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				RateLimitWindowStart: &metav1.Time{Time: time.Now()},
			},
		}

		_, _, err := limiter.CanAttemptRecovery(recovery)

		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid rate limit window")
	})

	t.Run("custom max retries", func(t *testing.T) {
		limiter := NewRateLimiter()
		windowStart := metav1.NewTime(time.Now().Add(-2 * time.Minute))
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				MaxRetries:      ptr(int32(5)),
				RateLimitWindow: "10m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				RateLimitWindowStart: &windowStart,
				RecoveryAttempts:     4,
			},
		}

		allowed, msg, err := limiter.CanAttemptRecovery(recovery)

		require.NoError(t, err)
		require.True(t, allowed)
		require.Empty(t, msg)
	})
}

func TestRateLimiter_RecordAttempt(t *testing.T) {
	t.Run("first attempt", func(t *testing.T) {
		limiter := NewRateLimiter()
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				RateLimitWindow: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				RateLimitWindowStart: nil,
				RecoveryAttempts:     0,
			},
		}

		before := time.Now()
		limiter.RecordAttempt(recovery)
		after := time.Now()

		require.NotNil(t, recovery.Status.RateLimitWindowStart)
		require.True(t, recovery.Status.RateLimitWindowStart.Time.After(before) ||
			recovery.Status.RateLimitWindowStart.Time.Equal(before))
		require.True(t, recovery.Status.RateLimitWindowStart.Time.Before(after) ||
			recovery.Status.RateLimitWindowStart.Time.Equal(after))
		require.Equal(t, int32(1), recovery.Status.RecoveryAttempts)

		// LastRecoveryTime should also be set
		require.NotNil(t, recovery.Status.LastRecoveryTime)
		require.True(t, recovery.Status.LastRecoveryTime.Time.After(before) ||
			recovery.Status.LastRecoveryTime.Time.Equal(before))
		require.True(t, recovery.Status.LastRecoveryTime.Time.Before(after) ||
			recovery.Status.LastRecoveryTime.Time.Equal(after))
	})

	t.Run("increment within window", func(t *testing.T) {
		limiter := NewRateLimiter()
		windowStart := metav1.NewTime(time.Now().Add(-2 * time.Minute))
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				RateLimitWindow: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				RateLimitWindowStart: &windowStart,
				RecoveryAttempts:     1,
			},
		}

		limiter.RecordAttempt(recovery)

		require.Equal(t, windowStart.Time, recovery.Status.RateLimitWindowStart.Time)
		require.Equal(t, int32(2), recovery.Status.RecoveryAttempts)
	})

	t.Run("reset window after expiration", func(t *testing.T) {
		limiter := NewRateLimiter()
		windowStart := metav1.NewTime(time.Now().Add(-10 * time.Minute))
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				RateLimitWindow: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				RateLimitWindowStart: &windowStart,
				RecoveryAttempts:     5,
			},
		}

		before := time.Now()
		limiter.RecordAttempt(recovery)
		after := time.Now()

		require.NotEqual(t, windowStart.Time, recovery.Status.RateLimitWindowStart.Time)
		require.True(t, recovery.Status.RateLimitWindowStart.Time.After(before) ||
			recovery.Status.RateLimitWindowStart.Time.Equal(before))
		require.True(t, recovery.Status.RateLimitWindowStart.Time.Before(after) ||
			recovery.Status.RateLimitWindowStart.Time.Equal(after))
		require.Equal(t, int32(1), recovery.Status.RecoveryAttempts)
	})

	t.Run("default rate limit window", func(t *testing.T) {
		limiter := NewRateLimiter()
		windowStart := metav1.NewTime(time.Now().Add(-2 * time.Minute))
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				RateLimitWindowStart: &windowStart,
				RecoveryAttempts:     1,
			},
		}

		limiter.RecordAttempt(recovery)

		require.Equal(t, int32(2), recovery.Status.RecoveryAttempts)
	})
}

func TestRateLimiter_ResetWindow(t *testing.T) {
	t.Run("reset window", func(t *testing.T) {
		limiter := NewRateLimiter()
		windowStart := metav1.NewTime(time.Now().Add(-2 * time.Minute))
		recovery := &cosmosv1.StuckHeightRecovery{
			Status: cosmosv1.StuckHeightRecoveryStatus{
				RateLimitWindowStart: &windowStart,
				RecoveryAttempts:     3,
			},
		}

		limiter.ResetWindow(recovery)

		require.Nil(t, recovery.Status.RateLimitWindowStart)
		require.Equal(t, int32(0), recovery.Status.RecoveryAttempts)
	})

	t.Run("reset already empty", func(t *testing.T) {
		limiter := NewRateLimiter()
		recovery := &cosmosv1.StuckHeightRecovery{
			Status: cosmosv1.StuckHeightRecoveryStatus{
				RateLimitWindowStart: nil,
				RecoveryAttempts:     0,
			},
		}

		limiter.ResetWindow(recovery)

		require.Nil(t, recovery.Status.RateLimitWindowStart)
		require.Equal(t, int32(0), recovery.Status.RecoveryAttempts)
	})
}
