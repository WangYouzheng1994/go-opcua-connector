package collector

import (
	"go-opcua-connector/internal/model"

	"go.uber.org/zap"
)

func summarizePointHealth(snapshots []PointSnapshot) model.CollectorStats {
	stats := model.CollectorStats{CurrentPoints: int64(len(snapshots))}
	for _, snapshot := range snapshots {
		switch snapshot.Health {
		case PointHealthy:
			stats.HealthyPoints++
		case PointInitializing:
			stats.InitializingPoints++
		case PointSampleInvalid:
			stats.SampleInvalidPoints++
		case PointSubscribeFailed:
			stats.SubscribeFailedPoints++
		case PointVerificationFailed:
			stats.VerificationFailedPoints++
		case PointSubscriptionMismatch:
			stats.SubscriptionMismatchPoints++
		case PointSourceDisconnected:
			stats.SourceDisconnectedPoints++
		case PointStale:
			stats.StaleCount++
		}
	}
	stats.UnhealthyPoints = stats.CurrentPoints - stats.HealthyPoints
	return stats
}

func pointHealthLogFields(stats model.CollectorStats) []zap.Field {
	return []zap.Field{
		zap.Int64("current_points", stats.CurrentPoints),
		zap.Int64("healthy_points", stats.HealthyPoints),
		zap.Int64("unhealthy_points", stats.UnhealthyPoints),
		zap.Int64("initializing_points", stats.InitializingPoints),
		zap.Int64("sample_invalid_points", stats.SampleInvalidPoints),
		zap.Int64("subscribe_failed_points", stats.SubscribeFailedPoints),
		zap.Int64("verification_failed_points", stats.VerificationFailedPoints),
		zap.Int64("subscription_mismatch_points", stats.SubscriptionMismatchPoints),
		zap.Int64("source_disconnected_points", stats.SourceDisconnectedPoints),
		zap.Int64("stale_points", stats.StaleCount),
	}
}
