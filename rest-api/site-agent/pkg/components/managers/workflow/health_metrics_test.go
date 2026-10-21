// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package workflow

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	computils "github.com/NVIDIA/infra-controller/rest-api/site-agent/pkg/components/utils"
	"github.com/NVIDIA/infra-controller/rest-api/site-agent/pkg/conftypes"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPI_Init(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		statuses  []computils.CompStatus
	}{
		{
			name:      "default namespace follows health transitions",
			namespace: conftypes.DefaultMetricsNamespace,
			statuses:  []computils.CompStatus{computils.CompHealthy, computils.CompUnhealthy, computils.CompHealthy},
		},
		{
			name:      "configured namespace preserves unknown status",
			namespace: "custom_site_agent",
			statuses:  []computils.CompStatus{computils.CompNotKnown},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			previousRegisterer := prometheus.DefaultRegisterer
			previousGauge := CertExpirationMetric
			t.Cleanup(func() {
				prometheus.DefaultRegisterer = previousRegisterer
				CertExpirationMetric = previousGauge
			})
			registry := prometheus.NewRegistry()
			prometheus.DefaultRegisterer = registry
			data := newHealthTestManager(t, &conftypes.Config{MetricsNamespace: tt.namespace})
			state := data.Managers.Workflow.State
			state.HealthStatus.Store(uint64(computils.CompHealthy))
			(&API{}).Init()
			require.Equal(t, uint64(computils.CompUnhealthy), state.HealthStatus.Load())
			handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
			checkHealthMetrics := func(t *testing.T, status computils.CompStatus) string {
				t.Helper()
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
				require.Equal(t, http.StatusOK, response.Code)
				body := response.Body.String()
				for _, metric := range []struct {
					name string
					kind string
					help string
				}{
					{
						name: "temporal_connection_health", kind: "gauge",
						help: "Current Temporal connection health of the Site Agent: 0 unhealthy, 1 healthy, 2 not known",
					},
					{
						name: MetricTemporalConnStatus, kind: "counter",
						help: "Temporal health status of the Site Agent",
					},
				} {
					name := tt.namespace + "_" + metric.name
					assert.Contains(t, body, fmt.Sprintf("# HELP %s %s\n", name, metric.help))
					assert.Contains(t, body, fmt.Sprintf("# TYPE %s %s\n", name, metric.kind))
					assert.Contains(t, body, fmt.Sprintf("\n%s %d\n", name, status))
				}
				return body
			}
			initialMetrics := checkHealthMetrics(t, computils.CompUnhealthy)
			for _, name := range []string{
				MetricTemporalConnAttempted,
				MetricTemporalConnSucc,
				"temporal_cert_expiration",
			} {
				assert.Contains(t, initialMetrics, "# TYPE "+tt.namespace+"_"+name+" ")
			}

			for _, status := range tt.statuses {
				t.Run(status.String(), func(t *testing.T) {
					state.HealthStatus.Store(uint64(status))
					checkHealthMetrics(t, status)
				})
			}
		})
	}
}
