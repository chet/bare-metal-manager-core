// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/NVIDIA/infra-controller/rest-api/site-agent/pkg/components/managers/managerapi"
	"github.com/NVIDIA/infra-controller/rest-api/site-agent/pkg/conftypes"
	"github.com/NVIDIA/infra-controller/rest-api/site-agent/pkg/datatypes/elektratypes"
	"github.com/NVIDIA/infra-controller/rest-api/site-agent/pkg/datatypes/managertypes"
	bootstraptypes "github.com/NVIDIA/infra-controller/rest-api/site-agent/pkg/datatypes/managertypes/bootstrap"
)

func TestBoostrapAPI_Init(t *testing.T) {
	require.Equal(t, "nico_rest_site_agent", conftypes.DefaultMetricsNamespace)

	tests := []struct {
		name      string
		namespace string
		master    bool
		disabled  bool
	}{
		{name: "counter aliases use default namespace", namespace: conftypes.DefaultMetricsNamespace, master: true},
		{name: "counter aliases honor namespace override", namespace: "acme_site_agent", master: true},
		{name: "non-master omits bootstrap counters", namespace: conftypes.DefaultMetricsNamespace},
		{name: "disabled bootstrap omits counters", namespace: conftypes.DefaultMetricsNamespace, master: true, disabled: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			previousAccess := ManagerAccess
			previousRegisterer := prometheus.DefaultRegisterer
			t.Cleanup(func() {
				ManagerAccess = previousAccess
				prometheus.DefaultRegisterer = previousRegisterer
			})

			registry := prometheus.NewRegistry()
			prometheus.DefaultRegisterer = registry
			conf := &conftypes.Config{
				MetricsNamespace: tt.namespace,
				IsMasterPod:      tt.master,
				DisableBootstrap: tt.disabled,
				BootstrapSecret:  t.TempDir(),
			}
			if tt.master && !tt.disabled {
				for name, contents := range map[string]string{
					bootstraptypes.TagUUID:     "test-site",
					bootstraptypes.TagOTP:      testOTP,
					bootstraptypes.TagCredsURL: "https://credentials.example.test",
					bootstraptypes.TagCACert:   "test-ca",
				} {
					err := os.WriteFile(filepath.Join(conf.BootstrapSecret, name), []byte(contents), 0600)
					require.NoError(t, err)
				}
			}
			data := &elektratypes.Elektra{Log: zerolog.Nop(), Managers: managertypes.NewManagerType()}
			NewBootstrapManager(data, nil, &managerapi.ManagerConf{EB: conf}).Init()
			for _, observation := range []struct {
				attempted uint64
				succeeded uint64
			}{
				{attempted: 0, succeeded: 0},
				{attempted: 7, succeeded: 5},
			} {
				data.Managers.Bootstrap.State.DownloadAttempted.Store(observation.attempted)
				data.Managers.Bootstrap.State.DownloadSucceeded.Store(observation.succeeded)
				metrics, err := registry.Gather()
				require.NoError(t, err)
				byName := make(map[string]*dto.MetricFamily, len(metrics))
				for _, metric := range metrics {
					byName[metric.GetName()] = metric
				}
				for name, want := range map[string]uint64{
					"credentials_download_attempted":       observation.attempted,
					"credentials_download_attempted_total": observation.attempted,
					"credentials_download_succeeded":       observation.succeeded,
					"credentials_download_succeeded_total": observation.succeeded,
				} {
					fullName := tt.namespace + "_" + name
					metric := byName[fullName]
					if tt.namespace != conftypes.DefaultMetricsNamespace {
						assert.NotContains(t, byName, "nico_rest_site_agent_"+name)
					}
					if !tt.master || tt.disabled {
						assert.Nil(t, metric, fullName)
						continue
					}
					require.NotNil(t, metric, fullName)
					assert.Equal(t, dto.MetricType_COUNTER, metric.GetType(), fullName)
					require.Len(t, metric.GetMetric(), 1, fullName)
					assert.Empty(t, metric.GetMetric()[0].GetLabel(), fullName)
					assert.Equal(t, float64(want), metric.GetMetric()[0].GetCounter().GetValue(), fullName)
				}
			}
		})
	}
}
