// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package workflow

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"github.com/NVIDIA/infra-controller/rest-api/site-agent/pkg/components/managers/managerapi"
	"github.com/NVIDIA/infra-controller/rest-api/site-agent/pkg/conftypes"
	"github.com/NVIDIA/infra-controller/rest-api/site-agent/pkg/datatypes/elektratypes"
	"github.com/NVIDIA/infra-controller/rest-api/site-agent/pkg/datatypes/managertypes"
)

func TestWorkflowOrchestrator(t *testing.T) {
	caPublicKey, caKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:              time.Date(2035, 1, 1, 0, 0, 0, 0, time.UTC),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, caPublicKey, caKey)
	require.NoError(t, err)
	publicKey, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	expiration := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		master       bool
		reload       bool
		failedReload bool
		omitLeaf     bool
		invalidKey   bool
		nilGauge     bool
		namespace    string
	}{
		{name: "master loads existing client certificate", master: true},
		{name: "non-master loads existing client certificate"},
		{name: "reload updates client expiration", reload: true},
		{name: "load and reload without cached leaf", omitLeaf: true, reload: true},
		{name: "failed reload retains previous expiration", failedReload: true},
		{name: "invalid key leaves metric unchanged", invalidKey: true},
		{name: "missing gauge does not interrupt certificate loading", nilGauge: true},
		{name: "certificate aliases honor namespace override", namespace: "acme_site_agent"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.omitLeaf {
				t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",x509keypairleaf=0")
			}
			previousAccess := ManagerAccess
			previousGauge := CertExpirationMetric
			previousSecondsGauge := certExpirationSecondsMetric
			previousRegisterer := prometheus.DefaultRegisterer
			t.Cleanup(func() {
				ManagerAccess = previousAccess
				CertExpirationMetric = previousGauge
				certExpirationSecondsMetric = previousSecondsGauge
				prometheus.DefaultRegisterer = previousRegisterer
			})

			registry := prometheus.NewRegistry()
			prometheus.DefaultRegisterer = registry
			conf := &conftypes.Config{
				EnableTLS:        true,
				DisableBootstrap: true,
				IsMasterPod:      tt.master,
				MetricsNamespace: conftypes.DefaultMetricsNamespace,
				Temporal: conftypes.TemporalConfig{
					TemporalCertPath: t.TempDir(),
				},
			}
			if tt.namespace != "" {
				conf.MetricsNamespace = tt.namespace
			}
			data := &elektratypes.Elektra{Log: zerolog.Nop(), Managers: managertypes.NewManagerType()}
			managerConf := &managerapi.ManagerConf{EB: conf}
			NewWorkflowManager(data, nil, managerConf).Init()
			expirationGauges := []prometheus.Gauge{CertExpirationMetric, certExpirationSecondsMetric}
			assertExpirationValues := func(want float64) {
				t.Helper()
				for _, gauge := range expirationGauges {
					metric := &dto.Metric{}
					writeErr := gauge.Write(metric)
					require.NoError(t, writeErr)
					assert.Equal(t, want, metric.GetGauge().GetValue())
				}
			}
			assertExpirationValues(0)
			if tt.nilGauge {
				CertExpirationMetric = nil
			}

			writeCertificate := func(notAfter time.Time) {
				t.Helper()
				cert := &x509.Certificate{
					SerialNumber: big.NewInt(notAfter.Unix()),
					NotBefore:    ca.NotBefore,
					NotAfter:     notAfter,
					KeyUsage:     x509.KeyUsageDigitalSignature,
					ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
				}
				certDER, certErr := x509.CreateCertificate(rand.Reader, cert, ca, publicKey, caKey)
				require.NoError(t, certErr)
				chain := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
				chain = append(chain, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})...)
				certErr = os.WriteFile(conf.Temporal.GetTemporalClientCertFullPath(), chain, 0600)
				require.NoError(t, certErr)
			}
			err = os.MkdirAll(filepath.Dir(conf.Temporal.GetTemporalClientCertFullPath()), 0700)
			require.NoError(t, err)
			writeCertificate(expiration)
			err = os.WriteFile(conf.Temporal.GetTemporalClientKeyFullPath(), keyPEM, 0600)
			require.NoError(t, err)
			if tt.invalidKey {
				err = os.WriteFile(conf.Temporal.GetTemporalClientKeyFullPath(), []byte("invalid PEM"), 0600)
				require.NoError(t, err)
			}

			// Omit the CA file to stop after loading the client certificate, before
			// dialing Temporal. The chain has different leaf and CA expirations.
			_, loadErr := workflowOrchestrator()
			if tt.invalidKey {
				require.ErrorContains(t, loadErr, "PEM data in key input")
				assertExpirationValues(0)
				return
			}
			var pathErr *os.PathError
			require.ErrorAs(t, loadErr, &pathErr)
			assert.Equal(t, filepath.Join(conf.Temporal.TemporalCertPath, "ca", "ca.crt"), pathErr.Path)
			if tt.nilGauge {
				assertExpirationValues(0)
				return
			}
			metrics, gatherErr := registry.Gather()
			require.NoError(t, gatherErr)
			var expirationMetric *dto.MetricFamily
			var expirationSecondsMetric *dto.MetricFamily
			for _, metric := range metrics {
				switch metric.GetName() {
				case conf.MetricsNamespace + "_temporal_cert_expiration":
					expirationMetric = metric
				case conf.MetricsNamespace + "_temporal_cert_expiration_seconds":
					expirationSecondsMetric = metric
				}
				if tt.namespace != "" {
					assert.NotEqual(t, "nico_rest_site_agent_temporal_cert_expiration", metric.GetName())
					assert.NotEqual(t, "nico_rest_site_agent_temporal_cert_expiration_seconds", metric.GetName())
				}
			}
			for _, metric := range []*dto.MetricFamily{expirationMetric, expirationSecondsMetric} {
				require.NotNil(t, metric)
				assert.Equal(t, dto.MetricType_GAUGE, metric.GetType())
				require.Len(t, metric.GetMetric(), 1)
				assert.Empty(t, metric.GetMetric()[0].GetLabel())
				assert.Equal(t, float64(expiration.Unix()), metric.GetMetric()[0].GetGauge().GetValue())
			}
			if tt.failedReload {
				err = os.WriteFile(conf.Temporal.GetTemporalClientKeyFullPath(), []byte("invalid PEM"), 0600)
				require.NoError(t, err)
				_, loadErr = workflowOrchestrator()
				require.ErrorContains(t, loadErr, "PEM data in key input")
				assertExpirationValues(float64(expiration.Unix()))
			}
			if tt.reload {
				newExpiration := expiration.Add(24 * time.Hour)
				writeCertificate(newExpiration)
				_, loadErr = workflowOrchestrator()
				require.ErrorAs(t, loadErr, &pathErr)
				assertExpirationValues(float64(newExpiration.Unix()))
			}
		})
	}

	connections := []struct {
		name          string
		network       string
		host          string
		listenAddress string
	}{
		{name: "Temporal hostname", network: "tcp4", host: "localhost", listenAddress: "127.0.0.1:0"},
		{name: "Temporal IPv4", network: "tcp4", host: "127.0.0.1", listenAddress: "127.0.0.1:0"},
		{name: "Temporal IPv6", network: "tcp6", host: "::1", listenAddress: "[::1]:0"},
		{name: "Temporal bracketed IPv6", network: "tcp6", host: "[::1]", listenAddress: "[::1]:0"},
	}
	for _, tt := range connections {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := net.Listen(tt.network, tt.listenAddress)
			if tt.network == "tcp6" && (errors.Is(err, syscall.EAFNOSUPPORT) || errors.Is(err, syscall.EADDRNOTAVAIL)) {
				t.Skipf("IPv6 loopback is unavailable: %v", err)
			}
			require.NoError(t, err)
			_, port, err := net.SplitHostPort(listener.Addr().String())
			require.NoError(t, err)
			service := &temporalConnectionServer{}
			server := grpc.NewServer()
			workflowservice.RegisterWorkflowServiceServer(server, service)
			serveErr := make(chan error, 1)
			go func() { serveErr <- server.Serve(listener) }()
			previousAccess := ManagerAccess
			t.Cleanup(func() {
				server.Stop()
				serverErr := <-serveErr
				assert.True(t, serverErr == nil || errors.Is(serverErr, grpc.ErrServerStopped), "%v", serverErr)
				ManagerAccess = previousAccess
			})

			conf := &conftypes.Config{
				Temporal: conftypes.TemporalConfig{
					Host:                       tt.host,
					Port:                       port,
					TemporalPublishNamespace:   "publish",
					TemporalSubscribeNamespace: "subscribe",
					TemporalSubscribeQueue:     "test",
				},
			}
			data := &elektratypes.Elektra{Conf: conf, Managers: managertypes.NewManagerType(), Log: zerolog.Nop()}
			t.Cleanup(func() {
				if data.Managers.Workflow.Temporal.Publisher != nil {
					data.Managers.Workflow.Temporal.Publisher.Close()
				}
				if data.Managers.Workflow.Temporal.Subscriber != nil {
					data.Managers.Workflow.Temporal.Subscriber.Close()
				}
			})

			// Stop after both clients connect, before registering workflows or
			// starting the worker.
			registrationErr := errors.New("stop after connecting Temporal clients")
			api := &managerapi.ManagerAPI{Site: siteRegistrationFailure{err: registrationErr}}
			NewWorkflowManager(data, api, &managerapi.ManagerConf{EB: conf})
			_, err = workflowOrchestrator()
			require.ErrorIs(t, err, registrationErr)
			assert.EqualValues(t, 2, service.calls.Load())
			assert.NotNil(t, data.Managers.Workflow.Temporal.Publisher)
			assert.NotNil(t, data.Managers.Workflow.Temporal.Subscriber)
		})
	}
}

func TestAPI_Init(t *testing.T) {
	for _, namespace := range []string{conftypes.DefaultMetricsNamespace, "acme_site_agent"} {
		t.Run("connection counter aliases with "+namespace, func(t *testing.T) {
			previousAccess := ManagerAccess
			previousGauge := CertExpirationMetric
			previousSecondsGauge := certExpirationSecondsMetric
			previousRegisterer := prometheus.DefaultRegisterer
			t.Cleanup(func() {
				ManagerAccess = previousAccess
				CertExpirationMetric = previousGauge
				certExpirationSecondsMetric = previousSecondsGauge
				prometheus.DefaultRegisterer = previousRegisterer
			})

			registry := prometheus.NewRegistry()
			prometheus.DefaultRegisterer = registry
			conf := &conftypes.Config{MetricsNamespace: namespace}
			data := &elektratypes.Elektra{Log: zerolog.Nop(), Managers: managertypes.NewManagerType()}
			NewWorkflowManager(data, nil, &managerapi.ManagerConf{EB: conf}).Init()
			for _, observation := range []struct {
				attempted uint64
				succeeded uint64
			}{
				{attempted: 0, succeeded: 0},
				{attempted: 3, succeeded: 2},
			} {
				data.Managers.Workflow.State.ConnectionAttempted.Store(observation.attempted)
				data.Managers.Workflow.State.ConnectionSucc.Store(observation.succeeded)
				metrics, gatherErr := registry.Gather()
				require.NoError(t, gatherErr)
				byName := make(map[string]*dto.MetricFamily, len(metrics))
				for _, metric := range metrics {
					byName[metric.GetName()] = metric
				}
				for _, name := range []string{
					"nico_rest_site_agent_temporal_cert_expiration",
					"nico_rest_site_agent_temporal_cert_expiration_seconds",
					"nico_rest_site_agent_temporal_connection_attempted",
					"nico_rest_site_agent_temporal_connection_attempted_total",
					"nico_rest_site_agent_temporal_connection_succeeded",
					"nico_rest_site_agent_temporal_connection_succeeded_total",
					"nico_rest_site_agent_temporal_connection_status",
				} {
					if namespace == conftypes.DefaultMetricsNamespace {
						assert.Contains(t, byName, name)
					} else {
						assert.NotContains(t, byName, name)
					}
				}
				for name, want := range map[string]uint64{
					"temporal_connection_attempted":       observation.attempted,
					"temporal_connection_attempted_total": observation.attempted,
					"temporal_connection_succeeded":       observation.succeeded,
					"temporal_connection_succeeded_total": observation.succeeded,
				} {
					fullName := namespace + "_" + name
					metric := byName[fullName]
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

type temporalConnectionServer struct {
	workflowservice.UnimplementedWorkflowServiceServer
	calls atomic.Int32
}

func (s *temporalConnectionServer) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	s.calls.Add(1)
	return &workflowservice.GetSystemInfoResponse{}, nil
}

type siteRegistrationFailure struct {
	managerapi.SiteInterface
	err error
}

func (s siteRegistrationFailure) RegisterPublisher() error {
	return s.err
}
