// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package sitemgr

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/infra-controller/rest-api/cert-manager/pkg/certs"
	"github.com/NVIDIA/infra-controller/rest-api/cert-manager/pkg/core"
	fakecrdclient "github.com/NVIDIA/infra-controller/rest-api/site-manager/pkg/client/clientset/versioned/fake"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManager(t *testing.T) {
	s, err := TestManagerCreateSite()
	assert.Nil(t, err)
	assert.NotNil(t, s)
	err = s.TestManagerSiteTest()
	assert.NotNil(t, err)
	s.Teardown()
}

func TestCLI(t *testing.T) {
	cmd := NewCommand()
	assert.NotEqual(t, nil, cmd)
}

func TestNewCertManagerClient(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	servingCAPath, err := writeCAFile(srv.Certificate())
	require.NoError(t, err)
	defer os.Remove(servingCAPath)

	// A well-formed CA that simply did not sign the endpoint being dialled.
	// Every httptest server shares one built-in certificate, so a second
	// server would not be a distinct trust anchor.
	unrelatedCAPath := filepath.Join(t.TempDir(), "unrelated-ca.pem")
	require.NoError(t, os.WriteFile(unrelatedCAPath, []byte(testCACert), 0600))

	emptyPath := filepath.Join(t.TempDir(), "empty.pem")
	require.NoError(t, os.WriteFile(emptyPath, []byte("not a certificate"), 0600))

	tests := []struct {
		name string
		// caPath is the bundle handed to the client under test.
		caPath string
		// wantBuildErr means the client cannot be constructed at all, so the
		// process fails at startup rather than running unverified.
		wantBuildErr bool
		// wantRequestErr means the client is built but refuses the connection.
		wantRequestErr bool
	}{
		{
			name:   "CA that signed the endpoint is accepted",
			caPath: servingCAPath,
		},
		{
			name:           "unrelated CA is refused",
			caPath:         unrelatedCAPath,
			wantRequestErr: true,
		},
		{
			name:         "missing bundle fails at construction",
			caPath:       filepath.Join(t.TempDir(), "absent.pem"),
			wantBuildErr: true,
		},
		{
			name:         "bundle with no certificates fails at construction",
			caPath:       emptyPath,
			wantBuildErr: true,
		},
		{
			name:         "unset bundle fails at construction",
			caPath:       "",
			wantBuildErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := newCertManagerClient(t.Context(), Options{credsMgrURL: srv.URL, credsMgrCAPath: tt.caPath})
			if tt.wantBuildErr {
				assert.Error(t, err)
				assert.Nil(t, client)
				return
			}
			require.NoError(t, err)

			resp, err := client.Get(srv.URL)
			if tt.wantRequestErr {
				assert.Error(t, err, "request should fail certificate verification")
				return
			}
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		})
	}

	for _, tc := range []struct {
		name             string
		scheme           string
		wrongHostname    bool
		inheritedProxy   bool
		unsetCA          bool
		missingIdentity  bool
		noIdentity       bool
		partialIdentity  bool
		missingCA        bool
		redirect         bool
		wantInitError    bool
		wantRequestError bool
		wantStatus       int
		check            func(*testing.T)
	}{
		{name: "TLS startup diagnostics use the configured service logger", check: func(t *testing.T) {
			var logs bytes.Buffer
			logger := logrus.New()
			logger.SetOutput(&logs)
			logger.SetFormatter(&logrus.JSONFormatter{})
			ctx := core.WithLogger(t.Context(), logrus.NewEntry(logger))
			caPath := writePEM(t, srv.Certificate().Raw)
			ca, err := os.ReadFile(caPath)
			require.NoError(t, err)
			ca = append(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("invalid DER")})...)
			require.NoError(t, os.WriteFile(caPath, ca, 0600))
			certPath := writePEM(t, srv.Certificate().Raw)
			stderr, err := os.Create(filepath.Join(t.TempDir(), "stderr.log"))
			require.NoError(t, err)
			originalStderr := os.Stderr
			os.Stderr = stderr
			t.Cleanup(func() {
				os.Stderr = originalStderr
				assert.NoError(t, stderr.Close())
			})
			// Identity loading fails synchronously after the CA diagnostic, so no poller starts.
			_, err = newCertManagerClient(ctx, Options{credsMgrURL: srv.URL, credsMgrCAPath: caPath, credsMgrClientCertPath: certPath, credsMgrClientKeyPath: filepath.Join(t.TempDir(), "missing.key")})
			require.Error(t, err)
			var record map[string]any
			require.NoError(t, json.Unmarshal(logs.Bytes(), &record))
			assert.Equal(t, "error", record["level"])
			assert.Contains(t, record["msg"], "using parseable certificates")
			assert.Equal(t, caPath, record["path"])
			os.Stderr = originalStderr
			textLogs, err := os.ReadFile(stderr.Name())
			require.NoError(t, err)
			assert.Empty(t, string(textLogs), "TLS diagnostics must not also emit text records")
		}},
		{name: "verified TLS sends client certificate", scheme: "https", wantStatus: http.StatusOK},
		{name: "ignores inherited proxy for in-cluster issuance", scheme: "https", inheritedProxy: true, wantStatus: http.StatusOK},
		{name: "old chart without client identity", scheme: "https", noIdentity: true, wantStatus: http.StatusOK},
		{name: "partial client identity", scheme: "https", partialIdentity: true, wantInitError: true},
		{name: "wrong TLS identity", scheme: "https", wrongHostname: true, wantRequestError: true},
		{name: "plain HTTP refused", scheme: "http", wantInitError: true},
		{name: "missing client certificate", scheme: "https", missingIdentity: true, wantInitError: true},
		{name: "missing CA file", scheme: "https", missingCA: true, wantInitError: true},
		{name: "unset CA path", scheme: "https", unsetCA: true, wantInitError: true},
		{name: "redirect does not forward identity", scheme: "https", redirect: true, wantStatus: http.StatusTemporaryRedirect},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.check != nil {
				tc.check(t)
				return
			}
			if tc.inheritedProxy {
				original := http.DefaultTransport
				transport := original.(*http.Transport).Clone()
				transport.Proxy = func(_ *http.Request) (*url.URL, error) {
					return nil, fmt.Errorf("in-cluster issuance must bypass proxy")
				}
				http.DefaultTransport = transport
				t.Cleanup(func() { http.DefaultTransport = original })
			}
			var calls atomic.Int32
			target := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if tc.noIdentity {
					assert.Empty(t, r.TLS.PeerCertificates)
				} else {
					assert.Len(t, r.TLS.PeerCertificates, 1)
				}
				assert.Empty(t, r.Header.Get("Authorization"))
				if tc.redirect {
					http.Redirect(w, r, "/redirected", http.StatusTemporaryRedirect)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			target.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequestClientCert}
			target.StartTLS()
			t.Cleanup(target.Close)
			dir := t.TempDir()
			caPath := filepath.Join(dir, "ca.crt")
			certPath := filepath.Join(dir, "client.crt")
			keyPath := filepath.Join(dir, "client.key")
			require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: target.Certificate().Raw}), 0600))
			require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: target.Certificate().Raw}), 0600))
			keyDER, err := x509.MarshalPKCS8PrivateKey(target.TLS.Certificates[0].PrivateKey)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600))
			if tc.noIdentity {
				certPath, keyPath = "", ""
			}
			if tc.partialIdentity {
				keyPath = ""
			}
			if tc.missingIdentity {
				certPath += ".absent"
			}
			if tc.missingCA {
				caPath += ".absent"
			}
			if tc.unsetCA {
				caPath = ""
			}
			endpoint := tc.scheme + target.URL[len("https"):]
			if tc.wrongHostname {
				endpoint = strings.Replace(endpoint, "127.0.0.1", "localhost", 1)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client, err := newCertManagerClient(ctx, Options{credsMgrURL: endpoint, credsMgrClientCertPath: certPath, credsMgrClientKeyPath: keyPath, credsMgrCAPath: caPath})
			if tc.wantInitError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			t.Cleanup(client.CloseIdleConnections)
			response, err := client.Get(endpoint)
			if tc.wantRequestError {
				require.Error(t, err)
				assert.Zero(t, calls.Load())
				return
			}
			require.NoError(t, err)
			defer response.Body.Close()
			_, err = io.Copy(io.Discard, response.Body)
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, response.StatusCode)
			assert.EqualValues(t, 1, calls.Load())
		})
	}
}

func writePEM(t *testing.T, der []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600))
	return path
}

func TestNewSiteManager(t *testing.T) {
	registerer := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	t.Cleanup(func() { prometheus.DefaultRegisterer = registerer })
	var peers atomic.Int32
	var srv *httptest.Server
	srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		peers.Store(int32(len(req.TLS.PeerCertificates)))
		pair := srv.TLS.Certificates[0]
		keyDER, err := x509.MarshalPKCS8PrivateKey(pair.PrivateKey)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		response := certs.CertificateResponse{
			Certificate: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pair.Certificate[0]})),
			Key:         string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})),
		}
		err = json.NewEncoder(w).Encode(response)
		assert.NoError(t, err)
	}))
	srv.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAnyClientCert}
	srv.StartTLS()
	defer srv.Close()
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	certPath := filepath.Join(dir, "client.crt")
	keyPath := filepath.Join(dir, "client.key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	require.NoError(t, os.WriteFile(caPath, certPEM, 0600))
	require.NoError(t, os.WriteFile(certPath, certPEM, 0600))
	keyDER, err := x509.MarshalPKCS8PrivateKey(srv.TLS.Certificates[0].PrivateKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	manager, err := newSiteManager(ctx, Options{
		credsMgrURL:            srv.URL,
		credsMgrCAPath:         caPath,
		credsMgrClientCertPath: certPath,
		credsMgrClientKeyPath:  keyPath,
		ingressHost:            "test-host",
		namespace:              "site-manager",
		tlsCertPath:            certPath,
		tlsKeyPath:             keyPath,
	}, fakecrdclient.NewSimpleClientset())
	require.NoError(t, err)
	require.NotNil(t, manager)
	_, err = manager.getCertificate(ctx, "client", "site-uuid", clientTTL)
	require.NoError(t, err)
	assert.EqualValues(t, 1, peers.Load())
}
