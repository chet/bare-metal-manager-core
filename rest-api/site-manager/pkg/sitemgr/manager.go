// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package sitemgr

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/NVIDIA/infra-controller/rest-api/cert-manager/pkg/certs"
	"github.com/NVIDIA/infra-controller/rest-api/cert-manager/pkg/core"
	commontls "github.com/NVIDIA/infra-controller/rest-api/common/pkg/tls"
	crdclient "github.com/NVIDIA/infra-controller/rest-api/site-manager/pkg/client/clientset/versioned"
	"github.com/getsentry/sentry-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"k8s.io/client-go/rest"
)

const (
	vaultTimeout      = 20 * time.Second
	oneYrHours        = 365 * 24
	mgrCertFile       = "/tmp/mgr.cert"
	mgrKeyFile        = "/tmp/mgr.key"
	crdClientName     = "site-crd-client"
	certMgrClientName = "cert-manager-client"
)

// Options are args passed to the site manager at boot
type Options struct {
	credsMgrURL            string
	credsMgrClientCertPath string
	credsMgrClientKeyPath  string
	// credsMgrCAPath is the CA bundle used to verify credsMgrURL. It is the
	// ca.crt of the same CA that signs the cert-manager's listener, so the
	// certificate chain and the hostname both have to check out.
	credsMgrCAPath string
	ingressHost    string
	listenPort     string
	tlsKeyPath     string
	tlsCertPath    string
	namespace      string
	sentryDSN      string
}

// SiteMgr defines an instance of site manager
type SiteMgr struct {
	Options
	crdClient  crdclient.Interface
	appService *core.HTTPService
	certClient *http.Client
	log        *logrus.Entry
	l          net.Listener
}

// NewSiteManager creates an instance of SiteMgr
func NewSiteManager(ctx context.Context, o Options) (*SiteMgr, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "rest.InClusterConfig()")
	}

	// wrap the transport with otel
	otelWrapper := func(rt http.RoundTripper) http.RoundTripper {
		return otelhttp.NewTransport(rt, otelhttp.WithSpanNameFormatter(func(string, *http.Request) string {
			return crdClientName
		}))
	}

	cfg.WrapTransport = otelWrapper
	c, err := crdclient.NewForConfig(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "crdclient.NewForConfig(cfg)")
	}

	if o.sentryDSN != "" {
		err := sentry.Init(sentry.ClientOptions{
			Dsn:   o.sentryDSN,
			Debug: true,
		})
		if err != nil {
			return nil, fmt.Errorf("initialize Sentry: %w", err)
		}
	}

	return newSiteManager(ctx, o, c)
}

func newSiteManager(ctx context.Context, o Options, c crdclient.Interface) (*SiteMgr, error) {
	certClient, err := newCertManagerClient(ctx, o)
	if err != nil {
		return nil, errors.Wrap(err, "newCertManagerClient")
	}

	s := &SiteMgr{
		Options:    o,
		certClient: certClient,
		log:        core.GetLogger(ctx),
	}

	s.crdClient = c

	err = s.tlsSetup(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "tlsSetup")
	}
	addr := fmt.Sprintf(":%s", s.listenPort)
	appService := core.NewTLSService(addr, s.tlsCertPath, s.tlsKeyPath)
	appService.AddHealthRoute(ctx)
	appService.AddVersionRoute(ctx)
	appService.AddMetricsRoute(ctx)
	appService.Use(core.NewHTTPMiddleware(ctx, core.WithRequestMetrics("nico_rest_site_manager"))...)
	appService.Path("/v1/site").Handler(s.siteCreateHandler()).Methods("POST")
	appService.Path("/v1/site/{uuid}").Handler(s.siteGetHandler()).Methods("GET")
	appService.Path("/v1/site/roll/{uuid}").Handler(s.siteRollHandler()).Methods("POST")
	appService.Path("/v1/site/register/{uuid}").Handler(s.siteRegisterHandler()).Methods("POST")
	appService.Path("/v1/site/{uuid}").Handler(s.siteDeleteHandler()).Methods("DELETE")
	appService.Path("/v1/sitecreds").Handler(s.siteCredsHandler()).Methods("POST")
	s.appService = appService
	return s, nil
}

// Start starts the manager
func (s *SiteMgr) Start(ctx context.Context) {
	l, err := s.appService.Start(ctx)
	if err != nil {
		log.Fatalf("failed to start appService: %v", err)
	}
	s.l = l
}

// newCertManagerClient builds the HTTP client used to reach
// nico-rest-cert-manager. The configured CA bundle is the only trust anchor, so
// an unreadable or malformed file is a startup failure rather than a silent
// downgrade to an unverified connection.
func newCertManagerClient(ctx context.Context, o Options) (*http.Client, error) {
	endpoint, err := url.Parse(o.credsMgrURL)
	if err != nil {
		return nil, errors.Wrap(err, "parse certificate manager URL")
	}
	if endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil {
		return nil, fmt.Errorf("certificate manager URL must use HTTPS without user information")
	}
	if o.credsMgrCAPath == "" {
		return nil, fmt.Errorf("a CA bundle path is required to verify the Cert Manager endpoint")
	}
	caPEM, err := os.ReadFile(o.credsMgrCAPath)
	if err != nil {
		return nil, errors.Wrapf(err, "read Cert Manager CA bundle %q", o.credsMgrCAPath)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("the Cert Manager CA bundle %q contains no certificates", o.credsMgrCAPath)
	}
	tlsConfig := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if o.credsMgrClientCertPath != "" || o.credsMgrClientKeyPath != "" {
		if o.credsMgrClientCertPath == "" || o.credsMgrClientKeyPath == "" {
			return nil, fmt.Errorf("both client certificate and key paths are required")
		}
		logger := core.GetLogger(ctx).Logger
		dynamic, err := commontls.NewDynTLSCfg(o.credsMgrClientKeyPath, o.credsMgrClientCertPath, o.credsMgrCAPath, logger)
		if err != nil {
			return nil, errors.Wrap(err, "configure certificate manager client identity")
		}
		context.AfterFunc(ctx, dynamic.Close)
		tlsConfig = dynamic.ClientCfg()
	}
	client := &http.Client{
		Timeout: vaultTimeout,
		Transport: otelhttp.NewTransport(&http.Transport{TLSClientConfig: tlsConfig},
			otelhttp.WithSpanNameFormatter(func(_ string, _ *http.Request) string { return certMgrClientName })),
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}
	context.AfterFunc(ctx, client.CloseIdleConnections)
	return client, nil
}

func (s *SiteMgr) tlsSetup(ctx context.Context) error {
	log := s.log
	if s.tlsCertPath != "" && s.tlsKeyPath != "" {
		log.Infof("Using %s/%s passed via command line", s.tlsCertPath, s.tlsKeyPath)
		return nil
	}

	log.Infof("Setting up TLS Config using vault service")

	var (
		resp *certs.CertificateResponse
		err  error
	)

	for {
		resp, err = s.getCertificate(ctx, s.ingressHost, "", oneYrHours)
		if err == nil {
			break
		}
		log.Infof("getCertificate: %v, retry in 10s", err)
		time.Sleep(10 * time.Second)
	}
	err = os.WriteFile(mgrCertFile, []byte(resp.Certificate), 0644)
	if err != nil {
		return err
	}

	err = os.WriteFile(mgrKeyFile, []byte(resp.Key), 0644)
	if err != nil {
		return err
	}

	s.tlsCertPath = mgrCertFile
	s.tlsKeyPath = mgrKeyFile
	return nil
}

func (s *SiteMgr) getCertificate(ctx context.Context, name, app string, ttl int) (*certs.CertificateResponse, error) {
	body := &certs.CertificateRequest{
		Name: name,
		App:  app,
		TTL:  ttl,
	}

	payloadBuf := new(bytes.Buffer)
	err := json.NewEncoder(payloadBuf).Encode(body)
	if err != nil {
		return nil, errors.Wrap(err, "json encode payload")
	}
	url := s.credsMgrURL + "/v1/pki/cloud-cert"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, payloadBuf)
	if err != nil {
		return nil, errors.Wrap(err, "create certificate request")
	}
	content, err := s.roundTrip(req)
	if err != nil {
		return nil, errors.Wrap(err, "s.roundTrip(req)")
	}

	if content == nil {
		return nil, fmt.Errorf("empty response")
	}

	creds := &certs.CertificateResponse{}
	err = json.Unmarshal(content, &creds)
	if err != nil {
		return nil, errors.Wrap(err, "s.json.Unmarshal")
	}
	return creds, nil
}
func (s *SiteMgr) getCA(ctx context.Context) (string, error) {
	url := s.credsMgrURL + "/v1/pki/ca/pem"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", errors.Wrap(err, "create CA request")
	}
	content, err := s.roundTrip(req)
	if err != nil {
		return "", errors.Wrap(err, "s.roundTrip(req)")
	}

	if content == nil {
		return "", fmt.Errorf("empty response")
	}

	return string(content), nil
}

func (s *SiteMgr) roundTrip(req *http.Request) ([]byte, error) {
	resp, err := s.certClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusIMUsed {
		content, _ := io.ReadAll(resp.Body)
		err = fmt.Errorf("%s, %s", resp.Status, content)
		return nil, err
	}

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	return io.ReadAll(resp.Body)
}

func (s *SiteMgr) siteCreateHandler() http.Handler {
	h := &createHandler{manager: s}
	return s.withWraps(h, "csm-site-create")
}

func (s *SiteMgr) siteDeleteHandler() http.Handler {
	h := &deleteHandler{manager: s}
	return s.withWraps(h, "csm-site-delete")
}

func (s *SiteMgr) siteGetHandler() http.Handler {
	h := &getHandler{manager: s}
	return s.withWraps(h, "csm-site-get")
}

func (s *SiteMgr) siteRegisterHandler() http.Handler {
	h := &registerHandler{manager: s}
	return s.withWraps(h, "csm-site-register")
}
func (s *SiteMgr) siteRollHandler() http.Handler {
	h := &rollHandler{manager: s}
	return s.withWraps(h, "csm-site-roll")
}

func (s *SiteMgr) siteCredsHandler() http.Handler {
	h := &credsHandler{manager: s}
	return s.withWraps(h, "csm-site-bootstrap")
}

// withWraps applies the required wrappers to the handlers
func (s *SiteMgr) withWraps(h http.Handler, oper string) http.Handler {
	oh := otelhttp.NewHandler(h, oper)
	if s.sentryDSN != "" {
		return &sentryWrap{h: oh}
	}

	return oh
}

func (s *SiteMgr) writeJSONResp(w http.ResponseWriter, resp interface{}) {
	log := s.log
	c, err := json.Marshal(resp)
	if err != nil {
		log.Errorf("writeJSONResp Marshal %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(c)
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
