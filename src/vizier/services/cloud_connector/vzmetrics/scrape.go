/*
 * Copyright 2018- The Pixie Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vzmetrics

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"px.dev/pixie/src/utils/shared/k8s"
	"px.dev/pixie/src/vizier/messages/messagespb"
)

// Scraper is the interface for the metrics scraper that periodically scrapes metrics from the /metrics endpoint on each of the services provided to it.
type Scraper interface {
	// Run starts the metrics scraper.
	Run()
	// Stop the metrics scraper.
	Stop()
	// MetricsChannel gets the output channel for metrics scraped by the scraper.
	MetricsChannel() <-chan *messagespb.MetricsMessage
}

const (
	metricsPath          = "/metrics"
	scrapeAnnotationName = "px.dev/metrics_scrape"
	portAnnotationName   = "px.dev/metrics_port"
)

type scraperImpl struct {
	namespace    string
	period       time.Duration
	metricsCh    chan *messagespb.MetricsMessage
	httpClient   *http.Client
	k8sClientset *kubernetes.Clientset
	quitCh       chan bool
}

// NewScraper returns a new metrics scraper with the given scraping period.
func NewScraper(namespace string, period time.Duration) Scraper {
	tlsCACert := viper.GetString("tls_ca_cert")
	certPool := x509.NewCertPool()
	ca, err := os.ReadFile(tlsCACert)
	if err != nil {
		log.WithError(err).Fatal("failed to read CA cert.")
	}
	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		log.Fatal("failed to append cert to cert pool.")
	}

	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{RootCAs: certPool}
	client := &http.Client{Transport: tr}

	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		log.WithError(err).Fatal("Unable to get incluster kubeconfig")
	}
	clientset := k8s.GetClientset(kubeConfig)

	return &scraperImpl{
		namespace:    namespace,
		period:       period,
		metricsCh:    make(chan *messagespb.MetricsMessage, 128),
		httpClient:   client,
		k8sClientset: clientset,
		quitCh:       make(chan bool),
	}
}

func (s *scraperImpl) MetricsChannel() <-chan *messagespb.MetricsMessage {
	return s.metricsCh
}

type endpoint struct {
	reqPath string
	podName string
}

func (s *scraperImpl) getEndpointsToScrape() ([]endpoint, error) {
	pods, err := s.k8sClientset.CoreV1().Pods(s.namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return []endpoint{}, err
	}

	// We resolve the Service that fronts each annotated pod and use its DNS
	// name in the scrape URL. This avoids depending on `*.pod.cluster.local`
	// resolution, which is not enabled in some clusters.
	services, err := s.k8sClientset.CoreV1().Services(s.namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return []endpoint{}, err
	}

	endpoints := make([]endpoint, 0)
	for _, p := range pods.Items {
		if p.ObjectMeta.Annotations[scrapeAnnotationName] != "true" {
			continue
		}
		podName := p.ObjectMeta.Name
		port, ok := p.ObjectMeta.Annotations[portAnnotationName]
		if !ok {
			log.WithField("pod_name", podName).Warnf("Pod with %s annotation but no %s annotation which is required for metrics scraping", scrapeAnnotationName, portAnnotationName)
			continue
		}

		svc := findServiceForPod(services.Items, p, port)
		if svc == nil {
			log.WithField("pod_name", podName).WithField("port", port).
				Warn("No matching Service found for annotated pod; skipping metrics scrape")
			continue
		}

		host := k8s.GetServiceAddr(svc.ObjectMeta.Name, s.namespace)
		u := url.URL{
			Scheme: "https",
			Host:   net.JoinHostPort(host, port),
			Path:   metricsPath,
		}
		endpoints = append(endpoints, endpoint{
			reqPath: u.String(),
			podName: podName,
		})
	}
	return endpoints, nil
}

// findServiceForPod returns the Service whose selector matches the pod's
// labels and whose port set includes metricsPort. Returns nil if no Service
// fronts the pod on that port.
func findServiceForPod(services []v1.Service, pod v1.Pod, metricsPort string) *v1.Service {
	port, err := strconv.Atoi(metricsPort)
	if err != nil {
		return nil
	}
	for i := range services {
		svc := &services[i]
		if !selectorMatchesLabels(svc.Spec.Selector, pod.Labels) {
			continue
		}
		if !servicePortIncludes(svc, int32(port)) {
			continue
		}
		return svc
	}
	return nil
}

// selectorMatchesLabels reports whether every key/value in selector is present
// in labels. A service with an empty selector cannot be associated with a pod
// by labels and is skipped.
func selectorMatchesLabels(selector, labels map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// servicePortIncludes reports whether the service exposes the given numeric
// port either directly or as a numeric targetPort. Named (string) targetPorts
// are ignored: IntValue returns 0 for them, which never matches a real port.
func servicePortIncludes(svc *v1.Service, port int32) bool {
	for _, p := range svc.Spec.Ports {
		if p.Port == port || int32(p.TargetPort.IntValue()) == port {
			return true
		}
	}
	return false
}

func (s *scraperImpl) scrapeMetrics() {
	endpoints, err := s.getEndpointsToScrape()
	if err != nil {
		log.WithError(err).Error("failed to get metric endpoints to scrape")
		return
	}

	for _, e := range endpoints {
		resp, err := s.httpClient.Get(e.reqPath)
		if err != nil {
			log.WithError(err).WithField("endpoint", e).Error("failed to scrape metrics")
			continue
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			log.WithError(err).WithField("endpoint", e).Error("failed to read metrics body")
			continue
		}
		s.metricsCh <- &messagespb.MetricsMessage{
			PromMetricsText: string(b),
			PodName:         e.podName,
		}
	}
}

func (s *scraperImpl) Run() {
	t := time.NewTicker(s.period)
	defer t.Stop()

	for {
		select {
		case <-s.quitCh:
			return
		case <-t.C:
			s.scrapeMetrics()
		}
	}
}

func (s *scraperImpl) Stop() {
	s.quitCh <- true
}
