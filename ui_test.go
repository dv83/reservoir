//go:build integration
// +build integration

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	testBaseURL = "http://localhost:8080"
	testTimeout = 5 * time.Second
)

func TestServerRunning(t *testing.T) {
	client := &http.Client{Timeout: testTimeout}
	resp, err := client.Get(testBaseURL + "/api/health")
	if err != nil {
		t.Fatalf("Server not running: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("Health check failed with status: %d", resp.StatusCode)
	}
}

func TestUIPages(t *testing.T) {
	pages := []struct {
		name string
		path string
	}{
		{"Store", "/"},
		{"Store", "/store"},
		{"Metrics", "/metrics"},
		{"Timeline", "/timeline"},
	}

	client := &http.Client{Timeout: testTimeout}

	for _, page := range pages {
		t.Run(page.name, func(t *testing.T) {
			resp, err := client.Get(testBaseURL + page.path)
			if err != nil {
				t.Errorf("Failed to load %s page: %v", page.name, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				t.Errorf("%s page returned status %d, expected 200", page.name, resp.StatusCode)
			}
		})
	}
}

func TestNavigationActiveStates(t *testing.T) {
	navTests := []struct {
		name     string
		path     string
		expected string
	}{
		{"Store", "/store", `<a href="/store" class="active">Store</a>`},
		{"Metrics", "/metrics", `<a href="/metrics" class="active">Metrics</a>`},
		{"Timeline", "/timeline", `<a href="/timeline" class="active">Timeline</a>`},
	}

	client := &http.Client{Timeout: testTimeout}

	for _, test := range navTests {
		t.Run(test.name+"_Active", func(t *testing.T) {
			resp, err := client.Get(testBaseURL + test.path)
			if err != nil {
				t.Errorf("Failed to load %s page: %v", test.name, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				t.Errorf("%s page returned status %d", test.name, resp.StatusCode)
				return
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Errorf("Failed to read %s page body: %v", test.name, err)
				return
			}

			if !strings.Contains(string(body), test.expected) {
				t.Errorf("%s navigation is not active - expected to find: %s", test.name, test.expected)
			}
		})
	}
}

func TestMetricsPageFeatures(t *testing.T) {
	client := &http.Client{Timeout: testTimeout}

	t.Run("Metrics_Page_Has_Charts", func(t *testing.T) {
		resp, err := client.Get(testBaseURL + "/metrics")
		if err != nil {
			t.Errorf("Failed to load metrics page: %v", err)
			return
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("Failed to read metrics page body: %v", err)
			return
		}

		bodyStr := string(body)

		// Check for Chart.js integration
		if !strings.Contains(bodyStr, "chart.js") {
			t.Error("Metrics page missing Chart.js - charts may not work")
		}

		// Check for canvas elements (charts)
		expectedCharts := []string{
			"operationsChart",
			"latencyChart",
			"memoryChart",
			"connectionsChart",
		}

		for _, chartID := range expectedCharts {
			if !strings.Contains(bodyStr, fmt.Sprintf(`id="%s"`, chartID)) {
				t.Errorf("Metrics page missing chart: %s", chartID)
			}
		}

		// Check that it's NOT showing raw JSON data
		if strings.Contains(bodyStr, "JSON.stringify") {
			t.Error("Metrics page is displaying raw JSON instead of charts")
		}
	})

	t.Run("Metrics_Page_Controls_Layout", func(t *testing.T) {
		resp, err := client.Get(testBaseURL + "/metrics")
		if err != nil {
			t.Errorf("Failed to load metrics page: %v", err)
			return
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("Failed to read metrics page body: %v", err)
			return
		}

		bodyStr := string(body)

		// Check for proper controls layout with CSS class
		if !strings.Contains(bodyStr, `class="controls"`) {
			t.Error("Metrics page controls have improper layout - missing 'controls' CSS class")
		}

		// Check for refresh button
		if !strings.Contains(bodyStr, `id="refreshBtn"`) {
			t.Error("Metrics page missing refresh button")
		}

		// Check for auto refresh checkbox
		if !strings.Contains(bodyStr, `id="autoRefresh"`) {
			t.Error("Metrics page missing auto refresh checkbox")
		}

		// Check for status indicator
		if !strings.Contains(bodyStr, `id="status"`) {
			t.Error("Metrics page missing status indicator")
		}
	})

	t.Run("Metrics_Page_Stat_Values_Color", func(t *testing.T) {
		resp, err := client.Get(testBaseURL + "/metrics")
		if err != nil {
			t.Errorf("Failed to load metrics page: %v", err)
			return
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("Failed to read metrics page body: %v", err)
			return
		}

		bodyStr := string(body)

		// Check that stat values are black (#333), not blue
		if strings.Contains(bodyStr, "color: #007cba") && strings.Contains(bodyStr, ".stat-value") {
			t.Error("Metrics page stat values are blue instead of black - fix CSS")
		}

		// Check for proper black color
		if !strings.Contains(bodyStr, "color: #333") {
			t.Error("Metrics page stat values should be black (#333)")
		}
	})

	t.Run("Metrics_Page_No_WebSocket", func(t *testing.T) {
		resp, err := client.Get(testBaseURL + "/metrics")
		if err != nil {
			t.Errorf("Failed to load metrics page: %v", err)
			return
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("Failed to read metrics page body: %v", err)
			return
		}

		bodyStr := string(body)

		// Check that page uses AJAX, not WebSocket
		if strings.Contains(bodyStr, "WebSocket") || strings.Contains(bodyStr, "websocket") {
			t.Error("Metrics page should use AJAX polling, not WebSocket")
		}

		// Check for fetch API usage (AJAX)
		if !strings.Contains(bodyStr, "fetch('/api/metrics')") {
			t.Error("Metrics page should use fetch() for AJAX polling")
		}
	})
}

func TestAPIEndpoints(t *testing.T) {
	apis := []struct {
		name string
		path string
	}{
		{"Health", "/api/health"},
		{"Stats", "/api/stats"},
		{"Metrics", "/api/metrics"},
		{"Connections", "/api/connections/active"},
	}

	client := &http.Client{Timeout: testTimeout}

	for _, api := range apis {
		t.Run(api.name+"_API", func(t *testing.T) {
			resp, err := client.Get(testBaseURL + api.path)
			if err != nil {
				t.Errorf("Failed to call %s API: %v", api.name, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				t.Errorf("%s API returned status %d, expected 200", api.name, resp.StatusCode)
				return
			}

			// Validate JSON
			var data interface{}
			decoder := json.NewDecoder(resp.Body)
			if err := decoder.Decode(&data); err != nil {
				t.Errorf("%s API returned invalid JSON: %v", api.name, err)
			}
		})
	}
}

// Benchmark UI page loading performance
func BenchmarkPageLoad(b *testing.B) {
	client := &http.Client{Timeout: testTimeout}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(testBaseURL + "/store")
		if err != nil {
			b.Fatalf("Failed to load store page: %v", err)
		}
		resp.Body.Close()
	}
}

// Test with Redis benchmark load
func TestUIUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping load test in short mode")
	}

	client := &http.Client{Timeout: testTimeout}

	// Test that UI is responsive during load
	resp, err := client.Get(testBaseURL + "/api/stats")
	if err != nil {
		t.Errorf("UI not responsive during load: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("Stats API returned status %d during load", resp.StatusCode)
	}
}
