package mesos

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/require"
)

var mainMetrics map[string]interface{}
var mainTestServer *httptest.Server
var subordinateMetrics map[string]interface{}

// var subordinateTaskMetrics map[string]interface{}
var subordinateTestServer *httptest.Server

func randUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func generateMetrics() {
	mainMetrics = make(map[string]interface{})

	metricNames := []string{
		// resources
		"main/cpus_percent",
		"main/cpus_used",
		"main/cpus_total",
		"main/cpus_revocable_percent",
		"main/cpus_revocable_total",
		"main/cpus_revocable_used",
		"main/disk_percent",
		"main/disk_used",
		"main/disk_total",
		"main/disk_revocable_percent",
		"main/disk_revocable_total",
		"main/disk_revocable_used",
		"main/gpus_percent",
		"main/gpus_used",
		"main/gpus_total",
		"main/gpus_revocable_percent",
		"main/gpus_revocable_total",
		"main/gpus_revocable_used",
		"main/mem_percent",
		"main/mem_used",
		"main/mem_total",
		"main/mem_revocable_percent",
		"main/mem_revocable_total",
		"main/mem_revocable_used",
		// main
		"main/elected",
		"main/uptime_secs",
		// system
		"system/cpus_total",
		"system/load_15min",
		"system/load_5min",
		"system/load_1min",
		"system/mem_free_bytes",
		"system/mem_total_bytes",
		// agents
		"main/subordinate_registrations",
		"main/subordinate_removals",
		"main/subordinate_reregistrations",
		"main/subordinate_shutdowns_scheduled",
		"main/subordinate_shutdowns_canceled",
		"main/subordinate_shutdowns_completed",
		"main/subordinates_active",
		"main/subordinates_connected",
		"main/subordinates_disconnected",
		"main/subordinates_inactive",
		// frameworks
		"main/frameworks_active",
		"main/frameworks_connected",
		"main/frameworks_disconnected",
		"main/frameworks_inactive",
		"main/outstanding_offers",
		// tasks
		"main/tasks_error",
		"main/tasks_failed",
		"main/tasks_finished",
		"main/tasks_killed",
		"main/tasks_lost",
		"main/tasks_running",
		"main/tasks_staging",
		"main/tasks_starting",
		// messages
		"main/invalid_executor_to_framework_messages",
		"main/invalid_framework_to_executor_messages",
		"main/invalid_status_update_acknowledgements",
		"main/invalid_status_updates",
		"main/dropped_messages",
		"main/messages_authenticate",
		"main/messages_deactivate_framework",
		"main/messages_decline_offers",
		"main/messages_executor_to_framework",
		"main/messages_exited_executor",
		"main/messages_framework_to_executor",
		"main/messages_kill_task",
		"main/messages_launch_tasks",
		"main/messages_reconcile_tasks",
		"main/messages_register_framework",
		"main/messages_register_subordinate",
		"main/messages_reregister_framework",
		"main/messages_reregister_subordinate",
		"main/messages_resource_request",
		"main/messages_revive_offers",
		"main/messages_status_update",
		"main/messages_status_update_acknowledgement",
		"main/messages_unregister_framework",
		"main/messages_unregister_subordinate",
		"main/messages_update_subordinate",
		"main/recovery_subordinate_removals",
		"main/subordinate_removals/reason_registered",
		"main/subordinate_removals/reason_unhealthy",
		"main/subordinate_removals/reason_unregistered",
		"main/valid_framework_to_executor_messages",
		"main/valid_status_update_acknowledgements",
		"main/valid_status_updates",
		"main/task_lost/source_main/reason_invalid_offers",
		"main/task_lost/source_main/reason_subordinate_removed",
		"main/task_lost/source_subordinate/reason_executor_terminated",
		"main/valid_executor_to_framework_messages",
		// evgqueue
		"main/event_queue_dispatches",
		"main/event_queue_http_requests",
		"main/event_queue_messages",
		// registrar
		"registrar/state_fetch_ms",
		"registrar/state_store_ms",
		"registrar/state_store_ms/max",
		"registrar/state_store_ms/min",
		"registrar/state_store_ms/p50",
		"registrar/state_store_ms/p90",
		"registrar/state_store_ms/p95",
		"registrar/state_store_ms/p99",
		"registrar/state_store_ms/p999",
		"registrar/state_store_ms/p9999",
	}

	for _, k := range metricNames {
		mainMetrics[k] = rand.Float64()
	}

	subordinateMetrics = make(map[string]interface{})

	metricNames = []string{
		// resources
		"subordinate/cpus_percent",
		"subordinate/cpus_used",
		"subordinate/cpus_total",
		"subordinate/cpus_revocable_percent",
		"subordinate/cpus_revocable_total",
		"subordinate/cpus_revocable_used",
		"subordinate/disk_percent",
		"subordinate/disk_used",
		"subordinate/disk_total",
		"subordinate/disk_revocable_percent",
		"subordinate/disk_revocable_total",
		"subordinate/disk_revocable_used",
		"subordinate/gpus_percent",
		"subordinate/gpus_used",
		"subordinate/gpus_total",
		"subordinate/gpus_revocable_percent",
		"subordinate/gpus_revocable_total",
		"subordinate/gpus_revocable_used",
		"subordinate/mem_percent",
		"subordinate/mem_used",
		"subordinate/mem_total",
		"subordinate/mem_revocable_percent",
		"subordinate/mem_revocable_total",
		"subordinate/mem_revocable_used",
		// agent
		"subordinate/registered",
		"subordinate/uptime_secs",
		// system
		"system/cpus_total",
		"system/load_15min",
		"system/load_5min",
		"system/load_1min",
		"system/mem_free_bytes",
		"system/mem_total_bytes",
		// executors
		"containerizer/mesos/container_destroy_errors",
		"subordinate/container_launch_errors",
		"subordinate/executors_preempted",
		"subordinate/frameworks_active",
		"subordinate/executor_directory_max_allowed_age_secs",
		"subordinate/executors_registering",
		"subordinate/executors_running",
		"subordinate/executors_terminated",
		"subordinate/executors_terminating",
		"subordinate/recovery_errors",
		// tasks
		"subordinate/tasks_failed",
		"subordinate/tasks_finished",
		"subordinate/tasks_killed",
		"subordinate/tasks_lost",
		"subordinate/tasks_running",
		"subordinate/tasks_staging",
		"subordinate/tasks_starting",
		// messages
		"subordinate/invalid_framework_messages",
		"subordinate/invalid_status_updates",
		"subordinate/valid_framework_messages",
		"subordinate/valid_status_updates",
	}

	for _, k := range metricNames {
		subordinateMetrics[k] = rand.Float64()
	}

	// subordinateTaskMetrics = map[string]interface{}{
	// 	"executor_id":   fmt.Sprintf("task_name.%s", randUUID()),
	// 	"executor_name": "Some task description",
	// 	"framework_id":  randUUID(),
	// 	"source":        fmt.Sprintf("task_source.%s", randUUID()),
	// 	"statistics": map[string]interface{}{
	// 		"cpus_limit":                    rand.Float64(),
	// 		"cpus_system_time_secs":         rand.Float64(),
	// 		"cpus_user_time_secs":           rand.Float64(),
	// 		"mem_anon_bytes":                float64(rand.Int63()),
	// 		"mem_cache_bytes":               float64(rand.Int63()),
	// 		"mem_critical_pressure_counter": float64(rand.Int63()),
	// 		"mem_file_bytes":                float64(rand.Int63()),
	// 		"mem_limit_bytes":               float64(rand.Int63()),
	// 		"mem_low_pressure_counter":      float64(rand.Int63()),
	// 		"mem_mapped_file_bytes":         float64(rand.Int63()),
	// 		"mem_medium_pressure_counter":   float64(rand.Int63()),
	// 		"mem_rss_bytes":                 float64(rand.Int63()),
	// 		"mem_swap_bytes":                float64(rand.Int63()),
	// 		"mem_total_bytes":               float64(rand.Int63()),
	// 		"mem_total_memsw_bytes":         float64(rand.Int63()),
	// 		"mem_unevictable_bytes":         float64(rand.Int63()),
	// 		"timestamp":                     rand.Float64(),
	// 	},
	// }
}

func TestMain(m *testing.M) {
	generateMetrics()

	mainRouter := http.NewServeMux()
	mainRouter.HandleFunc("/metrics/snapshot", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(mainMetrics)
	})
	mainTestServer = httptest.NewServer(mainRouter)

	subordinateRouter := http.NewServeMux()
	subordinateRouter.HandleFunc("/metrics/snapshot", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(subordinateMetrics)
	})
	// subordinateRouter.HandleFunc("/monitor/statistics", func(w http.ResponseWriter, r *http.Request) {
	// 	w.WriteHeader(http.StatusOK)
	// 	w.Header().Set("Content-Type", "application/json")
	// 	json.NewEncoder(w).Encode([]map[string]interface{}{subordinateTaskMetrics})
	// })
	subordinateTestServer = httptest.NewServer(subordinateRouter)

	rc := m.Run()

	mainTestServer.Close()
	subordinateTestServer.Close()
	os.Exit(rc)
}

func TestMesosMain(t *testing.T) {
	var acc testutil.Accumulator

	m := Mesos{
		Mains: []string{mainTestServer.Listener.Addr().String()},
		Timeout: 10,
	}

	err := acc.GatherError(m.Gather)

	if err != nil {
		t.Errorf(err.Error())
	}

	acc.AssertContainsFields(t, "mesos", mainMetrics)
}

func TestMainFilter(t *testing.T) {
	m := Mesos{
		MainCols: []string{
			"resources", "main", "registrar",
		},
	}
	b := []string{
		"system", "agents", "frameworks",
		"messages", "evqueue", "tasks",
	}

	m.filterMetrics(MASTER, &mainMetrics)

	for _, v := range b {
		for _, x := range getMetrics(MASTER, v) {
			if _, ok := mainMetrics[x]; ok {
				t.Errorf("Found key %s, it should be gone.", x)
			}
		}
	}
	for _, v := range m.MainCols {
		for _, x := range getMetrics(MASTER, v) {
			if _, ok := mainMetrics[x]; !ok {
				t.Errorf("Didn't find key %s, it should present.", x)
			}
		}
	}
}

func TestMesosSubordinate(t *testing.T) {
	var acc testutil.Accumulator

	m := Mesos{
		Mains: []string{},
		Subordinates:  []string{subordinateTestServer.Listener.Addr().String()},
		// SubordinateTasks: true,
		Timeout: 10,
	}

	err := acc.GatherError(m.Gather)

	if err != nil {
		t.Errorf(err.Error())
	}

	acc.AssertContainsFields(t, "mesos", subordinateMetrics)

	// expectedFields := make(map[string]interface{}, len(subordinateTaskMetrics["statistics"].(map[string]interface{}))+1)
	// for k, v := range subordinateTaskMetrics["statistics"].(map[string]interface{}) {
	// 	expectedFields[k] = v
	// }
	// expectedFields["executor_id"] = subordinateTaskMetrics["executor_id"]

	// acc.AssertContainsTaggedFields(
	// 	t,
	// 	"mesos_tasks",
	// 	expectedFields,
	// 	map[string]string{"server": "127.0.0.1", "framework_id": subordinateTaskMetrics["framework_id"].(string)})
}

func TestSubordinateFilter(t *testing.T) {
	m := Mesos{
		SubordinateCols: []string{
			"resources", "agent", "tasks",
		},
	}
	b := []string{
		"system", "executors", "messages",
	}

	m.filterMetrics(SLAVE, &subordinateMetrics)

	for _, v := range b {
		for _, x := range getMetrics(SLAVE, v) {
			if _, ok := subordinateMetrics[x]; ok {
				t.Errorf("Found key %s, it should be gone.", x)
			}
		}
	}
	for _, v := range m.MainCols {
		for _, x := range getMetrics(SLAVE, v) {
			if _, ok := subordinateMetrics[x]; !ok {
				t.Errorf("Didn't find key %s, it should present.", x)
			}
		}
	}
}

func TestWithPathDoesNotModify(t *testing.T) {
	u, err := url.Parse("http://localhost:5051")
	require.NoError(t, err)
	v := withPath(u, "/xyzzy")
	require.Equal(t, u.String(), "http://localhost:5051")
	require.Equal(t, v.String(), "http://localhost:5051/xyzzy")
}

func TestURLTagDoesNotModify(t *testing.T) {
	u, err := url.Parse("http://a:b@localhost:5051?timeout=1ms")
	require.NoError(t, err)
	v := urlTag(u)
	require.Equal(t, u.String(), "http://a:b@localhost:5051?timeout=1ms")
	require.Equal(t, v, "http://localhost:5051")
}
