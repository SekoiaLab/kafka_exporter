package main

import (
	"regexp"
	"sync"
	"testing"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// TestBatchedOffsets checks that the topic phase resolves leaders from metadata, batches
// the offset lookups into one request per broker, and still emits per-partition metrics.
func TestBatchedOffsets(t *testing.T) {
	const topic = "t"

	seed := sarama.NewMockBroker(t, 1)
	defer seed.Close()

	seed.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(seed.Addr(), seed.BrokerID()).
			SetLeader(topic, 0, seed.BrokerID()).
			SetLeader(topic, 1, seed.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(topic, 0, sarama.OffsetOldest, 10).
			SetOffset(topic, 0, sarama.OffsetNewest, 40).
			SetOffset(topic, 1, sarama.OffsetOldest, 20).
			SetOffset(topic, 1, sarama.OffsetNewest, 50),
		"ListGroupsRequest": sarama.NewMockListGroupsResponse(t),
	})

	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	client, err := sarama.NewClient([]string{seed.Addr()}, config)
	if err != nil {
		t.Fatalf("cannot create client: %v", err)
	}
	defer client.Close()

	any := regexp.MustCompile(".*")
	none := regexp.MustCompile("^$")
	e := &Exporter{
		client:       client,
		topicFilter:  any,
		topicExclude: none,
		groupFilter:  any,
		groupExclude: none,
		groupWorkers: 4,
		sgMutex:      sync.Mutex{},
		// The show-all + fetch-all combination is the one this exporter is tuned for:
		// it skips DescribeGroups and sends a null-partition OffsetFetch.
		offsetShowAll:         true,
		consumerGroupFetchAll: true,
	}

	initMetricDescs(nil)

	ch := make(chan prometheus.Metric, 1024)
	e.collect(ch)
	close(ch)

	// desc name -> partition -> value
	got := map[string]map[string]float64{}
	for m := range ch {
		var pb dto.Metric
		if err := m.Write(&pb); err != nil {
			t.Fatalf("cannot write metric: %v", err)
		}
		name := m.Desc().String()
		partition := ""
		for _, l := range pb.Label {
			if l.GetName() == "partition" {
				partition = l.GetValue()
			}
		}
		if got[name] == nil {
			got[name] = map[string]float64{}
		}
		got[name][partition] = pb.GetGauge().GetValue()
	}

	find := func(fqName string) map[string]float64 {
		for name, values := range got {
			if regexp.MustCompile(`fqName: "` + fqName + `"`).MatchString(name) {
				return values
			}
		}
		t.Fatalf("metric %s not collected", fqName)
		return nil
	}

	current := find("kafka_topic_partition_current_offset")
	if current["0"] != 40 || current["1"] != 50 {
		t.Errorf("current offsets = %v, want partition 0=40 partition 1=50", current)
	}

	oldest := find("kafka_topic_partition_oldest_offset")
	if oldest["0"] != 10 || oldest["1"] != 20 {
		t.Errorf("oldest offsets = %v, want partition 0=10 partition 1=20", oldest)
	}

	if leader := find("kafka_topic_partition_leader"); leader["0"] != 1 || leader["1"] != 1 {
		t.Errorf("leaders = %v, want both on broker 1", leader)
	}

	if partitions := find("kafka_topic_partitions"); partitions[""] != 2 {
		t.Errorf("kafka_topic_partitions = %v, want 2", partitions)
	}
}
