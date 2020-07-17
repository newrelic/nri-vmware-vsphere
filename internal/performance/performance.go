// Copyright 2020 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package performance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v2"

	logrus "github.com/sirupsen/logrus"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/performance"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/types"
)

const (
	counterLimit = 150 // limits the number of perf metrics to be added to avoid reach the 256 limit per event

	RealTimeInterval    = 20
	FiveMinutesInterval = 300
)

type PerfCollector struct {
	client           *govmomi.Client
	perfManager      *performance.Manager
	logger           *logrus.Logger
	MetricDefinition *perfMetricsIDs //These are the metrics contained in the config file once not available ones or not included in the level has been dropped
	collectionLevel  int             // Perf Counter level specified by Vmware

	metricsAvaliableByID   map[int32]string
	metricsAvaliableByName map[string]int32
	batchSizePerfEntities  int
	batchSizePerfMetrics   int
}

//this struct is not needed we can decide to pass more info and process it in the process, it would hide logic
type PerfMetric struct {
	Value   int64
	Counter string
}

func NewPerfCollector(client *govmomi.Client, logger *logrus.Logger, perfMetricFile string, logAvailableCounters bool, collectionLevel int, batchSizePerfEntitiesString string, batchSizePerfMetricsString string) (*PerfCollector, error) {

	batchSizePerfEntities, batchSizePerfMetrics, err := sanitizeArgs(batchSizePerfEntitiesString, batchSizePerfMetricsString)
	if err != nil {
		logger.WithError(err).Error("error while parsing args, not possible to collect perfMetrics")
		return nil, err
	}

	perfManager := performance.NewManager(client.Client)

	perfCollector := &PerfCollector{
		client:                client,
		perfManager:           perfManager,
		logger:                logger,
		collectionLevel:       collectionLevel,
		batchSizePerfEntities: batchSizePerfEntities,
		batchSizePerfMetrics:  batchSizePerfMetrics,
	}

	err = perfCollector.retrieveCounterMetadata(logAvailableCounters)
	if err != nil {
		logger.WithError(err).Errorf("failed to fetch available metrics from perfManager")
		return nil, err
	}
	err = perfCollector.parseConfigFile(perfMetricFile)
	if err != nil {
		logger.WithError(err).Errorf("failed to fetch data from config file")
		return nil, err
	}

	return perfCollector, err
}

func (c *PerfCollector) Collect(mos []types.ManagedObjectReference, metrics []types.PerfMetricId, intervalId int32) map[types.ManagedObjectReference][]PerfMetric {
	ctx := context.Background()
	perfMetricsByRef := map[types.ManagedObjectReference][]PerfMetric{}

	for i := 0; i < len(mos); i += c.batchSizePerfEntities {
		for m := 0; m < len(metrics); m += c.batchSizePerfMetrics {
			query := types.QueryPerf{
				This:      c.perfManager.Reference(),
				QuerySpec: []types.PerfQuerySpec{},
			}

			chunkEntities := mos[i:min(i+c.batchSizePerfEntities, len(mos))]
			chunkMetrics := metrics[m:min(m+c.batchSizePerfMetrics, len(metrics))]

			for _, ref := range chunkEntities {
				querySpec := types.PerfQuerySpec{
					Entity:     ref.Reference(),
					MaxSample:  1,
					MetricId:   chunkMetrics,
					IntervalId: intervalId,
					//If the optional intervalId is omitted, the metrics are returned in their originally sampled interval.
					//When an intervalId is specified, the server tries to summarize the information for the specified intervalId.
					//However, if that interval does not exist or has no data, the server summarizes the information using the best interval available.
				}
				query.QuerySpec = append(query.QuerySpec, querySpec)
			}

			retrievedStats, err := methods.QueryPerf(ctx, c.perfManager.Client(), &query)
			if err != nil {
				c.logger.Errorf("failed to exec queryPerf:%s", err)
				continue
			}

			for _, returnVal := range retrievedStats.Returnval {
				//The query return a generic inside a generic, however there is only one type we ca cast to:
				// More info: https://vdc-repo.vmware.com/vmwb-repository/dcr-public/790263bc-bd30-48f1-af12-ed36055d718b/e5f17bfc-ecba-40bf-a04f-376bbb11e811/vim.PerformanceManager.html#queryStats
				metricsValues, ok := returnVal.(*types.PerfEntityMetric)
				if !ok || metricsValues == nil {
					continue
				}
				c.processEntityMetrics(metricsValues, perfMetricsByRef)
			}
		}
	}
	return perfMetricsByRef
}

func (c *PerfCollector) processEntityMetrics(metricsValues *types.PerfEntityMetric, perfMetricsByRef map[types.ManagedObjectReference][]PerfMetric) {
	for _, metricValue := range metricsValues.Value {
		metricValueSeries, ok2 := metricValue.(*types.PerfMetricIntSeries)
		if !ok2 || metricValueSeries == nil {
			continue
		}
		name, ok := c.metricsAvaliableByID[metricValueSeries.Id.CounterId]
		if !ok {
			c.logger.Debugf("The perf metric Id:%v is not present in the map", metricValueSeries.Id.CounterId)
			continue
		}
		if metricValueSeries.Value == nil {
			c.logger.Debugf("vCenter returned no samples for the metric: %v", name)
			continue
		}
		var metricVal int64
		if len(metricValueSeries.Value) < 1 {
			c.logger.Debugf("The metric: %v is not containing at least one sample, this is not expected", name)
			continue
		}
		// MaxSamples is set to 1 but the API is retreiving multiple samples with the same value for historical interval metrics.
		// We will take just first one.
		metricVal = metricValueSeries.Value[0]
		perfMetricsByRef[metricsValues.Entity] = append(perfMetricsByRef[metricsValues.Entity], PerfMetric{
			Counter: name,
			Value:   metricVal,
		})
	}
}

func (c *PerfCollector) retrieveCounterMetadata(logAvailableCounters bool) error {
	ctx := context.Background()

	counters, err := c.perfManager.CounterInfo(ctx)
	c.metricsAvaliableByID = map[int32]string{}
	c.metricsAvaliableByName = map[string]int32{}

	if logAvailableCounters {
		c.logger.Infof("LogAvailableCounters FLAG ON, printing all %d available counters", len(counters))
	}
	for _, perfCounter := range counters {

		fullCounterName := perfCounter.GroupInfo.GetElementDescription().Key + "." + perfCounter.NameInfo.GetElementDescription().Key + "." + fmt.Sprint(perfCounter.RollupType)
		c.metricsAvaliableByName[fullCounterName] = perfCounter.Key
		c.metricsAvaliableByID[perfCounter.Key] = fullCounterName

		if logAvailableCounters {
			c.logger.Infof("%s [%d] %v", fullCounterName, perfCounter.Level, perfCounter.NameInfo.GetElementDescription().Summary)
		}
	}
	return err
}

func (c *PerfCollector) parseConfigFile(fileName string) error {
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return fmt.Errorf("error loading configuration from file. Configuration file does not exist")
	}
	configFile, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer configFile.Close()

	ymlParser := yaml.NewDecoder(configFile)

	var cf ymlConfig
	err = ymlParser.Decode(&cf)
	if err != nil {
		return err
	}

	c.MetricDefinition = &perfMetricsIDs{
		VM:                     c.buildPerMetricID(cf.VM),
		ClusterComputeResource: c.buildPerMetricID(cf.ClusterComputeResource),
		ResourcePool:           c.buildPerMetricID(cf.ResourcePool),
		Datastore:              c.buildPerMetricID(cf.Datastore),
		Host:                   c.buildPerMetricID(cf.Host),
	}

	return nil
}

func (c *PerfCollector) buildPerMetricID(countersByLevel map[string][]string) []types.PerfMetricId {
	var tmp []types.PerfMetricId
	maxLevel := fmt.Sprintf("level_%d", c.collectionLevel)
	for level, metrics := range countersByLevel {
		// compares strings es: level_2 > level_3
		if level > maxLevel {
			continue
		}
		for _, metricName := range metrics {
			if counterID, ok := c.metricsAvaliableByName[metricName]; ok {
				//““ – A string of length zero directs the vSphere Server to return only aggregated instance
				//data or rollup type data
				//https://vdc-download.vmware.com/vmwb-repository/dcr-public/cdbbd51c-4824-4a1b-ad43-45df55a76a76/8cb3ed93-cac2-46aa-b329-db5a096af5bc/vsphere-web-services-sdk-67-programming-guide.pdf
				pfi := types.PerfMetricId{CounterId: counterID, Instance: ""}

				tmp = append(tmp, pfi)
			} else {
				c.logger.WithField("metricName", metricName).Debug("metric not available")
			}
		}
	}
	// limit the number of counters to avoid reach the 256 limit on events metrics
	return tmp[:min(counterLimit, len(tmp))]
}

type perfMetricsIDs struct {
	Host                   []types.PerfMetricId
	VM                     []types.PerfMetricId
	ResourcePool           []types.PerfMetricId
	ClusterComputeResource []types.PerfMetricId
	Datastore              []types.PerfMetricId
}

//This struct is used to parse the config file
type ymlConfig struct {
	Host                   map[string][]string `yaml:"host"`
	VM                     map[string][]string `yaml:"vm"`
	ResourcePool           map[string][]string `yaml:"resourcePool"`
	ClusterComputeResource map[string][]string `yaml:"clusterComputeResource"`
	Datastore              map[string][]string `yaml:"datastore"`
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sanitizeArgs(batchSizePerfEntitiesString string, batchSizePerfMetricsString string) (int, int, error) {
	batchSizePerfEntities, err := strconv.Atoi(batchSizePerfEntitiesString)
	if err != nil {
		return 0, 0, err
	}

	if batchSizePerfEntities <= 0 {
		return 0, 0, errors.New("batchSizePerfEntities cannot be negative or zero")
	}

	batchSizePerfMetrics, err := strconv.Atoi(batchSizePerfMetricsString)
	if err != nil {
		return 0, 0, err
	}

	if batchSizePerfMetrics <= 0 {
		return 0, 0, errors.New("batchSizePerfMetrics cannot be negative or zero")
	}

	return batchSizePerfEntities, batchSizePerfMetrics, nil
}
