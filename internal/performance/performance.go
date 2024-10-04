// Copyright 2020 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package performance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"

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

// this struct is not needed we can decide to pass more info and process it in the process, it would hide logic
type PerfMetric struct {
	Value   int64
	Counter string
}

func NewCollector(client *govmomi.Client, logger *logrus.Logger, perfMetricFile string, logAvailableCounters bool, collectionLevel int, batchSizePerfEntitiesString string, batchSizePerfMetricsString string) (*PerfCollector, error) {

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
		logger.WithError(err).WithField("file", perfMetricFile).Errorf("failed to fetch data from performance config file")
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
				c.logger.Errorf("failed to exec queryPerf: %s", err)
				continue
			}

			for _, returnVal := range retrievedStats.Returnval {
				//The query return a generic inside a generic, however there is only one type we ca cast to:
				// More info: https://vdc-repo.vmware.com/vmwb-repository/dcr-public/790263bc-bd30-48f1-af12-ed36055d718b/e5f17bfc-ecba-40bf-a04f-376bbb11e811/vim.PerformanceManager.html#queryStats
				metricsValues, ok := returnVal.(*types.PerfEntityMetric)
				if !ok {
					continue
				}
				c.processEntityMetrics(metricsValues, perfMetricsByRef)
			}
		}
	}
	return perfMetricsByRef
}

// The metrics returned have a field indicating the 'instance' they refer to. However, that field could be empty in some cases.
//
//	Instance is an identifier that is derived from configuration names for the device associated with the metric.
//	It identifies the instance of the metric with its source. This property may be empty.
//	-  For memory and aggregated statistics, this property is empty.
//	-  For host and virtual machine devices, this property contains the name of the device, such as the name of the host-bus   adapter or the name of the virtual Ethernet adapter. For example, “mpx.vmhba33:C0:T0:L0” or “vmnic0:”
//	-  For a CPU, this property identifies the numeric position within the CPU core, such as 0, 1, 2, 3."""
//
// We give priority to the values having the `instance` specified. If more than one value is returned we compute the average.
// If no value having an 'instance' is found for a perf metric we fall back to 'instanceless' values.
// If no value is returned we do not report that specific perf metric
type perfEvaluer struct {
	instancelessValue *int64
	accumulator       accumulator
}

type accumulator struct {
	Occurrences int64
	Sum         int64
}

func (c *PerfCollector) processEntityMetrics(metricsValues *types.PerfEntityMetric, perfMetricsByRef map[types.ManagedObjectReference][]PerfMetric) {

	// If for the same metrics multiple instances are returned we perform the average of the values
	accumulateMetrics := map[string]*perfEvaluer{}

	if metricsValues == nil {
		return
	}

	for _, metricValue := range metricsValues.Value {

		metricName, metricVal, err := c.extractValue(metricValue)
		if err != nil {
			c.logger.Debugf("extracting value %v", err)
			continue
		}

		accumulateValues(accumulateMetrics, metricName, metricValue, metricVal)
	}

	for key, val := range accumulateMetrics {
		var value int64

		//We give priority to the raw values and fall back to 'instanceless' values in case no raw data has been received
		if val.accumulator.Occurrences != 0 {
			value = val.accumulator.Sum / val.accumulator.Occurrences
		} else if val.instancelessValue != nil {
			value = *val.instancelessValue
		}

		perfMetricsByRef[metricsValues.Entity] = append(perfMetricsByRef[metricsValues.Entity], PerfMetric{
			Counter: key,
			Value:   value,
		})
	}

}

func accumulateValues(accumulateMetrics map[string]*perfEvaluer, metricName string, metricValue types.BasePerfMetricSeries, metricVal int64) {
	// This is a short-lived object, the purpose is to compute the average of the different performance metrics
	// when more than one instance per entity returns a value
	pe, ok := accumulateMetrics[metricName]
	if !ok {
		pe = &perfEvaluer{accumulator: accumulator{}}
		accumulateMetrics[metricName] = pe
	}

	if metricValue.GetPerfMetricSeries().Id.Instance != "" {
		pe.accumulator.Occurrences++
		pe.accumulator.Sum += metricVal
	} else {
		pe.instancelessValue = &metricVal
	}
}

func (c *PerfCollector) extractValue(metricValue types.BasePerfMetricSeries) (string, int64, error) {
	metricValueSeries, ok2 := metricValue.(*types.PerfMetricIntSeries)
	if !ok2 || metricValueSeries == nil {
		return "", 0, fmt.Errorf("metricValue is not of type metricValueSeries or nil")
	}

	name, ok := c.metricsAvaliableByID[metricValueSeries.Id.CounterId]
	if !ok {
		return "", 0, fmt.Errorf("perf metric Id: %v is not present in the map", metricValueSeries.Id.CounterId)
	}

	if metricValueSeries.Value == nil {
		return "", 0, fmt.Errorf("vCenter returned no samples for the metric: %v", name)
	}

	if len(metricValueSeries.Value) < 1 {
		return "", 0, fmt.Errorf(" metric: %v is not containing at least one sample, this is not expected", name)
	}

	// MaxSamples is set to 1 but the API is retrieving multiple samples with the same value for historical interval metrics.
	// We will take just first one.
	return name, metricValueSeries.Value[0], nil
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
			c.logger.Infof("%s [%d] %v %d", fullCounterName, perfCounter.Level, perfCounter.NameInfo.GetElementDescription().Summary, perfCounter.Key)
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
				// For the instance property, specify an asterisk (“*”) to retrieve instance and aggregate data
				// https://vdc-download.vmware.com/vmwb-repository/dcr-public/cdbbd51c-4824-4a1b-ad43-45df55a76a76/8cb3ed93-cac2-46aa-b329-db5a096af5bc/vsphere-web-services-sdk-67-programming-guide.pdf
				pfi := types.PerfMetricId{CounterId: counterID, Instance: "*"}

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

// This struct is used to parse the config file
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
