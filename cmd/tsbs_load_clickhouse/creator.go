package main

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/mailru/go-clickhouse" // _ "github.com/mailru/go-clickhouse"
)

// loader.DBCreator interface implementation
type dbCreator struct {
	tags    string
	cols    []string
	connStr string
}

// loader.DBCreator interface implementation
func (d *dbCreator) Init() {
	br := loader.GetBufferedReader()
	d.readDataHeader(br)
}

// readDataHeader fills dbCreator struct with data structure (tables description)
// specified at the beginning of the data file
func (d *dbCreator) readDataHeader(br *bufio.Reader) {
	// First N lines are header, describing data structure.
	// The first line containing tags table name ('tags') followed by list of tags, comma-separated.
	// Ex.: tags,hostname,region,datacenter,rack,os,arch,team,service,service_version
	// The second through N-1 line containing table name (ex.: 'cpu') followed by list of column names,
	// comma-separated. Ex.: cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq
	// The last line being blank to separate from the data
	//
	// Header example:
	// tags,hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment
	// cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
	// disk,total,free,used,used_percent,inodes_total,inodes_free,inodes_used
	// nginx,accepts,active,handled,reading,requests,waiting,writing

	i := 0
	for {
		var err error
		var line string

		if i == 0 {
			// read first line - list of tags
			d.tags, err = br.ReadString('\n')
			if err != nil {
				fatal("input has wrong header format: %v", err)
			}
			d.tags = strings.TrimSpace(d.tags)
		} else {
			// read the second and further lines - metrics descriptions
			line, err = br.ReadString('\n')
			if err != nil {
				fatal("input has wrong header format: %v", err)
			}
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				// empty line - end of header
				break
			}
			// append new table/columns set to the list of tables/columns set
			d.cols = append(d.cols, line)
		}
		i++
	}
}

// loader.DBCreator interface implementation
func (d *dbCreator) DBExists(dbName string) bool {
	db := sqlx.MustConnect(dbType, getConnectString(false))
	defer db.Close()

	sql := fmt.Sprintf("SELECT name, engine FROM system.databases WHERE name = '%s'", dbName)
	if debug > 0 {
		fmt.Printf(sql)
	}
	var rows []struct {
		Name   string `db:"name"`
		Engine string `db:"engine"`
	}

	err := db.Select(&rows, sql)
	if err != nil {
		panic(err)
	}
	for _, row := range rows {
		if row.Name == dbName {
			return true
		}
	}

	return false
}

// loader.DBCreator interface implementation
func (d *dbCreator) RemoveOldDB(dbName string) error {
	db := sqlx.MustConnect(dbType, getConnectString(false))
	defer db.Close()

	sql := fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName)
	if _, err := db.Exec(sql); err != nil {
		panic(err)
	}
	return nil
}

// loader.DBCreator interface implementation
func (d *dbCreator) CreateDB(dbName string) error {
	// Connect to ClickHouse in general and CREATE DATABASE
	db := sqlx.MustConnect(dbType, getConnectString(false))
	defer db.Close()
	sql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}

	// d.tags content:
	//tags,hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment
	//
	// Parts would contain
	// 0: tags - reserved word - tags mark
	// 1:
	// N: actual tags
	// so we'll use tags[1:] for tags specification
	parts := strings.Split(strings.TrimSpace(d.tags), ",")
	if parts[0] != "tags" {
		return fmt.Errorf("input header in wrong format. got '%s', expected 'tags'", parts[0])
	}
	tagNames, tagTypes := extractTagNamesAndTypes(parts[1:])

	// d.Cols content are lines (metrics descriptions) as:
	// cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
	// disk,total,free,used,used_percent,inodes_total,inodes_free,inodes_used
	// nginx,accepts,active,handled,reading,requests,waiting,writing
	// generalised description:
	// tableName,fieldName1,...,fieldNameX

	// cpu content:
	// cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
	cpu_content := d.cols[0]
	cpu_infos := strings.Split(strings.TrimSpace(cpu_content), ",")
	cpuMetricNames := []string{}
	cpuMetricNames = append(cpuMetricNames, cpu_infos[1:]...)
	cpuMetricTypes := make([]string, len(cpuMetricNames))

	for i := 0; i < len(cpuMetricNames); i++ {
		cpuMetricTypes[i] = "Nullable(Float64)"
	}

	if !useTSModel {
		createCpuTagMetricTable(db, cpuMetricNames, tagNames, tagTypes)

		tagCols["cpu_tags_metrics_logical_distributed"] = tagNames
		tagColumnTypes = tagTypes
		metricCols["cpu_tags_metrics_logical_distributed"] = cpuMetricNames
		tableCols["cpu_tags_metrics_logical_distributed"] = append(tagNames, cpuMetricNames...)
		tableColumnTypes["cpu_tags_metrics_logical_distributed"] = append(tagTypes, cpuMetricTypes...)
	} else {
		createTSTable(db, tagNames, tagTypes)

		tagCols["cpu_tags_metrics"] = tagNames
		tagColumnTypes = tagTypes
		metricCols["cpu_tags_metrics"] = cpuMetricNames
	}

	return nil
}

func createCpuTagMetricTable(db *sqlx.DB, metricColNames []string, tagNames, tagTypes []string) {
	localTableQuery, distributedTableQuery := generateCreateTableQuery(db, metricColNames, tagNames, tagTypes)
	if debug > 0 {
		fmt.Println(localTableQuery)
		fmt.Println(distributedTableQuery)
	}

	_, err := db.Exec(localTableQuery)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(distributedTableQuery)
	if err != nil {
		panic(err)
	}
}

func generateCreateTableQuery(db *sqlx.DB, metricColNames []string, tagNames, tagTypes []string) (string, string) {
	if len(tagNames) != len(tagTypes) {
		panic("wrong number of tag names and tag types")
	}

	// tags info
	tagColumnDefinitions := make([]string, len(tagNames))
	tagColumnName := make([]string, len(tagNames))
	for i, tagName := range tagNames {
		tagType := serializedTypeToClickHouseType(tagTypes[i])
		tagColumnDefinitions[i] = fmt.Sprintf("%s %s", tagName, tagType)
		tagColumnName[i] = fmt.Sprintf("%s", tagName)
	}

	tagsCols := strings.Join(tagColumnDefinitions, ",\n")
	key := strings.Join(tagColumnName, ",")

	// metricColsWithType - metricColName specifications with type. Ex.: "cpu_usage Nullable(Float64)"
	metricColsWithType := []string{}
	for _, metricColName := range metricColNames {
		if len(metricColName) == 0 {
			// Skip nameless columns
			continue
		}
		metricColsWithType = append(metricColsWithType, fmt.Sprintf("%s Nullable(Float64)", metricColName))
	}

	metricCols := strings.Join(metricColsWithType, ",\n")

	localTable := fmt.Sprintf(
		"CREATE TABLE %s.cpu_tags_metrics(\n"+
			"time DateTime DEFAULT now(),\n"+
			"%s,\n"+
			"%s"+
			") ENGINE = MergeTree() PARTITION BY toYYYYMM(time) ORDER BY (%s)",
		loader.DBName,
		tagsCols,
		metricCols,
		key)

	consistency := "logical_consistency_cluster"
	if usePhyConsistency {
		consistency = "physical_consistency_cluster"
		localTable = fmt.Sprintf(
			"CREATE TABLE %s.cpu_tags_metrics(\n"+
				"time DateTime DEFAULT now(),\n"+
				"%s,\n"+
				"%s"+
				") ENGINE = ReplicatedMergeTree('{namespace}/%s/cpu_tags_metrics', '{replica}') PARTITION BY toYYYYMM(time) ORDER BY (%s)",
			loader.DBName,
			tagsCols,
			metricCols,
			loader.DBName,
			key)
	}

	distributTable := fmt.Sprintf(
		"CREATE TABLE %s.cpu_tags_metrics_logical_distributed(\n"+
			"time DateTime DEFAULT now(),\n"+
			"%s,\n"+
			"%s"+
			") ENGINE = Distributed(%s, %s, cpu_tags_metrics, rand())",
		loader.DBName,
		tagsCols,
		metricCols,
		consistency,
		loader.DBName)

	return localTable, distributTable
}

// createTSTable create time series table
func createTSTable(db *sqlx.DB, tagNames, tagTypes []string) {
	insertTable, materializedTable, table, distributedTable, queryTable, calcQueryTable := genereateTSTableQuery(db, tagNames, tagTypes)
	if debug > 0 {
		fmt.Println(insertTable)
		fmt.Println(materializedTable)
		fmt.Println(table)
		fmt.Println(distributedTable)
		fmt.Println(queryTable)
		fmt.Println(calcQueryTable)
	}

	_, err := db.Exec(insertTable)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(materializedTable)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(table)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(distributedTable)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(queryTable)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(calcQueryTable)
	if err != nil {
		panic(err)
	}
}

func genereateTSTableQuery(db *sqlx.DB, tagNames, tagTypes []string) (string, string, string, string, string, string) {
	if len(tagNames) != len(tagTypes) {
		panic("wrong number of tag names and tag types")
	}

	// tags info
	tagColumnDefinitions := make([]string, len(tagNames))
	tagColumnName := make([]string, len(tagNames))
	for i, tagName := range tagNames {
		tagType := serializedTypeToClickHouseType(tagTypes[i])
		tagColumnDefinitions[i] = fmt.Sprintf("%s %s", tagName, tagType)
		tagColumnName[i] = fmt.Sprintf("%s", tagName)
	}

	tagsCols := strings.Join(tagColumnDefinitions, ",\n")
	key := strings.Join(tagColumnName, ",")

	insertTable := fmt.Sprintf(
		"CREATE TABLE %s.cpu_tags_metrics(\n"+
			"time DateTime DEFAULT now(),\n"+
			"metric_name String,\n"+
			"%s,\n"+
			"value Float64"+
			") ENGINE = Null",
		loader.DBName,
		tagsCols)

	materializedTable := fmt.Sprintf(
		"CREATE MATERIALIZED VIEW %s.mate_view_cpu_tags_metrics\n"+
			"TO %s.cpu_tags_metrics_ts AS SELECT\n"+
			"toStartOfInterval(time, toIntervalMinute(30)) AS time_series_interval,\n"+
			"metric_name,\n"+
			"%s,\n"+
			"groupArrayState((time, value)) AS time_series\n"+
			"FROM %s.cpu_tags_metrics\n"+
			"GROUP BY time_series_interval, metric_name, %s;", loader.DBName, loader.DBName, key, loader.DBName, key)

	table := fmt.Sprintf(
		"CREATE TABLE %s.cpu_tags_metrics_ts(\n"+
			"time_series_interval DateTime,\n"+
			"metric_name LowCardinality(String),\n"+
			"%s, \n"+
			"time_series AggregateFunction(groupArray, Tuple(DateTime, Float64)))\n"+
			"ENGINE = AggregatingMergeTree()\n"+
			"PARTITION BY toYYYYMM(time_series_interval)\n"+
			"ORDER BY (metric_name, time_series_interval, %s);", loader.DBName, tagsCols, key)

	consistency := "logical_consistency_cluster"
	if usePhyConsistency {
		consistency = "physical_consistency_cluster"
		table = fmt.Sprintf(
			"CREATE TABLE %s.cpu_tags_metrics_ts(\n"+
				"time_series_interval DateTime,\n"+
				"metric_name LowCardinality(String),\n"+
				"%s, \n"+
				"time_series AggregateFunction(groupArray, Tuple(DateTime, Float64)))\n"+
				"ENGINE = ReplicatedAggregatingMergeTree('{namespace}/%s/cpu_tags_metrics_ts', '{replica}')\n"+
				"PARTITION BY toYYYYMM(time_series_interval)\n"+
				"ORDER BY (metric_name, time_series_interval, %s);", loader.DBName, tagsCols, loader.DBName, key)
	}

	distributedTable := fmt.Sprintf(
		"CREATE TABLE %s.cpu_tags_metrics_ts_distributed(\n"+
			"time_series_interval DateTime,\n"+
			"metric_name LowCardinality(String),\n"+
			"%s,\n"+
			"time_series AggregateFunction(groupArray, Tuple(DateTime, Float64)))\n"+
			"ENGINE = Distributed(%s, %s, cpu_tags_metrics_ts, rand());", loader.DBName, tagsCols, consistency, loader.DBName)

	queryTable := fmt.Sprintf(
		"CREATE VIEW %s.cpu_tags_metrics_ts_query AS\n"+
			"SELECT\n"+
			"metric_name,\n"+
			"%s,\n"+
			"finalizeAggregation(time_series) AS time_series\n"+
			"FROM %s.cpu_tags_metrics_ts_distributed;", loader.DBName, key, loader.DBName)

	calcQueryTable := fmt.Sprintf(
		"CREATE VIEW %s.calc_cpu_tags_metrics_ts_query AS\n"+
			"SELECT\n"+
			"metric_name,\n"+
			"%s,\n"+
			"time_series.1 AS date_time,  time_series.2 AS value\n"+
			"FROM %s.cpu_tags_metrics_ts_query\n"+
			"ARRAY JOIN time_series AS time_series;", loader.DBName, key, loader.DBName)

	return insertTable, materializedTable, table, distributedTable, queryTable, calcQueryTable
}

// createTagsTable builds CREATE TABLE SQL statement and runs it
func createTagsTable(db *sqlx.DB, tagNames, tagTypes []string) {
	sql := generateTagsTableQuery(tagNames, tagTypes)
	if debug > 0 {
		fmt.Printf(sql)
	}
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
}

func generateTagsTableQuery(tagNames, tagTypes []string) string {
	// prepare COLUMNs specification for CREATE TABLE statement
	// all columns would be of the type specified in the tags header
	// e.g. tags, tag2 string,tag2 int32...
	if len(tagNames) != len(tagTypes) {
		panic("wrong number of tag names and tag types")
	}

	tagColumnDefinitions := make([]string, len(tagNames))
	for i, tagName := range tagNames {
		tagType := serializedTypeToClickHouseType(tagTypes[i])
		tagColumnDefinitions[i] = fmt.Sprintf("%s %s", tagName, tagType)
	}

	cols := strings.Join(tagColumnDefinitions, ",\n")

	index := "id"

	return fmt.Sprintf(
		"CREATE TABLE %s.tags(\n"+
			"created_date Date     DEFAULT today(),\n"+
			"created_at   DateTime DEFAULT now(),\n"+
			"id           UInt32,\n"+
			"%s"+
			") ENGINE = MergeTree(created_date, (%s), 8192)",
		loader.DBName,
		cols,
		index)
}

// createMetricsTable builds CREATE TABLE SQL statement and runs it
func createMetricsTable(db *sqlx.DB, tableSpec []string) {
	// tableSpec contain
	// 0: table name
	// 1: table column name 1
	// N: table column name N

	// Ex.: cpu OR disk OR nginx
	tableName := tableSpec[0]
	tableCols[tableName] = tableSpec[1:]

	// We'll have some service columns in table to be created and columnNames contains all column names to be created
	columnNames := []string{}

	if inTableTag {
		// First column in the table - service column - partitioning field
		partitioningColumn := tableCols["tags"][0] // would be 'hostname'
		columnNames = append(columnNames, partitioningColumn)
	}

	// Add all column names from tableSpec into columnNames
	columnNames = append(columnNames, tableSpec[1:]...)

	// columnsWithType - column specifications with type. Ex.: "cpu_usage Float64"
	columnsWithType := []string{}
	for _, column := range columnNames {
		if len(column) == 0 {
			// Skip nameless columns
			continue
		}
		columnsWithType = append(columnsWithType, fmt.Sprintf("%s Nullable(Float64)", column))
	}

	sql := fmt.Sprintf(`
			CREATE TABLE %s.%s (
				created_date    Date     DEFAULT today(),
				created_at      DateTime DEFAULT now(),
				time            String,
				tags_id         UInt32,
				%s,
				additional_tags String   DEFAULT ''
			) ENGINE = MergeTree(created_date, (tags_id, created_at), 8192)
			`,
		loader.DBName,
		tableName,
		strings.Join(columnsWithType, ","))
	if debug > 0 {
		fmt.Printf(sql)
	}
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
}

// getConnectString() builds HTTP/TCP connect string to ClickHouse
// db - whether database specification should be added to the connection string
func getConnectString(db bool) string {
	if useHTTP {
		return fmt.Sprintf("http://%s:%s@%s:8123", user, password, host)
	}

	// connectString: tcp://127.0.0.1:9000?debug=true
	// ClickHouse ex.:
	// tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000
	if db {
		return fmt.Sprintf("tcp://%s:9000?username=%s&password=%s&database=%s", host, user, password, loader.DatabaseName())
	}

	return fmt.Sprintf("tcp://%s:9000?username=%s&password=%s", host, user, password)
}

func extractTagNamesAndTypes(tags []string) ([]string, []string) {
	tagNames := make([]string, len(tags))
	tagTypes := make([]string, len(tags))
	for i, tagWithType := range tags {
		tagAndType := strings.Split(tagWithType, " ")
		if len(tagAndType) != 2 {
			panic("tag header has invalid format")
		}
		tagNames[i] = tagAndType[0]
		tagTypes[i] = tagAndType[1]
	}

	return tagNames, tagTypes
}

func serializedTypeToClickHouseType(serializedType string) string {
	switch serializedType {
	case "string":
		return "LowCardinality(String)"
	case "float32":
		return "Nullable(Float32)"
	case "float64":
		return "Nullable(Float64)"
	case "int64":
		return "Nullable(Int64)"
	case "int32":
		return "Nullable(Int32)"
	default:
		panic(fmt.Sprintf("unrecognized type %s", serializedType))
	}
}
