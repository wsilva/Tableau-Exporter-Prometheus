package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"time"

	c "tableau/config"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
)

var (
	hitTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tableau_server_hits_total",
		Help: "The total number of hits on tableau server.",
	})

	sessionTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tableau_server_sessions_total",
		Help: "The total number of sessions on tableau server.",
	})

	distinctUserCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tableau_server_users_count",
		Help: "The number of distinct users on the tableau server on a period of time.",
	}, []string{"period"})

	sessionDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "tableau_session_duration_seconds",
		Help:    "The time to answer user request per project/workbook/view.",
		Buckets: []float64{10, 60, 300, 600, 900, 1800, 3600, 7200, 10800, 14400, 28800},
	})

	workbookHitTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tableau_hits_total",
		Help: "The total number of hits per project/workbook/view.",
	}, []string{"project", "workbook", "view"})

	workbookDistinctUserCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tableau_users_count",
		Help: "The number of distinct users per project/workbook/view on a specific period of time.",
	}, []string{"period", "project", "workbook", "view"})

	viewLoadingTimeSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "tableau_response_time_seconds",
		Help:    "The time to answer user request per project/workbook/view.",
		Buckets: []float64{0.5, 1, 2, 3, 4, 5, 10, 15, 20, 30, 40, 50, 60},
	}, []string{"project", "workbook", "view"})

	nodeStatusCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tableau_node_status",
		Help: "The ammount of Tableau servers nodes in each status (running,degraded,error,stopped)",
	}, []string{"nodename", "hostname", "status"})

	processStatusCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tableau_process_status",
		Help: "The ammount of Tableau processes in each status (is running, status is unavailable, is in a degraded state, is in an error state, is synchronizing, is decommissioning, is running (Active Repository), is running (Passive Repository), is stopped)",
	}, []string{"nodename", "hostname", "processname", "status"})

	m = map[string]float64{}
)

// structure to store each node and status
type nodeSTATUS struct {
	nodename, hostname, status string
}

// structure to store each process and status
type processSTATUS struct {
	nodename, hostname, processname, status string
}

func WaitForCtrlC() {
	var end_waiter sync.WaitGroup
	end_waiter.Add(1)
	var signal_channel chan os.Signal
	signal_channel = make(chan os.Signal, 1)
	signal.Notify(signal_channel, os.Interrupt)
	go func() {
		<-signal_channel
		end_waiter.Done()
	}()
	end_waiter.Wait()
}

func startHttpServer(port int) *http.Server {
	srv := &http.Server{Addr: fmt.Sprintf(":%d", port)}

	go func() {
		// returns ErrServerClosed on graceful close
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			// NOTE: there is a chance that next line won't have time to run,
			// as main() doesn't wait for this goroutine to stop. don't use
			// code with race conditions like these for production. see post
			// comments below on more discussion on how to handle this.
			log.Fatalf("ListenAndServe(): %s", err)
		}
	}()

	// returning reference so caller can call Shutdown()
	return srv
}

func QueryOutInt(db *sql.DB, query string) int64 {
	var ret int64
	err := db.QueryRow(query).Scan(&ret)
	if err != nil {
		panic(err)
	}
	return ret
}

func QueryCountRow(db *sql.DB, query string) int64 {
	return QueryOutInt(db, `select count(*) FROM(`+query+`) as xdsdssff`)
}

func TotalVecQueryOffset(db *sql.DB, prometheusObject PromObj, query string) {
	keyOffset := query + "_offset"
	offset := int64(m[keyOffset])
	if offset == 0 {
		offset = QueryCountRow(db, query)
	}
	//only for debugging and get something:
	//offset = 0
	TotalVecQuery(db, prometheusObject, query+fmt.Sprintf(" OFFSET %d", offset))
}

func TotalVecQuery(db *sql.DB, prometheusObject PromObj, query string) {
	rows, err := db.Query(query)
	if err != nil {
		if err == sql.ErrNoRows {
			fmt.Println("Zero rows found")
		} else {
			panic(err)
		}
	}

	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)
		labels := make([]string, count-1)
		key := query

		for i := 0; i < len(columns)-1; i++ {
			val := values[i]
			b, _ := val.(string)
			//fmt.Printf("%d-%t-%s\n", i, ok, b)
			labels[i] = b
			key = key + labels[i] + "/"
		}

		value, ok := values[len(columns)-1].(int64)
		if ok {
			prometheusObject.Set(key, labels, float64(value))
		} else {
			value, ok := values[len(columns)-1].(float64)
			if ok {
				prometheusObject.Set(key, labels, value)
			}
		}
	}
}

func main() {

	fmt.Println("Starting...")
	var config c.Configurations = GetConfig()
	fmt.Printf("Configuration read. Scrap time = %d seconds.\n", config.ScrapeIntervalSeconds)

	http.Handle("/metrics", promhttp.Handler())
	srv := startHttpServer(config.Port)
	fmt.Printf("Metrics server started on 'http://localhost:%d/metrics'\n", config.Port)

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", config.Database.Host, config.Database.Port, config.Database.User, config.Database.Password, config.Database.Name)
	fmt.Println("Tableau DB connection: " + fmt.Sprintf("host=%s port=%d user=%s password=*** dbname=%s sslmode=disable", config.Database.Host, config.Database.Port, config.Database.User, config.Database.Name))
	fmt.Printf("Run tsm status command set to: %t\n", config.TsmStatus)
	var firstConnection bool = true

	// runs only if tsm status config is set to true
	if config.TsmStatus {

		// routine to run tsm status -v from time to time
		go func() {
			fmt.Println("Starting routine to fetch metrics from tsm cli.")
			for {

				var (
					str                                                        []string
					nodename, hostname, nodestatus, processName, processStatus string
				)
				node := make(map[nodeSTATUS]int)
				process := make(map[processSTATUS]int)
				tsmcmd := "tsm status -v"
				tsm := strings.Split(tsmcmd, " ")

				// run tsm status -v command
				out, err := exec.Command(tsm[0], tsm[1:]...).Output()
				if err != nil {
					fmt.Printf("Error running %s: %v", tsmcmd, err)
				}
				scanner := bufio.NewScanner(strings.NewReader(string(out)))

				// parsing each line
				for scanner.Scan() {
					ln := scanner.Text()

					if strings.Contains(ln, "node") { // if it finds the word node just store it
						str = strings.Split(ln, ": ")
						nodename = str[0]
						hostname = str[1]
					} else if strings.Contains(ln, "Status: ") { // if it finds the word Status we fill the struct...
						str = strings.Split(ln, "Status: ")
						nodestatus = strings.ToLower(str[1])
						// fmt.Printf("Node %s(%s): %s\n", nodename, hostname, nodestatus)
						n := nodeSTATUS{
							nodename: nodename,
							hostname: hostname,
							status:   nodestatus,
						}
						// ... and check if exists in node map to initialize or increment
						qtde, ok := node[n]
						if ok {
							node[n] = qtde + 1
						} else {
							node[n] = 1
						}
					} else if strings.Contains(ln, "Tableau Server ") { // if it finds "Tableu Server" we parse the process name and it's status...
						startprocess := strings.Index(ln, "Tableau Server ")
						endprocess := strings.Index(ln, "' ")
						if startprocess != -1 && endprocess != -1 {
							processName = strings.ToLower(strings.TrimSpace(ln[startprocess+15 : endprocess-2]))
							processStatus = strings.ToLower(strings.TrimSpace(ln[endprocess+1 : len(ln)-1]))
							// ... we fill the struct ...
							p := processSTATUS{
								nodename:    nodename,
								hostname:    hostname,
								processname: processName,
								status:      processStatus,
							}
							// ... and check if it exists in process map to increment or initialize
							qtde, ok := process[p]
							if ok {
								process[p] = qtde + 1
							} else {
								process[p] = 1
							}
						} else {
							fmt.Println("Process name or Status could not be parsed.")
						}

					}
					// else {
					// 	fmt.Println("Not able to parse this line")
					// }
				}

				// iterate over node map and process map and set the prometheus with values
				for n, qtde := range node {
					nodeStatusCount.WithLabelValues(n.nodename, n.hostname, n.status).Set(float64(qtde))
				}
				for p, qtde := range process {
					processStatusCount.WithLabelValues(p.nodename, p.hostname, p.processname, p.status).Set(float64(qtde))
				}

				// wait for the next iteration
				time.Sleep(time.Duration(config.ScrapeIntervalSeconds) * time.Second)
			}
		}()
	}

	// routine to fetch metrics from the database
	go func() {
		for {

			db, err := sql.Open("postgres", psqlInfo)
			if err != nil {
				panic(err)
			}
			defer db.Close()

			err = db.Ping()
			if err != nil {
				panic(err)
			}
			defer db.Close()

			if firstConnection {
				fmt.Print("Tableau DB connected.")
				firstConnection = false
			}

			var periods = [...]string{"month", "week", "day", "hour"}
			for _, period := range periods {
				TotalVecQuery(db, GaugeObj{distinctUserCount.WithLabelValues(period)}, fmt.Sprintf("select count(distinct hist_actor_user_id) from historical_events where created_at >= date_trunc('%s', current_timestamp)", period))

				TotalVecQuery(db, GaugeVecObj{workbookDistinctUserCount.MustCurryWith(prometheus.Labels{"period": period})},
					fmt.Sprintf(`select hist_projects.name as projectName, hist_workbooks.name as workbookName, hist_views.name as viewName, count(distinct hist_actor_user_id) as count from historical_events
						LEFT JOIN hist_projects ON historical_events.hist_project_id = hist_projects.id
						LEFT JOIN hist_workbooks ON historical_events.hist_workbook_id = hist_workbooks.id
						LEFT JOIN hist_views ON historical_events.hist_view_id = hist_views.id				
						where created_at >= date_trunc('%s', current_timestamp)
						group by hist_workbooks.name, hist_views.name, hist_projects.name`, period))
			}

			TotalVecQuery(db, CounterObj{hitTotal}, `select count(*) from historical_events`)
			TotalVecQuery(db, CounterObj{sessionTotal}, `select count(http_requests.vizql_session) FROM http_requests where http_requests.vizql_session notnull`)

			TotalVecQuery(db, CounterVecObj{workbookHitTotal},
				`select hist_projects.name as projectName, hist_workbooks.name as workbookName, hist_views.name as viewName, count(*) as count from historical_events
				LEFT JOIN hist_projects ON historical_events.hist_project_id = hist_projects.id
				LEFT JOIN hist_workbooks ON historical_events.hist_workbook_id = hist_workbooks.id
				LEFT JOIN hist_views ON historical_events.hist_view_id = hist_views.id				
				group by hist_workbooks.name, hist_views.name, hist_projects.name`)

			TotalVecQueryOffset(db, HistogramVecObj{viewLoadingTimeSeconds}, ` select
			dataset._workbooks_project_name as project,
			dataset._workbooks_name as workbook,
			dataset._views_name as view,
			EXTRACT(EPOCH from (dataset._http_requests_completed_at - _http_requests_created_at)) as durationSeconds`+
				httpQuery+
				`where (dataset._http_requests_action = 'bootstrapSession' or dataset._http_requests_action = 'show') 
			and dataset._workbooks_project_name notnull
			order by _http_requests_created_at `)

			TotalVecQueryOffset(db, HistogramObj{sessionDurationSeconds}, ` select
			table4.durationSeconds
			from
			(			
			SELECT 
				max(http_requests.completed_at) AS end_at,
				EXTRACT(EPOCH from ( max(http_requests.completed_at) - min(http_requests.created_at))) as durationSeconds,
				(SELECT max(http_requests.completed_at) from http_requests) as maxTime
			FROM http_requests
			where http_requests.vizql_session notnull
			GROUP BY  http_requests.vizql_session
			) as table4
			where (maxTime - end_at) >= INTERVAL '3600' second `)

			db.Close()
			time.Sleep(time.Duration(config.ScrapeIntervalSeconds) * time.Second)
		}
	}()

	WaitForCtrlC()
	fmt.Println("Exiting...")
	if err := srv.Shutdown(context.TODO()); err != nil {
		panic(err) // failure/timeout shutting down the server gracefully
	}

	fmt.Println("Exit.")
}

func GetConfig() c.Configurations {
	// Set the file name of the configurations file
	viper.SetConfigName("config")

	// Set the path to look for the configurations file
	viper.AddConfigPath(".")

	// Enable VIPER to read Environment Variables
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetConfigType("yml")

	var configuration c.Configurations
	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("Error reading config file, %s", err)
	}

	err := viper.Unmarshal(&configuration)
	if err != nil {
		fmt.Printf("Unable to decode into struct, %v", err)
	}

	return configuration
}
