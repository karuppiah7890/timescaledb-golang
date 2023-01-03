package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()
	dbpool, err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create connection pool: %v\n", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	/********************************************/
	/* Create relational table                  */
	/********************************************/

	//Create relational table called sensors
	queryCreateTable := `CREATE TABLE IF NOT EXISTS sensors (id SERIAL PRIMARY KEY, type VARCHAR(50), location VARCHAR(50));`
	_, err = dbpool.Exec(ctx, queryCreateTable)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create SENSORS table: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Successfully created relational table SENSORS")

	/********************************************/
	/* Create Hypertable                        */
	/********************************************/
	// Create hypertable of time-series data called sensor_data

	//formulate statement
	queryCreateHypertable := `CREATE TABLE sensor_data (
		time TIMESTAMPTZ NOT NULL,
		sensor_id INTEGER,
		temperature DOUBLE PRECISION,
		cpu DOUBLE PRECISION,
		FOREIGN KEY (sensor_id) REFERENCES sensors (id)
		);
		SELECT create_hypertable('sensor_data', 'time');
		`

	//execute statement
	_, err = dbpool.Exec(ctx, queryCreateHypertable)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create SENSOR_DATA hypertable: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Successfully created hypertable SENSOR_DATA")

	/********************************************/
	/* INSERT into  relational table            */
	/********************************************/
	//Insert data into relational table

	// Slices of sample data to insert
	// observation i has type sensorTypes[i] and location sensorLocations[i]
	sensorTypes := []string{"a", "a", "b", "b"}
	sensorLocations := []string{"floor", "ceiling", "floor", "ceiling"}

	for i := range sensorTypes {
		//INSERT statement in SQL
		queryInsertMetadata := `INSERT INTO sensors (type, location) VALUES ($1, $2);`

		//Execute INSERT command
		_, err := dbpool.Exec(ctx, queryInsertMetadata, sensorTypes[i], sensorLocations[i])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to insert data into database: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Inserted sensor (%s, %s) into database \n", sensorTypes[i], sensorLocations[i])

	}
	fmt.Println("Successfully inserted all sensors into database")

	// Generate data to insert

	//SQL query to generate sample data
	queryDataGeneration := `
       SELECT generate_series(now() - interval '24 hour', now(), interval '5 minute') AS time,
       floor(random() * (3) + 1)::int as sensor_id,
       random()*100 AS temperature,
       random() AS cpu
       `
	//Execute query to generate samples for sensor_data hypertable
	rows, err := dbpool.Query(ctx, queryDataGeneration)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to generate sensor data: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()
	fmt.Println("Successfully generated sensor data")

	//Store data generated in slice results
	type result struct {
		Time        time.Time
		SensorId    int
		Temperature float64
		CPU         float64
	}
	var results []result
	for rows.Next() {
		var r result
		err = rows.Scan(&r.Time, &r.SensorId, &r.Temperature, &r.CPU)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to scan %v\n", err)
			os.Exit(1)
		}
		results = append(results, r)
	}
	// Any errors encountered by rows.Next or rows.Scan are returned here
	if rows.Err() != nil {
		fmt.Fprintf(os.Stderr, "rows Error: %v\n", rows.Err())
		os.Exit(1)
	}

	// Check contents of results slice
	fmt.Println("Contents of RESULTS slice")
	for i := range results {
		var r = results[i]
		fmt.Printf("Time: %s | ID: %d | Temperature: %f | CPU: %f |\n", &r.Time, r.SensorId, r.Temperature, r.CPU)
	}

	//SQL query to generate sample data
	queryInsertTimeseriesData := `
	INSERT INTO sensor_data (time, sensor_id, temperature, cpu) VALUES ($1, $2, $3, $4);
	`

	//Insert contents of results slice into TimescaleDB
	for i := range results {
		var r = results[i]
		_, err := dbpool.Exec(ctx, queryInsertTimeseriesData, r.Time, r.SensorId, r.Temperature, r.CPU)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to insert sample into Timescale %v\n", err)
			os.Exit(1)
		}
		defer rows.Close()
	}
	fmt.Println("Successfully inserted samples into sensor_data hypertable")

	// Generate data to insert

	rows, err = dbpool.Query(ctx, queryDataGeneration)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to generate sensor data: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()
	fmt.Println("Successfully generated sensor data")

	for rows.Next() {
		var r result
		err = rows.Scan(&r.Time, &r.SensorId, &r.Temperature, &r.CPU)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to scan %v\n", err)
			os.Exit(1)
		}
		results = append(results, r)
	}
	// Any errors encountered by rows.Next or rows.Scan are returned here
	if rows.Err() != nil {
		fmt.Fprintf(os.Stderr, "rows Error: %v\n", rows.Err())
		os.Exit(1)
	}

	// Check contents of results slice
	/*fmt.Println("Contents of RESULTS slice")
	  for i := range results {
	      var r result
	      r = results[i]
	      fmt.Printf("Time: %s | ID: %d | Temperature: %f | CPU: %f |\n", &r.Time, r.SensorId, r.Temperature, r.CPU)
	  }*/

	//Insert contents of results slice into TimescaleDB
	//SQL query to generate sample data

	/********************************************/
	/* Batch Insert into TimescaleDB            */
	/********************************************/
	//create batch
	batch := &pgx.Batch{}
	numInserts := len(results)
	//load insert statements into batch queue
	for i := range results {
		var r = results[i]
		batch.Queue(queryInsertTimeseriesData, r.Time, r.SensorId, r.Temperature, r.CPU)
	}
	batch.Queue("select count(*) from sensor_data")

	//send batch to connection pool
	br := dbpool.SendBatch(ctx, batch)
	//execute statements in batch queue
	_, err = br.Exec()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to execute statement in batch queue %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Successfully batch inserted data")

	//Compare length of results slice to size of table
	fmt.Printf("size of results: %d\n", numInserts)
	//check size of table for number of rows inserted
	// result of last SELECT statement
	var rowsInserted int
	err = br.QueryRow().Scan(&rowsInserted)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to scan %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("size of table: %d\n", rowsInserted)

	err = br.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to closer batch %v\n", err)
		os.Exit(1)
	}

	/********************************************/
	/* Execute a query                          */
	/********************************************/

	// Formulate query in SQL
	// Note the use of prepared statement placeholders $1 and $2
	queryTimebucketFiveMin := `
       SELECT time_bucket('5 minutes', time) AS five_min, avg(cpu)
       FROM sensor_data
       JOIN sensors ON sensors.id = sensor_data.sensor_id
       WHERE sensors.location = $1 AND sensors.type = $2
       GROUP BY five_min
       ORDER BY five_min DESC;
       `

	//Execute query on TimescaleDB
	rows, err = dbpool.Query(ctx, queryTimebucketFiveMin, "ceiling", "a")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to execute query %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()
	fmt.Println("Successfully executed query")

	//Do something with the results of query
	// Struct for results
	type result2 struct {
		Bucket time.Time
		Avg    float64
	}

	// Print rows returned and fill up results slice for later use
	// var results2 []result2
	for rows.Next() {
		var r result2
		err = rows.Scan(&r.Bucket, &r.Avg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to scan %v\n", err)
			os.Exit(1)
		}
		// results2 = append(results2, r)
		fmt.Printf("Time bucket: %s | Avg: %f\n", &r.Bucket, r.Avg)
	}
	// Any errors encountered by rows.Next or rows.Scan are returned here
	if rows.Err() != nil {
		fmt.Fprintf(os.Stderr, "rows Error: %v\n", rows.Err())
		os.Exit(1)
	}
}
