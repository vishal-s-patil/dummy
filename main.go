package main

import (
	"bufio"
	"flag"
	"fmt"

	// "math"
	"os"
	// "reflect"
	"sync"
	"time"

	// "math"
	"database/sql"
	"sort"
	"test/logs"

	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	_ "github.com/vertica/vertica-sql-go"
)

var db *sql.DB
var err error

func init() {
	logs.OpenLog("benchmark_csv_vertica/dblog")

	// serverless:connectionString := "user=admin dbname=dev sslmode=require password=Netcor3VAdm1n host=vertica-redshift-test.880359164916.us-east-1.redshift-serverless.amazonaws.com port=5439"

	// provisioned:connectionString := "host=redshift-cluster-1.csguf92m3yz5.us-east-1.redshift.amazonaws.com port=5439 user=admin password=Netcore123 dbname=dev sslmode=require"

	// local:connectionString := "vertica://dbadmin:@localhost:5433/mydb?sslmode=disable"
	connectionString := "vertica://dbadmin:netcore@172.30.0.27:5433/ppsmt_20201203?sslmode=disable"
	// connectionString := "host=redshift-cluster-1.csguf92m3yz5.us-east-1.redshift.amazonaws.com port=5439 user=dbadmin password=Netcore123 dbname=dev sslmode=require"

	db, err = sql.Open("vertica", connectionString)

	if err != nil {
		print("err : ", err)
	}

	err = db.Ping()

	if err != nil {
		print(err)
	} else {
		fmt.Println("Connected to database")
	}
}

func insert_into_channel(ch chan string) {
	// file, err := os.Open("x_files.csv")
	// file, err := os.Open("x_files2.csv")
	file, err := os.Open("x_files10k.csv")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	// Create a channel to push file names
	// fileNameChannel := make(chan string)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fileName := scanner.Text()
		ch <- fileName
	}
}

func copy_new(tid int, duration *[]time.Duration, ch chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	for file_name := range ch {

		q_vertica := `
		COPY crux_72994.tmp_userdetailsattrs(cid,utype,uid,em,fk,mo,ad,adat,pc,blacklisted,att1,att2,att3,att6,att7,att9,att12,att14,att15,att16,att28,att29,att38,att39,domid,disabled,att42,att43,att44,att45,att49,att50,att62,att63,att65,att66,att67,att68,att19,ws,wadat,mbl,eadat,madat,att79,att81,att84,att85,att93,att94,att95,att96,att97,att98,att99,att100,att101,att102,att103,att104,att105,att106,att107,att108,att110,att111,att112,att113,att114,att115,att116,att117,att118,att119,att120,att121,att122,att20,att124,att125,att127,att128,att130,att131,att149,att150,att151,att152,att154,att155,att157,att158,att178,att179,att180,att181,att208,att209,att210,att211,att212,att213,att214,att215,att227,att301,att380,att381,att391,att439,att453,att469,att472,att504,att582,att584,att583,att595,att594,att623,att628,att631,att632,att633,att634,att635,att636,att637,att638,att643,att644,att645,att665,att666,att667,att668,att669,att670,att671,att672,att673,att674,att675,att676,att677,att678,att679,att680,att681,att684,att686,att687,att689,att803,att831,att906,att932,att933,att938,att975,att992,att1103,att1104,att1124,att1125,att1126,att1129,att1130,att1148,att1168,att1169,att1198,att1199,att1201,att1202,att1215,att1229,att1230,att1248,att1249,att1276,att1355,att1385,att1386,att1387,att1388,att1389,att1549,att1550,att1576,att1577,att1578,att1581,att1626,att1627,att1628,att1629,att1630,att1631,att1632,att1633,att1634,att1635,att1636,att1637,att1729,att1728,att1730,att1731) FROM 's3://vertica-redshift-benchmark/copy_files/test5/` + file_name + `' delimiter ',';`

		// fmt.Println("q_vertica : ", q_vertica)

		// elapsed, rows_affected := execute(q_vertica)
		elapsed := execute(q_vertica)

		// queryLog := fmt.Sprintf("[COPY]\t[tid:%d]\t[time:%s]\t[rows:%d]", tid, elapsed, rows_affected)
		queryLog := fmt.Sprintf("[COPY]\t[tid:%d]\t[time:%s]", tid, elapsed)
		*duration = append(*duration, elapsed)
		log.Print(queryLog)
	}
}

func execute(q string) time.Duration {
	// fmt.Println("entered")
	// if db == nil {
	// 	fmt.Println("null database")
	// 	execute(tid, q)
	// 	execute(tid, q)
	// 	return time.NOW
	// }

	// fmt.Println("executing query...")
	start_time := time.Now()
	// rows, err := db.Query(q)

	_, err := db.Exec(q)

	elapsed := time.Since(start_time)

	if err != nil {
		fmt.Println("error executing query", err)
	}
	// copyCount, err := copyResult.RowsAffected()
	// if err != nil {
	// 	fmt.Println("error in copyResult.RowsAffected", err)
	// }

	// if err != nil {
	// 	fmt.Println("error while executing : ", err)
	// }

	// columns, err := rows.Columns()
	// if err != nil {
	// 	fmt.Println("error fetching columns : ", err)
	// }

	// values := make([]interface{}, len(columns))
	// for i := range values {
	// 	var value interface{}
	// 	values[i] = &value
	// }

	// for rows.Next() {
	// 	err := rows.Scan(values...)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}

	// 	for i, value := range values {
	// 		fmt.Printf("%s: %v\t", columns[i], *value.(*interface{}))
	// 	}
	// 	// fmt.Println()
	// }

	// rows.Close()

	// return elapsed, int(copyCount)
	return elapsed
}

func select_insert(wg *sync.WaitGroup) {
	defer wg.Done()

	durations := []time.Duration{}

	// prev := 0
	prev := (time.Time{})
	x := GetMaxEpoch_ts()

	// fmt.Println("Type of x:", reflect.TypeOf(x))

	for { // prev != x || x == 0
		// if x != 0 && prev != x {
		if x != (time.Time{}) && prev != x {
			q := fmt.Sprintf(`INSERT INTO crux_72994.tmp2_userdetailsattrs (
				cid, utype, uid, em, fk, mo, ad, adat, pc, blacklisted,
				att1, att2, att3, att6, att7, att9, att12, att14, att15, att16,
				att28, att29, att38, att39, domid, disabled, att42, att43, att44,
				att45, att49, att50, att62, att63, att65, att66, att67, att68,
				att19, ws, wadat, mbl, eadat, madat, att79, att81, att84, att85,
				att93, att94, att95, att96, att97, att98, att99, att100, att101,
				att102, att103, att104, att105, att106, att107, att108, att110,
				att111, att112, att113, att114, att115, att116, att117, att118,
				att119, att120, att121, att122, att20, att124, att125, att127,
				att128, att130, att131, att149, att150, att151, att152, att154,
				att155, att157, att158, att178, att179, att180, att181, att208,
				att209, att210, att211, att212, att213, att214, att215, att227,
				att301, att380, att381, att391, att439, att453, att469, att472,
				att504, att582, att584, att583, att595, att594, att623, att628,
				att631, att632, att633, att634, att635, att636, att637, att638,
				att643, att644, att645, att665, att666, att667, att668, att669,
				att670, att671, att672, att673, att674, att675, att676, att677,
				att678, att679, att680, att681, att684, att686, att687, att689,
				att803, att831, att906, att932, att933, att938, att975, att992,
				att1103, att1104, att1124, att1125, att1126, att1129, att1130,
				att1148, att1168, att1169, att1198, att1199, att1201, att1202,
				att1215, att1229, att1230, att1248, att1249, att1276, att1355,
				att1385, att1386, att1387, att1388, att1389, att1549, att1550,
				att1576, att1577, att1578, att1581, att1626, att1627, att1628,
				att1629, att1630, att1631, att1632, att1633, att1634, att1635,
				att1636, att1637, att1729, att1728, att1730, att1731
			)
			SELECT
				cid, utype, uid, em, fk, mo, ad, adat, pc, blacklisted,
				att1, att2, att3, att6, att7, att9, att12, att14, att15, att16,
				att28, att29, att38, att39, domid, disabled, att42, att43, att44,
				att45, att49, att50, att62, att63, att65, att66, att67, att68,
				att19, ws, wadat, mbl, eadat, madat, att79, att81, att84, att85,
				att93, att94, att95, att96, att97, att98, att99, att100, att101,
				att102, att103, att104, att105, att106, att107, att108, att110,
				att111, att112, att113, att114, att115, att116, att117, att118,
				att119, att120, att121, att122, att20, att124, att125, att127,
				att128, att130, att131, att149, att150, att151, att152, att154,
				att155, att157, att158, att178, att179, att180, att181, att208,
				att209, att210, att211, att212, att213, att214, att215, att227,
				att301, att380, att381, att391, att439, att453, att469, att472,
				att504, att582, att584, att583, att595, att594, att623, att628,
				att631, att632, att633, att634, att635, att636, att637, att638,
				att643, att644, att645, att665, att666, att667, att668, att669,
				att670, att671, att672, att673, att674, att675, att676, att677,
				att678, att679, att680, att681, att684, att686, att687, att689,
				att803, att831, att906, att932, att933, att938, att975, att992,
				att1103, att1104, att1124, att1125, att1126, att1129, att1130,
				att1148, att1168, att1169, att1198, att1199, att1201, att1202,
				att1215, att1229, att1230, att1248, att1249, att1276, att1355,
				att1385, att1386, att1387, att1388, att1389, att1549, att1550,
				att1576, att1577, att1578, att1581, att1626, att1627, att1628,
				att1629, att1630, att1631, att1632, att1633, att1634, att1635,
				att1636, att1637, att1729, att1728, att1730, att1731
			FROM
				crux_72994.tmp_userdetailsattrs
			WHERE
			epoch_ts > '%s' AND epoch_ts <= '%s';`, (prev).Format("2006-01-02 15:04:05.999999"), (x).Format("2006-01-02 15:04:05.999999"))

			// fmt.Println(q)
			// (prev).Format("2006-01-02 15:04:05.999999"), (x).Format("2006-01-02 15:04:05.999999")
			// %s -> %d, '' -> , prex, x ->, format, epoch -> epoch_ts

			start_time := time.Now()
			rows, err := db.Query(q)
			if err != nil {
				fmt.Println("Error executing query:", err)
				return
			}
			defer rows.Close()

			var rowCount int
			for rows.Next() {
				err := rows.Scan(&rowCount)
				if err != nil {
					fmt.Println("Error scanning row:", err)
					return
				}
			}

			if err := rows.Err(); err != nil {
				fmt.Println("Error iterating over rows:", err)
				return
			}
			elapsed := time.Since(start_time)

			durations = append(durations, elapsed)

			selectLog := fmt.Sprintf("[select_insert]\t[time:%s]\t[args{start:%s, end:%s, rows_inserted:%d}]", elapsed, prev, x, rowCount)
			// selectLog := fmt.Sprintf("[select_insert]\t[time:%s]\t[args{start:%d, end:%d, rows_inserted:%d}]", elapsed, prev, x, rowCount)

			log.Print(selectLog)

			fmt.Println("prev, x : ", prev, x, prev == x)
		} else {
			time.Sleep(1 * time.Second)

			if len(durations) != 0 {
				sort.Slice(durations, func(i, j int) bool {
					return durations[i] < durations[j]
				})

				min := durations[0]

				max := durations[len(durations)-1]

				var total time.Duration
				for _, duration := range durations {
					total += duration
				}
				avg := total / time.Duration(len(durations))

				endLog := fmt.Sprintf("[END:SELECT_INSERT]\t[total_time:%s]\t[args:{min:%s, max:%s, avg:%s}]", total, min, max, avg)
				log.Printf(endLog)
			}

		}
		prev = x
		x = GetMaxEpoch_ts()
	}
}

func GetMaxEpoch_ts() time.Time {
	var maxEpoch interface{}
	var t time.Time
	q := "SELECT MAX(epoch_ts) FROM crux_72994.tmp_userdetailsattrs;"
	err := db.QueryRow(q).Scan(&maxEpoch)
	if err != nil {
		fmt.Println("Error executing query1:", err)
		return t
	}
	if maxEpoch == nil {
		return t
	}

	if val, ok := maxEpoch.(time.Time); ok {
		return val
	} else {
		fmt.Println("Interface value is not an time.Time")
		return t
	}
}

func GetMaxEpoch() int {
	var maxEpoch interface{}
	q := "SELECT MAX(epoch) FROM crux_72994.tmp_userdetailsattrs;"
	err := db.QueryRow(q).Scan(&maxEpoch)
	if err != nil {
		fmt.Println("Error executing query1:", err)
		return 0
	}
	if maxEpoch == nil {
		return 0
	}

	if val, ok := maxEpoch.(int); ok {
		return val
	} else {
		fmt.Println("Interface value is not an int")
		return 0
	}
}

func main() {
	f_name := flag.String("f", "", "function to call")
	flag.Parse()

	if *f_name == "copy" {
		var wg sync.WaitGroup
		var durations []time.Duration

		ch := make(chan string, 100000)

		not := 20 // 10

		/*
			incremental uid generation logic

			delclarations:
				// nor := 1000000 // 1000000
				// nor_in_each_file := 10
				// nof := nor / nor_in_each_file
				// filesPerRoutine := nof / not

			initialtion:
				start := 0
				end := filesPerRoutine

			updation:
				start = end + 1
				end = int(math.Min(float64(end+filesPerRoutine), float64(nof)))
		*/

		start_time := time.Now()
		for i := 0; i < not; i++ {
			wg.Add(1)
			go copy_new(i, &durations, ch, &wg)
		}

		insert_into_channel(ch)
		close(ch)

		wg.Wait()

		elapsed := time.Since(start_time)

		if len(durations) != 0 {
			sort.Slice(durations, func(i, j int) bool {
				return durations[i] < durations[j]
			})

			min := durations[0]

			max := durations[len(durations)-1]

			var total time.Duration
			for _, duration := range durations {
				total += duration
			}
			avg := total / time.Duration(len(durations))

			endLog := fmt.Sprintf("[END:COPY]\t[total_time:%s]\t[args:{min:%s, max:%s, avg:%s}]", elapsed, min, max, avg)
			log.Printf(endLog)
		}

	} else if *f_name == "select_insert" {
		var wg sync.WaitGroup

		wg.Add(1)
		go select_insert(&wg)

		wg.Wait()
	} else if *f_name == "um" {
		var wg sync.WaitGroup

        wg.Add(1)
        go unify_merge(&wg)

        wg.Wait()
	} else {
		fmt.Println("please provide function name")
		return
	}
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


func unify(start, end int) (time.Duration, int) {
	q_vertica := fmt.Sprintf(`
	INSERT INTO crux_72994.tmp2_userDetailsAttrs(
        cid,
        utype,
        uid,
        em,
        fk,
        mo,
        ad,
        adat,
        pc,
        blacklisted,
        att1,
        att2,
        att3,
        att6,
        att7,
        att9,
        att12,
        att14,
        att15,
        att16,
        att28,
        att29,
        att38,
        att39,
        domid,
        disabled,
        att42,
        att43,
        att44,
        att45,
        att49,
        att50,
        att62,
        att63,
        att65,
        att66,
        att67,
        att68,
        att19,
        ws,
        wadat,
        mbl,
        eadat,
        madat,
        att79,
        att81,
        att84,
        att85,
        att93,
        att94,
        att95,
        att96,
        att97,
        att98,
        att99,
        att100,
        att101,
        att102,
        att103,
        att104,
        att105,
        att106,
        att107,
        att108,
        att110,
        att111,
        att112,
        att113,
        att114,
        att115,
        att116,
        att117,
        att118,
        att119,
        att120,
        att121,
        att122,
        att20,
        att124,
        att125,
        att127,
        att128,
        att130,
        att131,
        att149,
        att150,
        att151,
        att152,
        att154,
        att155,
        att157,
        att158,
        att178,
        att179,
        att180,
        att181,
        att208,
        att209,
        att210,
        att211,
        att212,
        att213,
        att214,
        att215,
        att227,
        att301,
        att380,
        att381,
        att391,
        att439,
        att453,
        att469,
        att472,
        att504,
        att582,
        att584,
        att583,
        att595,
        att594,
        att623,
        att628,
        att631,
        att632,
        att633,
        att634,
        att635,
        att636,
        att637,
        att638,
        att643,
        att644,
        att645,
        att665,
        att666,
        att667,
        att668,
        att669,
        att670,
        att671,
        att672,
        att673,
        att674,
        att675,
        att676,
        att677,
        att678,
        att679,
        att680,
        att681,
        att684,
        att686,
        att687,
        att689,
        att803,
        att831,
        att906,
        att932,
        att933,
        att938,
        att975,
        att992,
        att1103,
        att1104,
        att1124,
        att1125,
        att1126,
        att1129,
        att1130,
        att1148,
        att1168,
        att1169,
        att1198,
        att1199,
        att1201,
        att1202,
        att1215,
        att1229,
        att1230,
        att1248,
        att1249,
        att1276,
        att1355,
        att1385,
        att1386,
        att1387,
        att1388,
        att1389,
        att1549,
        att1550,
        att1576,
        att1577,
        att1578,
        att1581,
        createddate
    )
SELECT cid,
    utype,
    uid,
    FIRST_VALUE(em IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS em,
    FIRST_VALUE(fk IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS fk,
    FIRST_VALUE(mo IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS mo,
    LAST_VALUE(ad IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS ad,
    LAST_VALUE(adat IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS adat,
    FIRST_VALUE(pc IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS pc,
    FIRST_VALUE(blacklisted IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS blacklisted,
    FIRST_VALUE(att1 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1,
    FIRST_VALUE(att2 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att2,
    FIRST_VALUE(att3 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att3,
    FIRST_VALUE(att6 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att6,
    FIRST_VALUE(att7 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att7,
    FIRST_VALUE(att9 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att9,
    FIRST_VALUE(att12 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att12,
    FIRST_VALUE(att14 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att14,
    FIRST_VALUE(att15 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att15,
    FIRST_VALUE(att16 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att16,
    FIRST_VALUE(att28 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att28,
    FIRST_VALUE(att29 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att29,
    FIRST_VALUE(att38 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att38,
    FIRST_VALUE(att39 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att39,
    FIRST_VALUE(domid IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS domid,
    FIRST_VALUE(disabled IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS disabled,
    FIRST_VALUE(att42 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att42,
    FIRST_VALUE(att43 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att43,
    FIRST_VALUE(att44 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att44,
    FIRST_VALUE(att45 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att45,
    FIRST_VALUE(att49 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att49,
    FIRST_VALUE(att50 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att50,
    FIRST_VALUE(att62 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att62,
    FIRST_VALUE(att63 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att63,
    FIRST_VALUE(att65 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att65,
    FIRST_VALUE(att66 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att66,
    FIRST_VALUE(att67 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att67,
    FIRST_VALUE(att68 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att68,
    FIRST_VALUE(att19 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att19,
    FIRST_VALUE(ws IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS ws,
    FIRST_VALUE(wadat IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS wadat,
    FIRST_VALUE(mbl IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS mbl,
    FIRST_VALUE(eadat IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS eadat,
    FIRST_VALUE(madat IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS madat,
    FIRST_VALUE(att79 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att79,
    FIRST_VALUE(att81 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att81,
    FIRST_VALUE(att84 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att84,
    FIRST_VALUE(att85 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att85,
    FIRST_VALUE(att93 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att93,
    FIRST_VALUE(att94 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att94,
    FIRST_VALUE(att95 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att95,
    FIRST_VALUE(att96 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att96,
    FIRST_VALUE(att97 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att97,
    FIRST_VALUE(att98 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att98,
    FIRST_VALUE(att99 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att99,
    FIRST_VALUE(att100 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att100,
    FIRST_VALUE(att101 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att101,
    FIRST_VALUE(att102 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att102,
    FIRST_VALUE(att103 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att103,
    FIRST_VALUE(att104 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att104,
    FIRST_VALUE(att105 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att105,
    FIRST_VALUE(att106 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att106,
    FIRST_VALUE(att107 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att107,
    FIRST_VALUE(att108 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att108,
    FIRST_VALUE(att110 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att110,
    FIRST_VALUE(att111 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att111,
    FIRST_VALUE(att112 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att112,
    FIRST_VALUE(att113 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att113,
    FIRST_VALUE(att114 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att114,
    FIRST_VALUE(att115 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att115,
    FIRST_VALUE(att116 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att116,
    FIRST_VALUE(att117 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att117,
    FIRST_VALUE(att118 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att118,
    FIRST_VALUE(att119 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att119,
    FIRST_VALUE(att120 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att120,
    FIRST_VALUE(att121 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att121,
    FIRST_VALUE(att122 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att122,
    FIRST_VALUE(att20 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att20,
    FIRST_VALUE(att124 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att124,
    FIRST_VALUE(att125 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att125,
    FIRST_VALUE(att127 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att127,
    FIRST_VALUE(att128 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att128,
    FIRST_VALUE(att130 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att130,
    FIRST_VALUE(att131 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att131,
    FIRST_VALUE(att149 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att149,
    FIRST_VALUE(att150 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att150,
    FIRST_VALUE(att151 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att151,
    FIRST_VALUE(att152 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att152,
    FIRST_VALUE(att154 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att154,
    FIRST_VALUE(att155 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att155,
    FIRST_VALUE(att157 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att157,
    FIRST_VALUE(att158 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att158,
    FIRST_VALUE(att178 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att178,
    FIRST_VALUE(att179 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att179,
    FIRST_VALUE(att180 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att180,
    FIRST_VALUE(att181 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att181,
    FIRST_VALUE(att208 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att208,
    FIRST_VALUE(att209 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att209,
    FIRST_VALUE(att210 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att210,
    FIRST_VALUE(att211 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att211,
    FIRST_VALUE(att212 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att212,
    FIRST_VALUE(att213 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att213,
    FIRST_VALUE(att214 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att214,
    FIRST_VALUE(att215 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att215,
    FIRST_VALUE(att227 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att227,
    FIRST_VALUE(att301 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att301,
    FIRST_VALUE(att380 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att380,
    FIRST_VALUE(att381 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att381,
    FIRST_VALUE(att391 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att391,
    FIRST_VALUE(att439 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att439,
    FIRST_VALUE(att453 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att453,
    FIRST_VALUE(att469 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att469,
    FIRST_VALUE(att472 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att472,
    FIRST_VALUE(att504 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att504,
    FIRST_VALUE(att582 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att582,
    FIRST_VALUE(att584 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att584,
    FIRST_VALUE(att583 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att583,
    FIRST_VALUE(att595 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att595,
    FIRST_VALUE(att594 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att594,
    FIRST_VALUE(att623 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att623,
    FIRST_VALUE(att628 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att628,
    FIRST_VALUE(att631 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att631,
    FIRST_VALUE(att632 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att632,
    FIRST_VALUE(att633 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att633,
    FIRST_VALUE(att634 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att634,
    FIRST_VALUE(att635 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att635,
    FIRST_VALUE(att636 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att636,
    FIRST_VALUE(att637 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att637,
    FIRST_VALUE(att638 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att638,
    FIRST_VALUE(att643 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att643,
    FIRST_VALUE(att644 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att644,
    FIRST_VALUE(att645 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att645,
    FIRST_VALUE(att665 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att665,
    FIRST_VALUE(att666 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att666,
    FIRST_VALUE(att667 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att667,
    FIRST_VALUE(att668 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att668,
    FIRST_VALUE(att669 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att669,
    FIRST_VALUE(att670 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att670,
    FIRST_VALUE(att671 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att671,
    FIRST_VALUE(att672 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att672,
    FIRST_VALUE(att673 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att673,
    FIRST_VALUE(att674 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att674,
    FIRST_VALUE(att675 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att675,
    FIRST_VALUE(att676 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att676,
    FIRST_VALUE(att677 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att677,
    FIRST_VALUE(att678 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att678,
    FIRST_VALUE(att679 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att679,
    FIRST_VALUE(att680 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att680,
    FIRST_VALUE(att681 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att681,
    FIRST_VALUE(att684 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att684,
    FIRST_VALUE(att686 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att686,
    FIRST_VALUE(att687 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att687,
    FIRST_VALUE(att689 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att689,
    FIRST_VALUE(att803 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att803,
    FIRST_VALUE(att831 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att831,
    FIRST_VALUE(att906 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att906,
    FIRST_VALUE(att932 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att932,
    FIRST_VALUE(att933 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att933,
    FIRST_VALUE(att938 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att938,
    FIRST_VALUE(att975 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att975,
    FIRST_VALUE(att992 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att992,
    FIRST_VALUE(att1103 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1103,
    FIRST_VALUE(att1104 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1104,
    FIRST_VALUE(att1124 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1124,
    FIRST_VALUE(att1125 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1125,
    FIRST_VALUE(att1126 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1126,
    FIRST_VALUE(att1129 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1129,
    FIRST_VALUE(att1130 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1130,
    FIRST_VALUE(att1148 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1148,
    FIRST_VALUE(att1168 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1168,
    FIRST_VALUE(att1169 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1169,
    FIRST_VALUE(att1198 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1198,
    FIRST_VALUE(att1199 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1199,
    FIRST_VALUE(att1201 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1201,
    FIRST_VALUE(att1202 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1202,
    FIRST_VALUE(att1215 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1215,
    FIRST_VALUE(att1229 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1229,
    FIRST_VALUE(att1230 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1230,
    FIRST_VALUE(att1248 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1248,
    FIRST_VALUE(att1249 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1249,
    FIRST_VALUE(att1276 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1276,
    FIRST_VALUE(att1355 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1355,
    FIRST_VALUE(att1385 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1385,
    FIRST_VALUE(att1386 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1386,
    FIRST_VALUE(att1387 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1387,
    FIRST_VALUE(att1388 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1388,
    FIRST_VALUE(att1389 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1389,
    FIRST_VALUE(att1549 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1549,
    FIRST_VALUE(att1550 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1550,
    FIRST_VALUE(att1576 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1576,
    FIRST_VALUE(att1577 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1577,
    FIRST_VALUE(att1578 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1578,
    FIRST_VALUE(att1581 IGNORE NULLS) OVER(
        w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS att1581,
    createddate
FROM crux_72994.tmp_userDetailsAttrs
WHERE epoch > %d
    and epoch <= %d WINDOW w AS (
        PARTITION BY uid,
        utype
        ORDER BY createddate desc
    )
LIMIT 1 OVER(
        PARTITION BY uid,
        utype
        ORDER BY createddate desc
    );
	`, start, end)

	// start.Format("2006-01-02 15:04:05.999999"), end.Format("2006-01-02 15:04:05.999999")

	elapsed, rows := execute_unify_merge(q_vertica)
	return elapsed, rows
}

func merge() (time.Duration, int) {
	q_merge := `
	MERGE INTO crux_72994.userDetailsAttrs t1 USING crux_72994.tmp2_userDetailsAttrs t2 ON t2.uid = t1.uid
AND t2.utype = t1.utype
WHEN MATCHED THEN
UPDATE
SET cid =(
        case
            when t2.cid::VARCHAR = '-11111' then NULL
            else nvl(t2.cid, t1.cid)
        end
    ),
    utype =(
        case
            when t2.utype::VARCHAR = '-11111' then NULL
            else nvl(t2.utype, t1.utype)
        end
    ),
    uid =(
        case
            when t2.uid::VARCHAR = '-11111' then NULL
            else nvl(t2.uid, t1.uid)
        end
    ),
    em =(
        case
            when t2.em::VARCHAR = '-11111' then NULL
            else nvl(t2.em, t1.em)
        end
    ),
    fk =(
        case
            when t2.fk::VARCHAR = '-11111' then NULL
            else nvl(t2.fk, t1.fk)
        end
    ),
    mo =(
        case
            when t2.mo::VARCHAR = '-11111' then NULL
            else nvl(t2.mo, t1.mo)
        end
    ),
    pc =(
        case
            when t2.pc::VARCHAR = '-11111' then NULL
            else nvl(t2.pc, t1.pc)
        end
    ),
    blacklisted =(
        case
            when t2.blacklisted::VARCHAR = '-11111' then NULL
            else nvl(t2.blacklisted, t1.blacklisted)
        end
    ),
    att1 =(
        case
            when t2.att1::VARCHAR = '-11111' then NULL
            else nvl(t2.att1, t1.att1)
        end
    ),
    att2 =(
        case
            when t2.att2::VARCHAR = '-11111' then NULL
            else nvl(t2.att2, t1.att2)
        end
    ),
    att3 =(
        case
            when t2.att3::VARCHAR = '-11111' then NULL
            else nvl(t2.att3, t1.att3)
        end
    ),
    att6 =(
        case
            when t2.att6::VARCHAR = '-11111' then NULL
            else nvl(t2.att6, t1.att6)
        end
    ),
    att7 =(
        case
            when t2.att7::VARCHAR = '-11111' then NULL
            else nvl(t2.att7, t1.att7)
        end
    ),
    att9 =(
        case
            when t2.att9::VARCHAR = '-11111' then NULL
            else nvl(t2.att9, t1.att9)
        end
    ),
    att12 =(
        case
            when t2.att12::VARCHAR = '-11111' then NULL
            else nvl(t2.att12, t1.att12)
        end
    ),
    att14 =(
        case
            when t2.att14::VARCHAR = '-11111' then NULL
            else nvl(t2.att14, t1.att14)
        end
    ),
    att15 =(
        case
            when t2.att15::VARCHAR = '-11111' then NULL
            else nvl(t2.att15, t1.att15)
        end
    ),
    att16 =(
        case
            when t2.att16::VARCHAR = '-11111' then NULL
            else nvl(t2.att16, t1.att16)
        end
    ),
    att28 =(
        case
            when t2.att28::VARCHAR = '-11111' then NULL
            else nvl(t2.att28, t1.att28)
        end
    ),
    att29 =(
        case
            when t2.att29::VARCHAR = '-11111' then NULL
            else nvl(t2.att29, t1.att29)
        end
    ),
    att38 =(
        case
            when t2.att38::VARCHAR = '-11111' then NULL
            else nvl(t2.att38, t1.att38)
        end
    ),
    att39 =(
        case
            when t2.att39::VARCHAR = '-11111' then NULL
            else nvl(t2.att39, t1.att39)
        end
    ),
    domid =(
        case
            when t2.domid::VARCHAR = '-11111' then NULL
            else nvl(t2.domid, t1.domid)
        end
    ),
    disabled =(
        case
            when t2.disabled::VARCHAR = '-11111' then NULL
            else nvl(t2.disabled, t1.disabled)
        end
    ),
    att42 =(
        case
            when t2.att42::VARCHAR = '-11111' then NULL
            else nvl(t2.att42, t1.att42)
        end
    ),
    att43 =(
        case
            when t2.att43::VARCHAR = '-11111' then NULL
            else nvl(t2.att43, t1.att43)
        end
    ),
    att44 =(
        case
            when t2.att44::VARCHAR = '-11111' then NULL
            else nvl(t2.att44, t1.att44)
        end
    ),
    att45 =(
        case
            when t2.att45::VARCHAR = '-11111' then NULL
            else nvl(t2.att45, t1.att45)
        end
    ),
    att49 =(
        case
            when t2.att49::VARCHAR = '-11111' then NULL
            else nvl(t2.att49, t1.att49)
        end
    ),
    att50 =(
        case
            when t2.att50::VARCHAR = '-11111' then NULL
            else nvl(t2.att50, t1.att50)
        end
    ),
    att62 =(
        case
            when t2.att62::VARCHAR = '-11111' then NULL
            else nvl(t2.att62, t1.att62)
        end
    ),
    att63 =(
        case
            when t2.att63::VARCHAR = '-11111' then NULL
            else nvl(t2.att63, t1.att63)
        end
    ),
    att65 =(
        case
            when t2.att65::VARCHAR = '-11111' then NULL
            else nvl(t2.att65, t1.att65)
        end
    ),
    att66 =(
        case
            when t2.att66::VARCHAR = '-11111' then NULL
            else nvl(t2.att66, t1.att66)
        end
    ),
    att67 =(
        case
            when t2.att67::VARCHAR = '-11111' then NULL
            else nvl(t2.att67, t1.att67)
        end
    ),
    att68 =(
        case
            when t2.att68::VARCHAR = '-11111' then NULL
            else nvl(t2.att68, t1.att68)
        end
    ),
    att19 =(
        case
            when t2.att19::VARCHAR = '-11111' then NULL
            else nvl(t2.att19, t1.att19)
        end
    ),
    ws =(
        case
            when t2.ws::VARCHAR = '-11111' then NULL
            else nvl(t2.ws, t1.ws)
        end
    ),
    wadat =(
        case
            when t2.wadat::VARCHAR = '-11111' then NULL
            else nvl(t2.wadat, t1.wadat)
        end
    ),
    mbl =(
        case
            when t2.mbl::VARCHAR = '-11111' then NULL
            else nvl(t2.mbl, t1.mbl)
        end
    ),
    eadat =(
        case
            when t2.eadat::VARCHAR = '-11111' then NULL
            else nvl(t2.eadat, t1.eadat)
        end
    ),
    madat =(
        case
            when t2.madat::VARCHAR = '-11111' then NULL
            else nvl(t2.madat, t1.madat)
        end
    ),
    att79 =(
        case
            when t2.att79::VARCHAR = '-11111' then NULL
            else nvl(t2.att79, t1.att79)
        end
    ),
    att81 =(
        case
            when t2.att81::VARCHAR = '-11111' then NULL
            else nvl(t2.att81, t1.att81)
        end
    ),
    att84 =(
        case
            when t2.att84::VARCHAR = '-11111' then NULL
            else nvl(t2.att84, t1.att84)
        end
    ),
    att85 =(
        case
            when t2.att85::VARCHAR = '-11111' then NULL
            else nvl(t2.att85, t1.att85)
        end
    ),
    att93 =(
        case
            when t2.att93::VARCHAR = '-11111' then NULL
            else nvl(t2.att93, t1.att93)
        end
    ),
    att94 =(
        case
            when t2.att94::VARCHAR = '-11111' then NULL
            else nvl(t2.att94, t1.att94)
        end
    ),
    att95 =(
        case
            when t2.att95::VARCHAR = '-11111' then NULL
            else nvl(t2.att95, t1.att95)
        end
    ),
    att96 =(
        case
            when t2.att96::VARCHAR = '-11111' then NULL
            else nvl(t2.att96, t1.att96)
        end
    ),
    att97 =(
        case
            when t2.att97::VARCHAR = '-11111' then NULL
            else nvl(t2.att97, t1.att97)
        end
    ),
    att98 =(
        case
            when t2.att98::VARCHAR = '-11111' then NULL
            else nvl(t2.att98, t1.att98)
        end
    ),
    att99 =(
        case
            when t2.att99::VARCHAR = '-11111' then NULL
            else nvl(t2.att99, t1.att99)
        end
    ),
    att100 =(
        case
            when t2.att100::VARCHAR = '-11111' then NULL
            else nvl(t2.att100, t1.att100)
        end
    ),
    att101 =(
        case
            when t2.att101::VARCHAR = '-11111' then NULL
            else nvl(t2.att101, t1.att101)
        end
    ),
    att102 =(
        case
            when t2.att102::VARCHAR = '-11111' then NULL
            else nvl(t2.att102, t1.att102)
        end
    ),
    att103 =(
        case
            when t2.att103::VARCHAR = '-11111' then NULL
            else nvl(t2.att103, t1.att103)
        end
    ),
    att104 =(
        case
            when t2.att104::VARCHAR = '-11111' then NULL
            else nvl(t2.att104, t1.att104)
        end
    ),
    att105 =(
        case
            when t2.att105::VARCHAR = '-11111' then NULL
            else nvl(t2.att105, t1.att105)
        end
    ),
    att106 =(
        case
            when t2.att106::VARCHAR = '-11111' then NULL
            else nvl(t2.att106, t1.att106)
        end
    ),
    att107 =(
        case
            when t2.att107::VARCHAR = '-11111' then NULL
            else nvl(t2.att107, t1.att107)
        end
    ),
    att108 =(
        case
            when t2.att108::VARCHAR = '-11111' then NULL
            else nvl(t2.att108, t1.att108)
        end
    ),
    att110 =(
        case
            when t2.att110::VARCHAR = '-11111' then NULL
            else nvl(t2.att110, t1.att110)
        end
    ),
    att111 =(
        case
            when t2.att111::VARCHAR = '-11111' then NULL
            else nvl(t2.att111, t1.att111)
        end
    ),
    att112 =(
        case
            when t2.att112::VARCHAR = '-11111' then NULL
            else nvl(t2.att112, t1.att112)
        end
    ),
    att113 =(
        case
            when t2.att113::VARCHAR = '-11111' then NULL
            else nvl(t2.att113, t1.att113)
        end
    ),
    att114 =(
        case
            when t2.att114::VARCHAR = '-11111' then NULL
            else nvl(t2.att114, t1.att114)
        end
    ),
    att115 =(
        case
            when t2.att115::VARCHAR = '-11111' then NULL
            else nvl(t2.att115, t1.att115)
        end
    ),
    att116 =(
        case
            when t2.att116::VARCHAR = '-11111' then NULL
            else nvl(t2.att116, t1.att116)
        end
    ),
    att117 =(
        case
            when t2.att117::VARCHAR = '-11111' then NULL
            else nvl(t2.att117, t1.att117)
        end
    ),
    att118 =(
        case
            when t2.att118::VARCHAR = '-11111' then NULL
            else nvl(t2.att118, t1.att118)
        end
    ),
    att119 =(
        case
            when t2.att119::VARCHAR = '-11111' then NULL
            else nvl(t2.att119, t1.att119)
        end
    ),
    att120 =(
        case
            when t2.att120::VARCHAR = '-11111' then NULL
            else nvl(t2.att120, t1.att120)
        end
    ),
    att121 =(
        case
            when t2.att121::VARCHAR = '-11111' then NULL
            else nvl(t2.att121, t1.att121)
        end
    ),
    att122 =(
        case
            when t2.att122::VARCHAR = '-11111' then NULL
            else nvl(t2.att122, t1.att122)
        end
    ),
    att20 =(
        case
            when t2.att20::VARCHAR = '-11111' then NULL
            else nvl(t2.att20, t1.att20)
        end
    ),
    att124 =(
        case
            when t2.att124::VARCHAR = '-11111' then NULL
            else nvl(t2.att124, t1.att124)
        end
    ),
    att125 =(
        case
            when t2.att125::VARCHAR = '-11111' then NULL
            else nvl(t2.att125, t1.att125)
        end
    ),
    att127 =(
        case
            when t2.att127::VARCHAR = '-11111' then NULL
            else nvl(t2.att127, t1.att127)
        end
    ),
    att128 =(
        case
            when t2.att128::VARCHAR = '-11111' then NULL
            else nvl(t2.att128, t1.att128)
        end
    ),
    att130 =(
        case
            when t2.att130::VARCHAR = '-11111' then NULL
            else nvl(t2.att130, t1.att130)
        end
    ),
    att131 =(
        case
            when t2.att131::VARCHAR = '-11111' then NULL
            else nvl(t2.att131, t1.att131)
        end
    ),
    att149 =(
        case
            when t2.att149::VARCHAR = '-11111' then NULL
            else nvl(t2.att149, t1.att149)
        end
    ),
    att150 =(
        case
            when t2.att150::VARCHAR = '-11111' then NULL
            else nvl(t2.att150, t1.att150)
        end
    ),
    att151 =(
        case
            when t2.att151::VARCHAR = '-11111' then NULL
            else nvl(t2.att151, t1.att151)
        end
    ),
    att152 =(
        case
            when t2.att152::VARCHAR = '-11111' then NULL
            else nvl(t2.att152, t1.att152)
        end
    ),
    att154 =(
        case
            when t2.att154::VARCHAR = '-11111' then NULL
            else nvl(t2.att154, t1.att154)
        end
    ),
    att155 =(
        case
            when t2.att155::VARCHAR = '-11111' then NULL
            else nvl(t2.att155, t1.att155)
        end
    ),
    att157 =(
        case
            when t2.att157::VARCHAR = '-11111' then NULL
            else nvl(t2.att157, t1.att157)
        end
    ),
    att158 =(
        case
            when t2.att158::VARCHAR = '-11111' then NULL
            else nvl(t2.att158, t1.att158)
        end
    ),
    att178 =(
        case
            when t2.att178::VARCHAR = '-11111' then NULL
            else nvl(t2.att178, t1.att178)
        end
    ),
    att179 =(
        case
            when t2.att179::VARCHAR = '-11111' then NULL
            else nvl(t2.att179, t1.att179)
        end
    ),
    att180 =(
        case
            when t2.att180::VARCHAR = '-11111' then NULL
            else nvl(t2.att180, t1.att180)
        end
    ),
    att181 =(
        case
            when t2.att181::VARCHAR = '-11111' then NULL
            else nvl(t2.att181, t1.att181)
        end
    ),
    att208 =(
        case
            when t2.att208::VARCHAR = '-11111' then NULL
            else nvl(t2.att208, t1.att208)
        end
    ),
    att209 =(
        case
            when t2.att209::VARCHAR = '-11111' then NULL
            else nvl(t2.att209, t1.att209)
        end
    ),
    att210 =(
        case
            when t2.att210::VARCHAR = '-11111' then NULL
            else nvl(t2.att210, t1.att210)
        end
    ),
    att211 =(
        case
            when t2.att211::VARCHAR = '-11111' then NULL
            else nvl(t2.att211, t1.att211)
        end
    ),
    att212 =(
        case
            when t2.att212::VARCHAR = '-11111' then NULL
            else nvl(t2.att212, t1.att212)
        end
    ),
    att213 =(
        case
            when t2.att213::VARCHAR = '-11111' then NULL
            else nvl(t2.att213, t1.att213)
        end
    ),
    att214 =(
        case
            when t2.att214::VARCHAR = '-11111' then NULL
            else nvl(t2.att214, t1.att214)
        end
    ),
    att215 =(
        case
            when t2.att215::VARCHAR = '-11111' then NULL
            else nvl(t2.att215, t1.att215)
        end
    ),
    att227 =(
        case
            when t2.att227::VARCHAR = '-11111' then NULL
            else nvl(t2.att227, t1.att227)
        end
    ),
    att301 =(
        case
            when t2.att301::VARCHAR = '-11111' then NULL
            else nvl(t2.att301, t1.att301)
        end
    ),
    att380 =(
        case
            when t2.att380::VARCHAR = '-11111' then NULL
            else nvl(t2.att380, t1.att380)
        end
    ),
    att381 =(
        case
            when t2.att381::VARCHAR = '-11111' then NULL
            else nvl(t2.att381, t1.att381)
        end
    ),
    att391 =(
        case
            when t2.att391::VARCHAR = '-11111' then NULL
            else nvl(t2.att391, t1.att391)
        end
    ),
    att439 =(
        case
            when t2.att439::VARCHAR = '-11111' then NULL
            else nvl(t2.att439, t1.att439)
        end
    ),
    att453 =(
        case
            when t2.att453::VARCHAR = '-11111' then NULL
            else nvl(t2.att453, t1.att453)
        end
    ),
    att469 =(
        case
            when t2.att469::VARCHAR = '-11111' then NULL
            else nvl(t2.att469, t1.att469)
        end
    ),
    att472 =(
        case
            when t2.att472::VARCHAR = '-11111' then NULL
            else nvl(t2.att472, t1.att472)
        end
    ),
    att504 =(
        case
            when t2.att504::VARCHAR = '-11111' then NULL
            else nvl(t2.att504, t1.att504)
        end
    ),
    att582 =(
        case
            when t2.att582::VARCHAR = '-11111' then NULL
            else nvl(t2.att582, t1.att582)
        end
    ),
    att584 =(
        case
            when t2.att584::VARCHAR = '-11111' then NULL
            else nvl(t2.att584, t1.att584)
        end
    ),
    att583 =(
        case
            when t2.att583::VARCHAR = '-11111' then NULL
            else nvl(t2.att583, t1.att583)
        end
    ),
    att595 =(
        case
            when t2.att595::VARCHAR = '-11111' then NULL
            else nvl(t2.att595, t1.att595)
        end
    ),
    att594 =(
        case
            when t2.att594::VARCHAR = '-11111' then NULL
            else nvl(t2.att594, t1.att594)
        end
    ),
    att623 =(
        case
            when t2.att623::VARCHAR = '-11111' then NULL
            else nvl(t2.att623, t1.att623)
        end
    ),
    att628 =(
        case
            when t2.att628::VARCHAR = '-11111' then NULL
            else nvl(t2.att628, t1.att628)
        end
    ),
    att631 =(
        case
            when t2.att631::VARCHAR = '-11111' then NULL
            else nvl(t2.att631, t1.att631)
        end
    ),
    att632 =(
        case
            when t2.att632::VARCHAR = '-11111' then NULL
            else nvl(t2.att632, t1.att632)
        end
    ),
    att633 =(
        case
            when t2.att633::VARCHAR = '-11111' then NULL
            else nvl(t2.att633, t1.att633)
        end
    ),
    att634 =(
        case
            when t2.att634::VARCHAR = '-11111' then NULL
            else nvl(t2.att634, t1.att634)
        end
    ),
    att635 =(
        case
            when t2.att635::VARCHAR = '-11111' then NULL
            else nvl(t2.att635, t1.att635)
        end
    ),
    att636 =(
        case
            when t2.att636::VARCHAR = '-11111' then NULL
            else nvl(t2.att636, t1.att636)
        end
    ),
    att637 =(
        case
            when t2.att637::VARCHAR = '-11111' then NULL
            else nvl(t2.att637, t1.att637)
        end
    ),
    att638 =(
        case
            when t2.att638::VARCHAR = '-11111' then NULL
            else nvl(t2.att638, t1.att638)
        end
    ),
    att643 =(
        case
            when t2.att643::VARCHAR = '-11111' then NULL
            else nvl(t2.att643, t1.att643)
        end
    ),
    att644 =(
        case
            when t2.att644::VARCHAR = '-11111' then NULL
            else nvl(t2.att644, t1.att644)
        end
    ),
    att645 =(
        case
            when t2.att645::VARCHAR = '-11111' then NULL
            else nvl(t2.att645, t1.att645)
        end
    ),
    att665 =(
        case
            when t2.att665::VARCHAR = '-11111' then NULL
            else nvl(t2.att665, t1.att665)
        end
    ),
    att666 =(
        case
            when t2.att666::VARCHAR = '-11111' then NULL
            else nvl(t2.att666, t1.att666)
        end
    ),
    att667 =(
        case
            when t2.att667::VARCHAR = '-11111' then NULL
            else nvl(t2.att667, t1.att667)
        end
    ),
    att668 =(
        case
            when t2.att668::VARCHAR = '-11111' then NULL
            else nvl(t2.att668, t1.att668)
        end
    ),
    att669 =(
        case
            when t2.att669::VARCHAR = '-11111' then NULL
            else nvl(t2.att669, t1.att669)
        end
    ),
    att670 =(
        case
            when t2.att670::VARCHAR = '-11111' then NULL
            else nvl(t2.att670, t1.att670)
        end
    ),
    att671 =(
        case
            when t2.att671::VARCHAR = '-11111' then NULL
            else nvl(t2.att671, t1.att671)
        end
    ),
    att672 =(
        case
            when t2.att672::VARCHAR = '-11111' then NULL
            else nvl(t2.att672, t1.att672)
        end
    ),
    att673 =(
        case
            when t2.att673::VARCHAR = '-11111' then NULL
            else nvl(t2.att673, t1.att673)
        end
    ),
    att674 =(
        case
            when t2.att674::VARCHAR = '-11111' then NULL
            else nvl(t2.att674, t1.att674)
        end
    ),
    att675 =(
        case
            when t2.att675::VARCHAR = '-11111' then NULL
            else nvl(t2.att675, t1.att675)
        end
    ),
    att676 =(
        case
            when t2.att676::VARCHAR = '-11111' then NULL
            else nvl(t2.att676, t1.att676)
        end
    ),
    att677 =(
        case
            when t2.att677::VARCHAR = '-11111' then NULL
            else nvl(t2.att677, t1.att677)
        end
    ),
    att678 =(
        case
            when t2.att678::VARCHAR = '-11111' then NULL
            else nvl(t2.att678, t1.att678)
        end
    ),
    att679 =(
        case
            when t2.att679::VARCHAR = '-11111' then NULL
            else nvl(t2.att679, t1.att679)
        end
    ),
    att680 =(
        case
            when t2.att680::VARCHAR = '-11111' then NULL
            else nvl(t2.att680, t1.att680)
        end
    ),
    att681 =(
        case
            when t2.att681::VARCHAR = '-11111' then NULL
            else nvl(t2.att681, t1.att681)
        end
    ),
    att684 =(
        case
            when t2.att684::VARCHAR = '-11111' then NULL
            else nvl(t2.att684, t1.att684)
        end
    ),
    att686 =(
        case
            when t2.att686::VARCHAR = '-11111' then NULL
            else nvl(t2.att686, t1.att686)
        end
    ),
    att687 =(
        case
            when t2.att687::VARCHAR = '-11111' then NULL
            else nvl(t2.att687, t1.att687)
        end
    ),
    att689 =(
        case
            when t2.att689::VARCHAR = '-11111' then NULL
            else nvl(t2.att689, t1.att689)
        end
    ),
    att803 =(
        case
            when t2.att803::VARCHAR = '-11111' then NULL
            else nvl(t2.att803, t1.att803)
        end
    ),
    att831 =(
        case
            when t2.att831::VARCHAR = '-11111' then NULL
            else nvl(t2.att831, t1.att831)
        end
    ),
    att906 =(
        case
            when t2.att906::VARCHAR = '-11111' then NULL
            else nvl(t2.att906, t1.att906)
        end
    ),
    att932 =(
        case
            when t2.att932::VARCHAR = '-11111' then NULL
            else nvl(t2.att932, t1.att932)
        end
    ),
    att933 =(
        case
            when t2.att933::VARCHAR = '-11111' then NULL
            else nvl(t2.att933, t1.att933)
        end
    ),
    att938 =(
        case
            when t2.att938::VARCHAR = '-11111' then NULL
            else nvl(t2.att938, t1.att938)
        end
    ),
    att975 =(
        case
            when t2.att975::VARCHAR = '-11111' then NULL
            else nvl(t2.att975, t1.att975)
        end
    ),
    att992 =(
        case
            when t2.att992::VARCHAR = '-11111' then NULL
            else nvl(t2.att992, t1.att992)
        end
    ),
    att1103 =(
        case
            when t2.att1103::VARCHAR = '-11111' then NULL
            else nvl(t2.att1103, t1.att1103)
        end
    ),
    att1104 =(
        case
            when t2.att1104::VARCHAR = '-11111' then NULL
            else nvl(t2.att1104, t1.att1104)
        end
    ),
    att1124 =(
        case
            when t2.att1124::VARCHAR = '-11111' then NULL
            else nvl(t2.att1124, t1.att1124)
        end
    ),
    att1125 =(
        case
            when t2.att1125::VARCHAR = '-11111' then NULL
            else nvl(t2.att1125, t1.att1125)
        end
    ),
    att1126 =(
        case
            when t2.att1126::VARCHAR = '-11111' then NULL
            else nvl(t2.att1126, t1.att1126)
        end
    ),
    att1129 =(
        case
            when t2.att1129::VARCHAR = '-11111' then NULL
            else nvl(t2.att1129, t1.att1129)
        end
    ),
    att1130 =(
        case
            when t2.att1130::VARCHAR = '-11111' then NULL
            else nvl(t2.att1130, t1.att1130)
        end
    ),
    att1148 =(
        case
            when t2.att1148::VARCHAR = '-11111' then NULL
            else nvl(t2.att1148, t1.att1148)
        end
    ),
    att1168 =(
        case
            when t2.att1168::VARCHAR = '-11111' then NULL
            else nvl(t2.att1168, t1.att1168)
        end
    ),
    att1169 =(
        case
            when t2.att1169::VARCHAR = '-11111' then NULL
            else nvl(t2.att1169, t1.att1169)
        end
    ),
    att1198 =(
        case
            when t2.att1198::VARCHAR = '-11111' then NULL
            else nvl(t2.att1198, t1.att1198)
        end
    ),
    att1199 =(
        case
            when t2.att1199::VARCHAR = '-11111' then NULL
            else nvl(t2.att1199, t1.att1199)
        end
    ),
    att1201 =(
        case
            when t2.att1201::VARCHAR = '-11111' then NULL
            else nvl(t2.att1201, t1.att1201)
        end
    ),
    att1202 =(
        case
            when t2.att1202::VARCHAR = '-11111' then NULL
            else nvl(t2.att1202, t1.att1202)
        end
    ),
    att1215 =(
        case
            when t2.att1215::VARCHAR = '-11111' then NULL
            else nvl(t2.att1215, t1.att1215)
        end
    ),
    att1229 =(
        case
            when t2.att1229::VARCHAR = '-11111' then NULL
            else nvl(t2.att1229, t1.att1229)
        end
    ),
    att1230 =(
        case
            when t2.att1230::VARCHAR = '-11111' then NULL
            else nvl(t2.att1230, t1.att1230)
        end
    ),
    att1248 =(
        case
            when t2.att1248::VARCHAR = '-11111' then NULL
            else nvl(t2.att1248, t1.att1248)
        end
    ),
    att1249 =(
        case
            when t2.att1249::VARCHAR = '-11111' then NULL
            else nvl(t2.att1249, t1.att1249)
        end
    ),
    att1276 =(
        case
            when t2.att1276::VARCHAR = '-11111' then NULL
            else nvl(t2.att1276, t1.att1276)
        end
    ),
    att1355 =(
        case
            when t2.att1355::VARCHAR = '-11111' then NULL
            else nvl(t2.att1355, t1.att1355)
        end
    ),
    att1385 =(
        case
            when t2.att1385::VARCHAR = '-11111' then NULL
            else nvl(t2.att1385, t1.att1385)
        end
    ),
    att1386 =(
        case
            when t2.att1386::VARCHAR = '-11111' then NULL
            else nvl(t2.att1386, t1.att1386)
        end
    ),
    att1387 =(
        case
            when t2.att1387::VARCHAR = '-11111' then NULL
            else nvl(t2.att1387, t1.att1387)
        end
    ),
    att1388 =(
        case
            when t2.att1388::VARCHAR = '-11111' then NULL
            else nvl(t2.att1388, t1.att1388)
        end
    ),
    att1389 =(
        case
            when t2.att1389::VARCHAR = '-11111' then NULL
            else nvl(t2.att1389, t1.att1389)
        end
    ),
    att1549 =(
        case
            when t2.att1549::VARCHAR = '-11111' then NULL
            else nvl(t2.att1549, t1.att1549)
        end
    ),
    att1550 =(
        case
            when t2.att1550::VARCHAR = '-11111' then NULL
            else nvl(t2.att1550, t1.att1550)
        end
    ),
    att1576 =(
        case
            when t2.att1576::VARCHAR = '-11111' then NULL
            else nvl(t2.att1576, t1.att1576)
        end
    ),
    att1577 =(
        case
            when t2.att1577::VARCHAR = '-11111' then NULL
            else nvl(t2.att1577, t1.att1577)
        end
    ),
    att1578 =(
        case
            when t2.att1578::VARCHAR = '-11111' then NULL
            else nvl(t2.att1578, t1.att1578)
        end
    ),
    att1581 =(
        case
            when t2.att1581::VARCHAR = '-11111' then NULL
            else nvl(t2.att1581, t1.att1581)
        end
    )
    WHEN NOT MATCHED THEN
INSERT(
        cid,
        utype,
        uid,
        em,
        fk,
        mo,
        ad,
        adat,
        pc,
        blacklisted,
        att1,
        att2,
        att3,
        att6,
        att7,
        att9,
        att12,
        att14,
        att15,
        att16,
        att28,
        att29,
        att38,
        att39,
        domid,
        disabled,
        att42,
        att43,
        att44,
        att45,
        att49,
        att50,
        att62,
        att63,
        att65,
        att66,
        att67,
        att68,
        att19,
        ws,
        wadat,
        mbl,
        eadat,
        madat,
        att79,
        att81,
        att84,
        att85,
        att93,
        att94,
        att95,
        att96,
        att97,
        att98,
        att99,
        att100,
        att101,
        att102,
        att103,
        att104,
        att105,
        att106,
        att107,
        att108,
        att110,
        att111,
        att112,
        att113,
        att114,
        att115,
        att116,
        att117,
        att118,
        att119,
        att120,
        att121,
        att122,
        att20,
        att124,
        att125,
        att127,
        att128,
        att130,
        att131,
        att149,
        att150,
        att151,
        att152,
        att154,
        att155,
        att157,
        att158,
        att178,
        att179,
        att180,
        att181,
        att208,
        att209,
        att210,
        att211,
        att212,
        att213,
        att214,
        att215,
        att227,
        att301,
        att380,
        att381,
        att391,
        att439,
        att453,
        att469,
        att472,
        att504,
        att582,
        att584,
        att583,
        att595,
        att594,
        att623,
        att628,
        att631,
        att632,
        att633,
        att634,
        att635,
        att636,
        att637,
        att638,
        att643,
        att644,
        att645,
        att665,
        att666,
        att667,
        att668,
        att669,
        att670,
        att671,
        att672,
        att673,
        att674,
        att675,
        att676,
        att677,
        att678,
        att679,
        att680,
        att681,
        att684,
        att686,
        att687,
        att689,
        att803,
        att831,
        att906,
        att932,
        att933,
        att938,
        att975,
        att992,
        att1103,
        att1104,
        att1124,
        att1125,
        att1126,
        att1129,
        att1130,
        att1148,
        att1168,
        att1169,
        att1198,
        att1199,
        att1201,
        att1202,
        att1215,
        att1229,
        att1230,
        att1248,
        att1249,
        att1276,
        att1355,
        att1385,
        att1386,
        att1387,
        att1388,
        att1389,
        att1549,
        att1550,
        att1576,
        att1577,
        att1578,
        att1581
    )
VALUES(
        t2.cid,
        t2.utype,
        t2.uid,
        t2.em,
        t2.fk,
        t2.mo,
        t2.ad,
        t2.adat,
        t2.pc,
        t2.blacklisted,
        t2.att1,
        t2.att2,
        t2.att3,
        t2.att6,
        t2.att7,
        t2.att9,
        t2.att12,
        t2.att14,
        t2.att15,
        t2.att16,
        t2.att28,
        t2.att29,
        t2.att38,
        t2.att39,
        t2.domid,
        t2.disabled,
        t2.att42,
        t2.att43,
        t2.att44,
        t2.att45,
        t2.att49,
        t2.att50,
        t2.att62,
        t2.att63,
        t2.att65,
        t2.att66,
        t2.att67,
        t2.att68,
        t2.att19,
        t2.ws,
        t2.wadat,
        t2.mbl,
        t2.eadat,
        t2.madat,
        t2.att79,
        t2.att81,
        t2.att84,
        t2.att85,
        t2.att93,
        t2.att94,
        t2.att95,
        t2.att96,
        t2.att97,
        t2.att98,
        t2.att99,
        t2.att100,
        t2.att101,
        t2.att102,
        t2.att103,
        t2.att104,
        t2.att105,
        t2.att106,
        t2.att107,
        t2.att108,
        t2.att110,
        t2.att111,
        t2.att112,
        t2.att113,
        t2.att114,
        t2.att115,
        t2.att116,
        t2.att117,
        t2.att118,
        t2.att119,
        t2.att120,
        t2.att121,
        t2.att122,
        t2.att20,
        t2.att124,
        t2.att125,
        t2.att127,
        t2.att128,
        t2.att130,
        t2.att131,
        t2.att149,
        t2.att150,
        t2.att151,
        t2.att152,
        t2.att154,
        t2.att155,
        t2.att157,
        t2.att158,
        t2.att178,
        t2.att179,
        t2.att180,
        t2.att181,
        t2.att208,
        t2.att209,
        t2.att210,
        t2.att211,
        t2.att212,
        t2.att213,
        t2.att214,
        t2.att215,
        t2.att227,
        t2.att301,
        t2.att380,
        t2.att381,
        t2.att391,
        t2.att439,
        t2.att453,
        t2.att469,
        t2.att472,
        t2.att504,
        t2.att582,
        t2.att584,
        t2.att583,
        t2.att595,
        t2.att594,
        t2.att623,
        t2.att628,
        t2.att631,
        t2.att632,
        t2.att633,
        t2.att634,
        t2.att635,
        t2.att636,
        t2.att637,
        t2.att638,
        t2.att643,
        t2.att644,
        t2.att645,
        t2.att665,
        t2.att666,
        t2.att667,
        t2.att668,
        t2.att669,
        t2.att670,
        t2.att671,
        t2.att672,
        t2.att673,
        t2.att674,
        t2.att675,
        t2.att676,
        t2.att677,
        t2.att678,
        t2.att679,
        t2.att680,
        t2.att681,
        t2.att684,
        t2.att686,
        t2.att687,
        t2.att689,
        t2.att803,
        t2.att831,
        t2.att906,
        t2.att932,
        t2.att933,
        t2.att938,
        t2.att975,
        t2.att992,
        t2.att1103,
        t2.att1104,
        t2.att1124,
        t2.att1125,
        t2.att1126,
        t2.att1129,
        t2.att1130,
        t2.att1148,
        t2.att1168,
        t2.att1169,
        t2.att1198,
        t2.att1199,
        t2.att1201,
        t2.att1202,
        t2.att1215,
        t2.att1229,
        t2.att1230,
        t2.att1248,
        t2.att1249,
        t2.att1276,
        t2.att1355,
        t2.att1385,
        t2.att1386,
        t2.att1387,
        t2.att1388,
        t2.att1389,
        t2.att1549,
        t2.att1550,
        t2.att1576,
        t2.att1577,
        t2.att1578,
        t2.att1581
    );
	`

	elapsed, rows := execute_unify_merge(q_merge)
	return elapsed, rows
}

func unify_merge(wg *sync.WaitGroup) {

	defer wg.Done()

	durations := []time.Duration{}

	prev := 0
	// prev := (time.Time{})
	x := GetMaxEpoch()

	for { // prev != x || x == 0
		if x != 0 && prev != x {
		// if x != (time.Time{}) && prev != x {
			start_end := time.Now()

			elapsed, cnt := unify(prev, x)

			unifyLog := fmt.Sprintf("[UNIFY]\t[time:%s]\t[args{start:%d, end:%d, rows_inserted:%d}]", elapsed, prev, x, cnt)
			log.Print(unifyLog)

			elapsed, cnt = merge()

			mergeLog := fmt.Sprintf("[UNIFY]\t[time:%s]\t[args{start:%d, end:%d, rows_inserted:%d}]", elapsed, prev, x, cnt)
			log.Print(mergeLog)

			elapsed = time.Since(start_end)

			durations = append(durations, elapsed)

			selectLog := fmt.Sprintf("[UNIFY_MERGE]\t[time:%s]\t]", elapsed)
			log.Print(selectLog)

			execute("truncate table crux_72994.tmp2_userdetailsattrs")

			fmt.Println("prev, x : ", prev, x)
		} else {
			time.Sleep(1 * time.Second)

			if len(durations) != 0 {
				sort.Slice(durations, func(i, j int) bool {
					return durations[i] < durations[j]
				})

				min := durations[0]

				max := durations[len(durations)-1]

				var total time.Duration
				for _, duration := range durations {
					total += duration
				}
				avg := total / time.Duration(len(durations))

				endLog := fmt.Sprintf("[END:UNIFY_MERGE]\t[total_time:%s]\t[args:{min:%s, max:%s, avg:%s}]", total, min, max, avg)
				log.Printf(endLog)
			}
		}
		prev = x
		x = GetMaxEpoch()
	}
}

func execute_unify_merge(q string) (time.Duration, int) {

	start_time := time.Now()

	copyResult, err := db.Exec(q)

	elapsed := time.Since(start_time)

	if err != nil {
		fmt.Println("error executing query", err)
	}
	copyCount, err := copyResult.RowsAffected()
	if err != nil {
		fmt.Println("error in copyResult.RowsAffected", err)
	}

	if err != nil {
		fmt.Println("error while executing : ", err)
	}

	return elapsed, int(copyCount)
}