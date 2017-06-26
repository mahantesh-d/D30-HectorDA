package postgresxl

import (
	"github.com/dminGod/D30-HectorDA/model"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"time"

	"github.com/dminGod/D30-HectorDA/logger"
	"strings"

	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/endpoint/cassandra"
	"github.com/dminGod/test/D30-HectorDA/endpoint/postgresql"
)

//var prestgresqlChan chan *sql.DB


func Handle(dbAbstract *model.DBAbstract, secondQuery *model.DBAbstract, processSecond bool) (int) {

	rowsAffected := 0

	if dbAbstract.QueryType == "INSERT" {

		Insert(dbAbstract, secondQuery, processSecond)

	} else if dbAbstract.QueryType == "DELETE_INSERT" {

		DeleteInsert(dbAbstract, secondQuery, processSecond)

	} else if dbAbstract.QueryType == "SELECT" {

		Select(dbAbstract)

	} else if dbAbstract.QueryType == "UPDATE" {

		rowsAffected = Update(dbAbstract, secondQuery, processSecond)

	} else if dbAbstract.QueryType == "DELETE" {

		Delete(dbAbstract)
	}

	return rowsAffected
}

var dbpool *sql.DB

func init() {
	Conf := config.Get()

	dbName := Conf.Postgresxl.Database
	dbUser := Conf.Postgresxl.Username
	dbPass := Conf.Postgresxl.Password
	dbHost := Conf.Postgresxl.Host
	dbPort := Conf.Postgresxl.Port

	var dbInfo string

	if len(dbPass) == 0 {

		dbInfo = fmt.Sprintf("user=%s dbname=%s sslmode=disable host=%s port=%s",
			dbUser, dbName, dbHost, dbPort)
	} else {

		dbInfo = fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable host=%s port=%s",
			dbUser, dbPass, dbName, dbHost, dbPort)
	}

	dbpool, _ = sql.Open("postgres", dbInfo)

	dbpool.SetMaxOpenConns(20)
	dbpool.SetMaxIdleConns(10)
	dbpool.SetConnMaxLifetime(3 * time.Minute)

}

func getConnection() (*sql.DB, error) {

	return dbpool, nil
}

func closeConnection(sql *sql.DB) {
	//   sql.Close()

}

func Insert(dbAbstract *model.DBAbstract, secondAbs *model.DBAbstract, processSecond bool) {

	connection, err := getConnection()

	if err != nil {

		logger.Write("ERROR", err.Error())

		if err != nil {

			logger.Write("ERROR", err.Error())

			dbAbstract.Status = "fail"
			dbAbstract.Message = err.Error()
			dbAbstract.Data = "{}"
			return
		}
	}

	var error_messages []string

	if processSecond {

		tx, _ := connection.Begin()

		tx.Exec(dbAbstract.Query[0])

		cassandra.Handle(secondAbs)

		logger.Write("INFO", "Ran the Cassandra extra insert from PG Insert object is:", secondAbs)
		if secondAbs.Status == "success" {

			err := tx.Commit()

			if err != nil {

				logger.Write("ERROR", "Postgres transaction failed with error, " + err.Error())
				dbAbstract.Status = "fail"
				dbAbstract.Message = "Insert Transaction failed"
				dbAbstract.Data = "{}"
			} else {

				logger.Write("INFO", "Transaction Commited, insert successful")
			}


		} else {

			tx.Rollback()

			logger.Write("ERROR", "Cassandra insert did not go through, Rolling back commit")
			dbAbstract.Status = "fail"
			dbAbstract.Message = "The insert request failed."
			dbAbstract.Data = "{}"
			return
		}

	} else {

		_, err = connection.Exec(dbAbstract.Query[0])

	}

	var success_count uint64
	logger.Write("DEBUG", "Running Queries for insert start : num of queries to run " + string(len(dbAbstract.Query)))
	logger.Write("DEBUG", "Insert Record successfully")

	if err != nil {
		logger.Write("ERROR", "Query from set failed - Query : '" + dbAbstract.Query[0] + "' - Error : " + err.Error())
		error_messages = append(error_messages, "Query from set failed - Query : '" + dbAbstract.Query[0] + "' - Error : " + err.Error())
		// logger.Write("INFO","Execution Query"+","+ single_query)
	} else {
		success_count += 1
	}

	if len(error_messages) > 0 {

		// Error response text
		response_text := string(len(error_messages)) + " Out of " + string(len(dbAbstract.Query)) + " Had the following errors \n"
		response_text += strings.Join(error_messages, " \n")

		logger.Write("ERROR", response_text)
		dbAbstract.Status = "fail"
		dbAbstract.Message = response_text
		dbAbstract.Data = "{}"

	} else {
		logger.Write("INFO", "Inserted successfully")
		dbAbstract.Status = "success"
		dbAbstract.Message = "Inserted successfully"
		dbAbstract.Data = "{}"
		dbAbstract.Count = success_count

	}

	closeConnection(connection)

}

func DeleteInsert(dbAbstract *model.DBAbstract, secondAbs *model.DBAbstract, processSecond bool) {

	connection, err := getConnection()

	if err != nil {

		logger.Write("ERROR", err.Error())

		if err != nil {

			logger.Write("ERROR", err.Error())

			dbAbstract.Status = "fail"
			dbAbstract.Message = err.Error()
			dbAbstract.Data = "{}"
			return
		}
	}

	var error_messages []string

	if len(secondAbs.Query) > 0 {

		postgresxl.Handle(secondAbs)

		logger.Write("INFO", "Ran the Postgres delete for insert of overwite record :", secondAbs)
		logger.Write("INFO", "secondAbs is", secondAbs)


//		if secondAbs.Status == "success" {

			_, err = connection.Exec(dbAbstract.Query[0])

			if err != nil {

				logger.Write("ERROR", "Postgres Query failed with error, " + err.Error())
				dbAbstract.Status = "fail"
				dbAbstract.Message = "The insert request failed"
				dbAbstract.Data = "{}"
			} else {

				logger.Write("INFO", "Insert successful")
			}

//		} else {

			//logger.Write("ERROR", "Insert did not go through")
			//dbAbstract.Status = "fail"
			//dbAbstract.Message = "The insert request failed."
			//dbAbstract.Data = "{}"
			//return
//		}

	} else {

		_, err = connection.Exec(dbAbstract.Query[0])

	}

	var success_count uint64
	logger.Write("DEBUG", "Running Queries for insert start : num of queries to run " + string(len(dbAbstract.Query)))
	logger.Write("DEBUG", "Insert Record successfully")

	if err != nil {
		logger.Write("ERROR", "Query from set failed - Query : '" + dbAbstract.Query[0] + "' - Error : " + err.Error())
		error_messages = append(error_messages, "Query from set failed - Query : '" + dbAbstract.Query[0] + "' - Error : " + err.Error())
		// logger.Write("INFO","Execution Query"+","+ single_query)
	} else {
		success_count += 1
	}

	if len(error_messages) > 0 {

		// Error response text
		response_text := string(len(error_messages)) + " Out of " + string(len(dbAbstract.Query)) + " Had the following errors \n"
		response_text += strings.Join(error_messages, " \n")

		logger.Write("ERROR", response_text)
		dbAbstract.Status = "fail"
		dbAbstract.Message = response_text
		dbAbstract.Data = "{}"

	} else {
		logger.Write("INFO", "Inserted successfully")
		dbAbstract.Status = "success"
		dbAbstract.Message = "Inserted successfully"
		dbAbstract.Data = "{}"
		dbAbstract.Count = success_count

	}

	closeConnection(connection)

}

func Select(dbAbstract *model.DBAbstract) {
	var prestoResult []map[string]interface{}

	db, err := getConnection()

	if err != nil {

		logger.Write("ERROR", "Error in got connection, " + err.Error())

		dbAbstract.Status = "fail"
		dbAbstract.Message = "Error at in getting connection"
		dbAbstract.Data = "{}"
		dbAbstract.Count = 0

		return
	}

	logger.Write("INFO", "Running Postgres Query" + dbAbstract.Query[0])

	rows, err := db.Query(dbAbstract.Query[0])

	if err != nil {

		logger.Write("ERROR", "Error in running query, PG" + err.Error())

		dbAbstract.Status = "fail"
		dbAbstract.Message = "Error at in running query"
		dbAbstract.Data = "{}"
		dbAbstract.Count = 0

		return
	}

	cols, err := rows.Columns()

	if err != nil {

		logger.Write("ERROR", "Postgresxl select query problem after trying to get columns. -->" + err.Error())
		dbAbstract.Status = "fail"
		dbAbstract.Message = "Error at retriving data"
		dbAbstract.Data = "{}"
		dbAbstract.Count = 0
		return
	}

	data := make([]interface{}, len(cols))
	args := make([]interface{}, len(data))

	for i := range data {
		args[i] = &data[i]
	}

	for rows.Next() {

		var rowData = make(map[string]interface{})

		if err := rows.Scan(args...); err != nil {
			logger.Write("ERROR", "An Error occurred while scanning results : " + err.Error())
			return
		}

		for i := range data {

			rowData[ cols[i] ] = data[i]
		}

		prestoResult = append(prestoResult, rowData)

	}
	if err != nil {
		dbAbstract.Status = "fail"
		dbAbstract.Message = "Error connecting to endpoint"
		dbAbstract.Data = "{}"
		dbAbstract.Count = 0
		return
	} else {
		dbAbstract.Status = "success"
		dbAbstract.Message = "Select successful"
		dbAbstract.Data = utils.EncodeJSON(prestoResult)
		dbAbstract.RichData = prestoResult
		dbAbstract.Count = uint64(len(prestoResult))

	}

	checkErros(err)

	closeConnection(db)

}

func Update(dbAbstract *model.DBAbstract, secondAbs *model.DBAbstract, processSecond bool) (int) {

	db, err := getConnection()

	rowsAffected := 0




	if err != nil {

		logger.Write("ERROR", err.Error())
		dbAbstract.Status = "fail"
		dbAbstract.Message = err.Error()
		dbAbstract.Data = "{}"

		return 0
	}

	if processSecond {

		tx, _ := db.Begin()

		tx.Exec(dbAbstract.Query[0])

		cassandra.Handle(secondAbs)

		logger.Write("INFO", "Ran the Cassandra extra insert from PG Insert object is:", secondAbs)
		if secondAbs.Status == "success" {

			err := tx.Commit()

			if err != nil {

				logger.Write("ERROR", "Postgres transaction failed with error, " + err.Error())
				dbAbstract.Status = "fail"
				dbAbstract.Message = "Insert Transaction failed"
				dbAbstract.Data = "{}"
			} else {

				logger.Write("INFO", "Transaction Commited, insert successful")
			}


		} else {

			tx.Rollback()

			logger.Write("ERROR", "Cassandra insert did not go through, Rolling back commit" + secondAbs.Message)
			dbAbstract.Status = "fail"
			dbAbstract.Message = "Insert Transaction failed"
			dbAbstract.Data = "{}"
			return 0
		}

	} else {

		res, err := db.Exec(dbAbstract.Query[0])

		if err != nil {

			logger.Write("ERROR", err.Error())
			dbAbstract.Status = "fail"
			dbAbstract.Message = err.Error()
			dbAbstract.Data = "{}"

			return 0
		}


		rowsAffected64, _ := res.RowsAffected()
		rowsAffected = int(rowsAffected64)
	}

	var success_count uint64
	error_messages := []string{}

	if err != nil {

		logger.Write("ERROR", err.Error())
		dbAbstract.Status = "fail"
		dbAbstract.Message = err.Error()
		dbAbstract.Data = "{}"

		return 0

	} else {
		success_count += 1
	}

	if len(error_messages) > 0 {

		// Error response text
		response_text := string(len(error_messages)) + " Out of " + string(len(dbAbstract.Query)) + " Had the following errors \n"
		response_text += strings.Join(error_messages, " \n")

		logger.Write("ERROR", response_text)
		dbAbstract.Status = "fail"
		dbAbstract.Message = response_text
		dbAbstract.Data = "{}"
	} else {
		logger.Write("INFO", "Update successful")
		dbAbstract.Status = "success"
		dbAbstract.Message = "Update successful"
		dbAbstract.Data = "{}"
		dbAbstract.Count = success_count

	}

	closeConnection(db)

	return rowsAffected
}

func Delete(dbAbstract *model.DBAbstract) {

	db, err := getConnection()

	if err != nil {

		logger.Write("ERROR", err.Error())

		dbAbstract.Status = "fail"
		dbAbstract.Message = err.Error()
		dbAbstract.Data = "{}"
		return

	}

	_, err = db.Exec(dbAbstract.Query[0])

	if err != nil {

		logger.Write("ERROR", err.Error())

		dbAbstract.Status = "fail"
		dbAbstract.Message = err.Error()
		dbAbstract.Data = "{}"
		return

	} else {


		dbAbstract.Status = "success"
		dbAbstract.Message = "Delete executed successfully"
		dbAbstract.Data = "{}"
		return
	}

	closeConnection(db)
}

func checkErros(err error) {
	if err != nil {
		panic(err)
	}
}