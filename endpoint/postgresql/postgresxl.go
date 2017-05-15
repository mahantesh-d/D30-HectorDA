package postgresxl

import ("github.com/dminGod/D30-HectorDA/model"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"time"

	"github.com/dminGod/D30-HectorDA/logger"
	"strings"

	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/dminGod/D30-HectorDA/config"
)

//var prestgresqlChan chan *sql.DB

func Handle(dbAbstract *model.DBAbstract) {

	if dbAbstract.QueryType == "INSERT" {
		Insert(dbAbstract)
	} else if dbAbstract.QueryType == "SELECT" {
		Select(dbAbstract)
	} else if dbAbstract.QueryType == "UPDATE"{
		Update(dbAbstract)
	}else if dbAbstract.QueryType == "DELETE"{
		Delete(dbAbstract)
	}

}

func getConnection() (*sql.DB) {

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

	db, err := sql.Open("postgres", dbInfo)

	if err != nil {

		logger.Write("ERROR", "Trouble connecting to database Postgres Error : " + err.Error())
	}

	return db
}

func closeConnection(sql *sql.DB)  {
       sql.Close()
}
func Insert(dbAbstract *model.DBAbstract) {
	connection:= getConnection()
	connection.Begin()
	 var error_messages []string
	 row,err:= connection.Query(dbAbstract.Query[0])
	 var success_count uint64
	 logger.Write("DEBUG", "Running Queries for insert start : num of queries to run "+string(len(dbAbstract.Query)))
	 logger.Write("DEBUG","Insert Record successfully")
	 fmt.Print(row)
	if err != nil {
		//logger.Write("ERROR", "Query from set failed - Query : '"+single_query+"' - Error : "+err.Error())
		//error_messages = append(error_messages, "Query from set failed - Query : '"+single_query+"' - Error : "+err.Error())
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
		dbAbstract.Count=success_count

	}


	go closeConnection(connection)

}
func Select(dbAbstract *model.DBAbstract) {
	var prestoResult []map[string]interface{}

	db := getConnection()
	db.Begin()
	logger.Write("INFO", "Running Postgres Query" + dbAbstract.Query[0])
	rows, err := db.Query(dbAbstract.Query[0])

	if err != nil {

		fmt.Println(err)
	}

	fmt.Println("Rows is ", rows)
	cols, err := rows.Columns()

	if err != nil {

		logger.Write("ERROR", "Postgresxl select query problem after trying to get columns. -->" + err.Error())
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
		}

		for i := range data {

			rowData[ cols[i] ] = data[i]
		}

		prestoResult = append(prestoResult, rowData)

	}
	 if err!=nil {
		 dbAbstract.Status = "fail"
		 dbAbstract.Message = "Error connecting to endpoint"
		 dbAbstract.Data = "{}"
		 dbAbstract.Count = 0

	   }else{
		 dbAbstract.Status = "success"
		 dbAbstract.Message = "Select successful"
		 dbAbstract.Data = utils.EncodeJSON(prestoResult)
		 dbAbstract.RichData = prestoResult
		 dbAbstract.Count = uint64(len(prestoResult))

	}
	checkErros(err)
	defer db.Close()

}
func Update(dbAbstract *model.DBAbstract)  {
	db:=getConnection()
	db.Begin()
	data,err:=db.Query(dbAbstract.Query[0])
	var success_count uint64
	error_messages:=[]string{}
	checkErros(err)
	defer db.Close()
	if err != nil {
		//logger.Write("ERROR", "Query from set failed - Query : '"+single_query+"' - Error : "+err.Error())
		//error_messages = append(error_messages, "Query from set failed - Query : '"+single_query+"' - Error : "+err.Error())
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
		dbAbstract.Count=success_count

	}

	for data.Next() {
		var uid int
		var username string
		var department string
		var created time.Time
		err:= data.Scan(&uid, &username, &department, &created)
		checkErros(err)
		fmt.Println("uid | username | department | created ")
		fmt.Printf("%3v | %8v | %6v | %6v\n", uid, username, department, created)
	}
}
func Delete(dbAbstract *model.DBAbstract)  {
	db:=getConnection()
	db.Begin()
	data,err:=db.Query(dbAbstract.Query[0])
	fmt.Println(data)
	checkErros(err)
	defer db.Close()
	for data.Next() {
		var uid int
		var username string
		var department string
		var created time.Time
		err:= data.Scan(&uid, &username, &department, &created)
		checkErros(err)
		fmt.Println("uid | username | department | created ")
		fmt.Printf("%3v | %8v | %6v | %6v\n", uid, username, department, created)
	}

}
func checkErros(err error)  {
     if err!=nil {
	     panic(err)
     }
}