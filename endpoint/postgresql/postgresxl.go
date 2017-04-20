package postgresxl

import ("github.com/dminGod/D30-HectorDA/model"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"time"

)


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

const (
	DB_USER     = "postgres"
	DB_PASSWORD = "redhat"
	DB_NAME     = "postgres"
)


func Insert(dbAbstract *model.DBAbstract) {
	//comman logic do nt write here
	dbinfo:= fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		DB_USER, DB_PASSWORD, DB_NAME)
	   db,_:=sql.Open("postgres", dbinfo)
	defer db.Close()
	   db.Begin()
	var numberOfRow int
	  data,err:= db.Query(dbAbstract.Query[0])
	fmt.Println(data.Scan(&numberOfRow))
         checkErros(err)

}
func Select(dbAbstract *model.DBAbstract) {

	//get Config from Config.toml
	// write ones we can reusable
	dbinfo:= fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		DB_USER, DB_PASSWORD, DB_NAME)
	   db,_:= sql.Open("postgres", dbinfo)
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
func Update(dbAbstract *model.DBAbstract)  {
	//get Config from Config.toml
	// write ones we can reusable
	dbinfo:= fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		DB_USER, DB_PASSWORD, DB_NAME)
	db,_:= sql.Open("postgres", dbinfo)
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
func Delete(dbAbstract *model.DBAbstract)  {
	dbinfo:= fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		DB_USER, DB_PASSWORD, DB_NAME)
	db,_:= sql.Open("postgres", dbinfo)
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