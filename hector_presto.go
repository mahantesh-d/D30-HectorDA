package main

//
//import (
////	"database/sql"
////	"fmt"
////	"log"
//
//
////	_ "github.com/avct/prestgo"
//	// "github.com/dminGod/D30-HectorDA/endpoint/presto"
//
//	// "os"
//	// "fmt"
//)
//import (
//	"os"
//	"fmt"
//)
//
//
//// var kkk []map[string]interface{}
//
//
//
//func main() {
//
////	presto.Handle()
//
//
//}
//
//
//
//
//
////func main() {
////	db, err := sql.Open("prestgo", "presto://10.138.32.26:8080/cassandra/ais_test_all")
////
////	if err != nil {
////
////		log.Fatalf("failed to connect to presto: %v", err)
////	}
////
//////	rows, err := db.Query("SELECT * FROM obtain_detail LIMIT 5")
////	rows, err := db.Query("SELECT * from obtain_detail LIMIT 10")
////
////	if err != nil {
////		AppExit
////		fatal(fmt.Sprintf("failed query presto: %v", err))
////	}
////
////
////
////
////
////
////	defer rows.Close()
////
////	cols, err := rows.Columns()
////
////
////	if err != nil {
////
////
////
////		fatal(fmt.Sprintf("failed to read columns: %v", err))
////	}
////
////
////
////	if err != nil {
////
////		log.Fatalf("failed to run query: %v", err)
////	}
////
////
////	data := make([]interface{}, len(cols))
////	args := make([]interface{}, len(data))
////
////
////	for i := range data {
////		args[i] = &data[i]
////	}
////
////
////
////	for rows.Next() {
////
////		if err := rows.Scan(args...); err != nil {
////			fatal(err.Error())
////		}
////
////
////
////		for i := range data {
////
////			kkk = append(kkk, map[string]interface{}{ cols[i] : data[i] } )
////
////		}
////
////
////
////	}
////
////	fmt.Println(kkk)
////
////
////	//fmt.Println( rows.Scan()   )
////
////
////
////	//
////	//for rows.Next() {
////	//	//var name interface{}
////	//	//var name1 string
////	//	//var name2 string
////	//	//var name3 string
////	//
////	//
////	//
////	//	//if err := rows.Scan( &name ); err != nil {
////	//	//	log.Fatal(err.Error())
////	//	//}
////	//
////	//	// fmt.Printf("%s, %s, %s, %s \n", name, name1, name2, name3)
////	//}
////	//if err := rows.Err(); err != nil {
////	//	log.Fatal(err.Error())
////	//}
////}
//
//
//func fatal(msg string) {
//	fmt.Fprintln(os.Stderr, msg)
//	os.Exit(1)
//}