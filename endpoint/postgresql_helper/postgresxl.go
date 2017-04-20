package postgresql_helper

import "fmt"
/**

Schema:
CREATE TABLE userinfo
(
    uid serial NOT NULL,
    username character varying(100) NOT NULL,
    departname character varying(500) NOT NULL,
    Created date,
    CONSTRAINT userinfo_pkey PRIMARY KEY (uid)
)
WITH (OIDS=FALSE);


 */
func UpdateQueryBuilder(metaInput map[string]interface{}) string{
   var query string
	 fmt.Println(metaInput)
          query="UPDATE USERINFO SET username='indian' where uid=3";

	return query
}
func DeleteQueryBuilder(metaInput map[string]interface{})  string {
     var query string
        fmt.Print(metaInput)
	query="DELETE FROM userinfo WHERE uid=7"
	return query
}