package fcsutility

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

const (
	Statement = "1"
	Detail    = "2"
	Panic     = "P"
	NoPanic   = "NP"
)

type Database struct {
	Server   string
	Port     string
	Database string
	User     string
	Password string
}

type Behaviour struct {
	Debug        string
	DebugLevel   string
	DisplayDebug string
	ProgramId    string
	ProgramUser  string
}

//--------------------------------------------------------------------
// function establishes DB connection
//--------------------------------------------------------------------
func Getdb(dbtype string, dbdtl Database) *sql.DB {
	// Connect to database
	connString := ""
	if dbtype == "mssql" {
		connString = fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;",
			dbdtl.Server, dbdtl.User, dbdtl.Password, dbdtl.Port, dbdtl.Database)
	} else if dbtype == "mysql" {
		// Connect to database
		connString = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", dbdtl.User, dbdtl.Password, dbdtl.Server, dbdtl.Port, dbdtl.Database)
	}
	db, err := sql.Open(dbtype, connString)
	if err != nil {
		fmt.Println("Error Connecting to DB %s\n", err.Error())
		return db
	}
	log.Println("DB Connected!\n")

	return db
}

//--------------------------------------------------------------------
// function to insert debug message into db
//--------------------------------------------------------------------
func Debug(db *sql.DB, defaultdisp string, debug string, debuglevel string, pdebuglevel string, programid string, program string, msg string) {
	if defaultdisp == "Y" {
		log.Println(msg)
	}
	vn_debuglevel, _ := strconv.Atoi(debuglevel)
	vn_pdebuglevel, _ := strconv.Atoi(pdebuglevel)

	if debug == "Y" && vn_debuglevel >= vn_pdebuglevel {
		sqlString := "insert into debugtbl(debugtime,programid,programName,debugmsg) values(GETDATE(),'" + programid + "','" + program + "','" + msg + "')"
		_, inserterr := db.Exec(sqlString)
		if inserterr != nil {
			LogError(Panic, inserterr.Error())
		}
	}

}

//--------------------------------------------------------------------
// function to insert debug message into db
//--------------------------------------------------------------------
func Error(db *sql.DB, programid string, program string, msg string) {
	sqlString := "insert into errortbl(errortime,programid,programName,errormsg) values(GETDATE(),'" + programid + "','" + program + "','" + msg + "')"
	_, inserterr := db.Exec(sqlString)
	if inserterr != nil {
		LogError(Panic, inserterr.Error())
	} else {
		LogError(Panic, program+" "+msg)
	}

}

//--------------------------------------------------------------------
// function to log error
//--------------------------------------------------------------------
func LogError(logtype string, msg interface{}) {
	log.Println(msg)
	if logtype == "P" {
		log.Print("Press 'Enter' to exit...")
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		panic(msg)
	}
}

//--------------------------------------------------------------------
// function to get value from core setting for a given Key
//--------------------------------------------------------------------
func GetCoreSettingValue(db *sql.DB, key string) string {
	var value string
	sqlString := "select value from CoreSettings where [key] ='" + key + "'"
	rows, err := db.Query(sqlString)
	if err != nil {
		LogError("P", err.Error())
	}
	for rows.Next() {

		err := rows.Scan(&value)
		if err != nil {
			LogError("P", err)
		}

	}
	return value
}

//--------------------------------------------------------------------
// function return nil when the given string is blank or it will
// retrun the actual string
// main usage of this function is to avoid inserting/update 1900-01-01
// date for a date column in database for values that are blank
//--------------------------------------------------------------------

func ReturnNil(s string) interface{} {
	if s == "" {
		return nil
	} else {
		return s
	}
}

//--------------------------------------------------------------------
// function to get system hour
//--------------------------------------------------------------------
func GetCurrentHr() string {
	dt := time.Now()
	return dt.Format("15")
}
