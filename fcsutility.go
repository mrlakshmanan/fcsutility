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
	INSERT    = "INSERT"
	UPDATE    = "UPDATE"
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
	ProgramName  string
}

type UploadMaster struct {
	Summary          string
	SourceCode       string
	IsDefault        string
	IsForceDefault   string
	ZohoDepartmentId string
	UploadFileName   string
	UploadedBy       string
	CampaignId       string
	IsCampaign       string
}

type TicketLog struct {
	Id                 string
	ClientId           string
	Description        string
	Source             string
	SummaryUser        string
	TicketStatus       string
	ZohoDepartmentId   string
	UploadDataMasterId int
	AssigneeId         string
	STCode             string
	CreatedBy          string
	CreatedProgram     string
	UpdatedBy          string
	UpdatedProgram     string
}

type EmailLogType struct {
	Action         string
	Id             string
	EmailServer    string
	Type           string
	From           string
	FromDspName    string
	To             string
	Cc             string
	Bcc            string
	ReplyTo        string
	Subject        string
	Body           string
	CreationDate   string
	SentDate       string
	Status         string
	ErrorMsg       string
	CreatedProgram string
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
		sqlString := "insert into debugtbl(debugtime,programid,programName,debugmsg) values($1,$2,$3,$4)"
		_, inserterr := db.Exec(sqlString, time.Now(), programid, program, msg)
		//sqlString := "insert into debugtbl(debugtime,programid,programName,debugmsg) values(GETDATE(),'" + programid + "','" + program + "','" + msg + "')"
		//_, inserterr := db.Exec(sqlString)
		if inserterr != nil {
			LogError(Panic, inserterr.Error())
		}
	}

}

//--------------------------------------------------------------------
// function to insert debug message into db
//--------------------------------------------------------------------
func Error(db *sql.DB, programid string, program string, msg string) {
	sqlString := "insert into errortbl(errortime,programid,programName,errormsg) values($1,$2,$3,$4)"
	_, inserterr := db.Exec(sqlString, time.Now(), programid, program, msg)
	//sqlString := "insert into errortbl(errortime,programid,programName,errormsg) values(GETDATE(),'" + programid + "','" + program + "','" + msg + "')"
	//_, inserterr := db.Exec(sqlString)
	if inserterr != nil {
		LogError(Panic, inserterr.Error())
	} else {
		LogError(Panic, program+" "+msg)
	}

}

//--------------------------------------------------------------------
// function to insert debug message into db
//--------------------------------------------------------------------
func ErrorNP(db *sql.DB, programid string, program string, msg string) {
	sqlString := "insert into errortbl(errortime,programid,programName,errormsg) values($1,$2,$3,$4)"
	_, inserterr := db.Exec(sqlString, time.Now(), programid, program, msg)
	//sqlString := "insert into errortbl(errortime,programid,programName,errormsg) values(GETDATE(),'" + programid + "','" + program + "','" + msg + "')"
	//_, inserterr := db.Exec(sqlString)
	if inserterr != nil {
		LogError(Panic, inserterr.Error())
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

//--------------------------------------------------------------------
// function to record program start and end time details into db
//--------------------------------------------------------------------
func RecordRunDetails(db *sql.DB, id int, runType string, programName string, count int, cmt string) (int, error) {
	insertedID := 0
	if runType == INSERT {
		insertString := "INSERT INTO SchedulerRunDetails(StartTime,ProgramName,RecordCount,comment)  values($1,$2,$3,$4);SELECT SCOPE_IDENTITY() "
		inserterr := db.QueryRow(insertString, time.Now(), programName, count, cmt).Scan(&insertedID)
		if inserterr != nil {
			return insertedID, fmt.Errorf("Error while inserting SchedulerRunDetails: ", inserterr.Error())
			//LogError(Panic, inserterr.Error())
		}
	} else if runType == UPDATE {
		insertedID = id
		updateString := "UPDATE SchedulerRunDetails  WITH (UPDLOCK)  SET EndTime=$1,RecordCount=$2,comment=$3 where id=$4 "

		_, updateerr := db.Exec(updateString, time.Now(), count, cmt, insertedID)
		if updateerr != nil {
			//log.Println(updateerr.Error())
			return insertedID, fmt.Errorf("Error while updating SchedulerRunDetails: ", updateerr.Error())

		}
	}
	return insertedID, nil

}

//---------------------------------------------------------------------------------
//Function inserts and return upload master ID
//---------------------------------------------------------------------------------
func InsertUploadMaster(db *sql.DB, uploadMasterRec UploadMaster) (int, error) {
	var datetime = time.Now()
	insertString := "INSERT INTO UploadDataMaster (Summary,SourceCode,IsDefault,IsForceDefault,ZohoDepartmentId,UploadFileName,UploadedBy,UploadDate,CampaignId,IsCampaign) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10);SELECT SCOPE_IDENTITY() "
	insertedID := 0
	inserterr := db.QueryRow(insertString, uploadMasterRec.Summary, uploadMasterRec.SourceCode, uploadMasterRec.IsDefault, uploadMasterRec.IsForceDefault, uploadMasterRec.ZohoDepartmentId, uploadMasterRec.UploadFileName, uploadMasterRec.UploadedBy, datetime, uploadMasterRec.CampaignId, uploadMasterRec.IsCampaign).Scan(&insertedID)
	if inserterr != nil {
		//log.Println(inserterr)
		return insertedID, fmt.Errorf("Error while inserting insertUploadMaster: ", inserterr.Error())

	}
	return insertedID, nil
}

//---------------------------------------------------------------------------------
//Function inserts record into ticket log table
//---------------------------------------------------------------------------------
func InsertTicketLog(db *sql.DB, ticket TicketLog) error {
	var datetime = time.Now()
	insertString := "INSERT INTO ticketlog (ClientId,[Description],Source,Summary_User,TicketStatus,CreatedDate,ZohoDepartmentId,UploadDataMasterId,AssigneeId,STCode,CreatedBy,CreatedProgram,UpdatedBy,UpdatedDate,UpdatedProgram,isdeleted,Processed,Spawned) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)"
	_, inserterr := db.Exec(insertString, ticket.ClientId, ticket.Description, ticket.Source, ticket.SummaryUser, ticket.TicketStatus, datetime, ticket.ZohoDepartmentId, ticket.UploadDataMasterId, ReturnNil(ticket.AssigneeId), ticket.STCode, ticket.CreatedBy, ticket.CreatedProgram, ticket.UpdatedBy, datetime, ticket.UpdatedProgram, 0, 0, 0)
	if inserterr != nil {
		return fmt.Errorf("Error while inserting insertTicketLog: ", inserterr.Error())
		//log.Println(inserterr.Error())
	}
	return nil
}

//---------------------------------------------------------------------------------
//Function inserts record into ticket log table
//---------------------------------------------------------------------------------
func UpdateTicketLog(db *sql.DB, ticket TicketLog) error {
	var datetime = time.Now()
	updateString := "update TicketLog  WITH (UPDLOCK)  set TicketStatus=$1, AssigneeId=$2, ZohoDepartmentId=$3, stcode=$4,UpdatedBy=$5, UpdatedDate=$6,UpdatedProgram=$7 where id=$8 "
	_, updateerr := db.Exec(updateString, ticket.TicketStatus, ReturnNil(ticket.AssigneeId), ticket.ZohoDepartmentId, ticket.STCode, ticket.UpdatedBy, datetime, ticket.UpdatedProgram, ticket.Id)
	if updateerr != nil {
		return fmt.Errorf("Error while updating insertTicketLog: ", updateerr.Error())
		//log.Println(updateerr.Error())
	}
	return nil
}

//---------------------------------------------------------------------------------
//Function to manage record for email log table
//---------------------------------------------------------------------------------
func EmailLog(db *sql.DB, email EmailLogType) error {
	var datetime = time.Now()
	if email.Action == INSERT {
		insertString := "INSERT INTO Emaillog (FromId,FromDspName,ToId,Cc,Bcc,Subject,Body,CreationDate,Status,CreatedProgram,EmailServer,ReplyTo) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)"
		_, inserterr := db.Exec(insertString, email.From, email.FromDspName, email.To, email.Cc, email.Bcc, email.Subject, email.Body, datetime, "NEW", email.CreatedProgram, email.EmailServer, email.ReplyTo)
		if inserterr != nil {
			return fmt.Errorf("Error while inserting EmailLog: ", inserterr.Error())
			//log.Println(inserterr.Error())
		}
	} else if email.Action == UPDATE {
		updateString := "update Emaillog  WITH (UPDLOCK) set SentDate=$1,Status=$2,ErrorMsg=$3 where id=$4 "
		_, updateerr := db.Exec(updateString, email.SentDate, email.Status, email.ErrorMsg, email.Id)
		if updateerr != nil {
			return fmt.Errorf("Error while updating EmailLog: ", updateerr.Error())
			//log.Println(updateerr.Error())
		}
	}
	return nil
}
