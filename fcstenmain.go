package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net/smtp"
	"strconv"
	"strings"
	"sync"
	"time"

	util "github.com/mrlakshmanan/fcsutility"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Behaviour    util.Behaviour
	Database     util.Database
	CoreSettings CoresettingsKey
}

type CoresettingsKey struct {
	StartHr        string
	EndHr          string
	FcsEmailServer string
	FcsAccount     string
	FcsPassword    string
}

type coresettings struct {
	StartHr        int
	EndHr          int
	CurrentHr      int
	FcsEmailServer string
	FcsAccount     string
	FcsPassword    string
}

type loginAuth struct {
	username, password string
}

func LoginAuth(username, password string) smtp.Auth {
	return &loginAuth{username, password}
}

func (a *loginAuth) Start(server *smtp.ServerInfo) (string, []byte, error) {
	return "LOGIN", []byte(a.username), nil
}

func (a *loginAuth) Next(fromServer []byte, more bool) ([]byte, error) {
	if more {
		switch string(fromServer) {
		case "Username:":
			return []byte(a.username), nil
		case "Password:":
			return []byte(a.password), nil
		default:
			return nil, errors.New("Unkown fromServer")
		}
	}
	return nil, nil
}

var globalDB *sql.DB
var config Config
var globalCmt string
var globalCnt int

//--------------------------------------------------------------------
// function to insert debug message into db
//--------------------------------------------------------------------
func debug(level string, program string, msg string) {

	util.Debug(globalDB, config.Behaviour.DisplayDebug, config.Behaviour.Debug, config.Behaviour.DebugLevel, level, config.Behaviour.ProgramId, program, msg)

}

//--------------------------------------------------------------------
// function to insert error message into db
//--------------------------------------------------------------------
func errorlog(mode string, program string, msg string) {
	debug(util.Statement, program, msg)
	if mode == util.Panic {
		util.Error(globalDB, config.Behaviour.ProgramId, program, msg)
	} else if mode == util.NoPanic {
		util.ErrorNP(globalDB, config.Behaviour.ProgramId, program, msg)
	}

}

//--------------------------------------------------------------------
// function reads the constants from the config.toml file
//--------------------------------------------------------------------
func readConfig() {
	if _, err := toml.DecodeFile("./fcs10config.toml", &config); err != nil {
		fmt.Println(err)
	}
}

//--------------------------------------------------------------------
// function to get and set program related settings
//--------------------------------------------------------------------
func getProgramSettings() coresettings {
	debug(util.Statement, "getProgramSettings", "(+)")
	var coreSettings coresettings
	currentHr := util.GetCurrentHr()

	startHr := util.GetCoreSettingValue(globalDB, config.CoreSettings.StartHr)
	endHr := util.GetCoreSettingValue(globalDB, config.CoreSettings.EndHr)

	vn_currentHr, _ := strconv.Atoi(currentHr)
	vn_startHr, _ := strconv.Atoi(startHr)
	vn_endHr, _ := strconv.Atoi(endHr)

	coreSettings.StartHr = vn_startHr
	coreSettings.EndHr = vn_endHr
	coreSettings.CurrentHr = vn_currentHr

	coreSettings.FcsEmailServer = util.GetCoreSettingValue(globalDB, config.CoreSettings.FcsEmailServer)
	coreSettings.FcsAccount = util.GetCoreSettingValue(globalDB, config.CoreSettings.FcsAccount)
	coreSettings.FcsPassword = util.GetCoreSettingValue(globalDB, config.CoreSettings.FcsPassword)

	debug(util.Detail, "getProgramSettings", "currentHr > "+currentHr)
	debug(util.Detail, "getProgramSettings", "startHr > "+startHr)
	debug(util.Detail, "getProgramSettings", "endHr > "+endHr)
	debug(util.Detail, "getProgramSettings", "FcsEmailServer > "+coreSettings.FcsEmailServer)
	debug(util.Detail, "getProgramSettings", "FcsAccount > "+coreSettings.FcsAccount)
	debug(util.Statement, "getProgramSettings", "(-)")
	return coreSettings
}

//--------------------------------------------------------------------
// function sends email
//--------------------------------------------------------------------
func sendEmail(wg *sync.WaitGroup, wgcnt string, coreSettings coresettings, emailrec util.EmailLogType) {
	debug(util.Statement, "sendEmail_"+wgcnt, "(+)")
	debug(util.Detail, "sendEmail_"+wgcnt, "EmailLog ID :"+emailrec.Id)
	status := ""
	errmsg := ""

	from := ""
	bcc := ""
	cc := ""
	replyto := ""
	mailserver := ""

	account := coreSettings.FcsAccount
	pwd := coreSettings.FcsPassword

	to := strings.Split(emailrec.To, ",")
	toHeader := "To: " + strings.Join(to, ",") + "\n"

	if emailrec.EmailServer == "" || emailrec.EmailServer == "ftctrade.co.in" {
		mailserver = coreSettings.FcsEmailServer
	} else {
		mailserver = coreSettings.FcsEmailServer
	}
	if emailrec.FromDspName != "" {
		from = emailrec.FromDspName + " <" + emailrec.From + ">"
	} else {
		from = emailrec.From
	}
	if emailrec.FromDspName != "" {
		replyto = "reply-to: " + emailrec.ReplyTo + "\n"
	}
	if emailrec.Bcc != "" {
		bcc = "Bcc: " + emailrec.Bcc + "\n"
	}
	if emailrec.Cc != "" {
		cc = "Cc: " + emailrec.Cc + "\n"
	}
	mime := "MIME-version: 1.0;\nContent-Type: text/html; charset=\"UTF-8\";\n\n"
	msg := "From: " + from + "\n" +
		toHeader +
		bcc +
		cc +
		replyto +
		"Subject: " + emailrec.Subject + "\n" + mime +
		emailrec.Body

	auth := LoginAuth(account, pwd)
	err := smtp.SendMail(mailserver, auth, from, to, []byte(msg))

	if err != nil {
		errorlog(util.NoPanic, "sendEmail_"+wgcnt, err.Error())
		errmsg = err.Error()
		status = "ERROR"
	} else {
		errmsg = ""
		status = "SENT"
	}

	sql := "update emaillog WITH (UPDLOCK) set status = $1,sentdate=$2, ErrorMsg=$3  where id = $4 "
	_, err = globalDB.Exec(sql, status, time.Now(), errmsg, emailrec.Id)
	if err != nil {
		errorlog(util.NoPanic, "sendEmail_"+wgcnt, err.Error())
	}
	wg.Done()
	debug(util.Statement, "sendEmail_"+wgcnt, "(-)")
}

//--------------------------------------------------------------------
// function that process emails from emaillog table
//--------------------------------------------------------------------
func processEmail(coreSettings coresettings) {
	debug(util.Statement, "processEmail", "(+)")
	var wg sync.WaitGroup

	gocalled := 0
	sqlString := "select id,FromId,ToId,isnull(Cc,''),isnull(Bcc,''),Subject,Body,isnull(EmailServer,''),isnull(FromDspName,''),isnull(ReplyTo,'') from EmailLog where status='NEW' "
	rows, err := globalDB.Query(sqlString)
	if err != nil {
		errorlog(util.Panic, "processEmail", err.Error())
	}
	for rows.Next() {
		var emailRec util.EmailLogType
		err := rows.Scan(&emailRec.Id, &emailRec.From, &emailRec.To, &emailRec.Cc, &emailRec.Bcc, &emailRec.Subject, &emailRec.Body, &emailRec.EmailServer, &emailRec.FromDspName, &emailRec.ReplyTo)
		if err != nil {
			errorlog(util.NoPanic, "processEmail", err.Error())

		}
		wg.Add(1)
		v_gocalled := strconv.Itoa(gocalled)
		go sendEmail(&wg, v_gocalled, coreSettings, emailRec)
		gocalled++
		if gocalled%50 == 0 {
			debug(util.Detail, "processEmail", "v_gocalled >"+v_gocalled)
			debug(util.Detail, "processEmail", "I am in a break ! ")
			wg.Wait()
		}
	}
	globalCmt = "Total Records Processed :" + strconv.Itoa(gocalled)

	if gocalled > 0 {
		debug(util.Detail, "processEmail", "I am waiting.. ")
		wg.Wait()
		debug(util.Detail, "processEmail", globalCmt)
	} else {
		debug(util.Detail, "processEmail", "No Records to process")
	}
	debug(util.Statement, "processEmail", "(-)")
}

//--------------------------------------------------------------------
// main function executed from command
//--------------------------------------------------------------------
func main() {
	readConfig()

	globalDB = util.Getdb("mssql", config.Database)
	defer globalDB.Close()
	debug(util.Statement, "main", "(+)")

	scheduleID, err := util.RecordRunDetails(globalDB, 0, util.INSERT, config.Behaviour.ProgramName, globalCnt, globalCmt)
	if err != nil {
		errorlog(util.Panic, "main", err.Error())
	} else {
		coreSettings := getProgramSettings()

		if coreSettings.CurrentHr >= coreSettings.StartHr && coreSettings.CurrentHr <= coreSettings.EndHr {

			debug(util.Detail, "main", "With in Scheduled hour")
			processEmail(coreSettings)

		} else {
			debug(util.Detail, "main", "Outside Scheduled hour")

		}
		scheduleID, err = util.RecordRunDetails(globalDB, scheduleID, util.UPDATE, config.Behaviour.ProgramName, globalCnt, globalCmt)
		if err != nil {
			errorlog(util.NoPanic, "main", err.Error())
		}
	}

	debug(util.Statement, "main", "(-)")

}
