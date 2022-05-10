package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb" //sqlserver
	mssql "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror" // oracle
	_ "github.com/lib/pq"        // postgresql
)

// Database Setting
type Config struct {
	host     string
	port     int
	user     string
	password string
	dbname   string
	params   string
	dbtype   string
	extra    string
}

// Database type
const (
	DB_TYPE_MYSQL      = "mysql"
	DB_TYPE_POSTGRESQL = "postgresql"
	DB_TYPE_ORACLE     = "oracle"
	DB_TYPE_MSSQL      = "mssql"
)

const (
	DB_TYPE_MYSQL_DRIVER      = "mysql"
	DB_TYPE_POSTGRESQL_DRIVER = "postgres"
	DB_TYPE_ORACLE_DRIVER     = "godror"
	DB_TYPE_MSSQL_DRIVER      = "mssql"
)

// time layout
const (
	TIME_LAYOUT_DT = "2006-01-02T15:04:05-07:00"
	TIME_LAYOUT_TS = "2006-01-02T15:04:05.999-07:00"
)

type Connection struct {
	*sql.DB
	*Config
	connected bool
}

type Client struct {
	// database type
	dbtype string

	// current database user
	user string

	// =;
	delim string

	// default prompt
	prompt0 string
	// database prompt
	prompt1 string

	// table format sep
	colsepc string
	colsepl string
	colsepr string
	colsep  bool

	// datetime format
	dtformat string
	// timestamp format
	tsformat string

	// slient? print with not border
	slient   bool
	feedback bool

	// sql buffer info
	// sqlbuffsize int
	// sqlbuff     []string

	// current connection
	conn *Connection

	// reader
	rd *bufio.Reader

	// sql history.
	history []string

	time   bool
	timing bool

	spool    string
	linesize int
}

const (
	LINEFEED = '\n'
)

var dbTypeDriverMap map[string]string
var client *Client

func initBase() {

	// create map
	dbTypeDriverMap = make(map[string]string)
	dbTypeDriverMap[DB_TYPE_MYSQL] = DB_TYPE_MYSQL_DRIVER
	dbTypeDriverMap[DB_TYPE_MSSQL] = DB_TYPE_MSSQL_DRIVER
	dbTypeDriverMap[DB_TYPE_POSTGRESQL] = DB_TYPE_POSTGRESQL_DRIVER
	dbTypeDriverMap[DB_TYPE_ORACLE] = DB_TYPE_ORACLE_DRIVER

	// init config
	client = &Client{
		user:    "",
		delim:   ";",
		prompt0: "dbc>> ",
		prompt1: "",
		colsepc: " | ",
		colsepl: "| ",
		colsepr: " |",
		colsep:  true,

		dtformat: "2006-01-02 15:04:05",
		tsformat: "2006-01-02 15:04:05.000000 -0700",

		slient:   false,
		feedback: true,
		// sqlbuffsize: 1000,
		conn: nil,

		time:   false,
		timing: false,
		spool:  "",

		linesize: 0,
	}

	// init sqlbuffer
	// client.sqlbuff = make([]string, client.sqlbuffsize)

	// init reader
	client.rd = bufio.NewReader(os.Stdin)
	client.dbtype = DB_TYPE_ORACLE

	// auto setting linesize
	columns, _ := strconv.Atoi(os.Getenv("COLUMNS"))
	client.linesize = columns

}

const (
	SYSCMD_END int = 0
	SYSCMD_NEXT
	SYSCMD_RESET
)

const (
	YES bool = true
	NO  bool = false
)

func interactive(client *Client) {

	var sb strings.Builder

	var isCmd, isDone bool
	var cmdLen int
	var input []byte

	// read data from file ?
	var buf [][]byte
	var bufi int

	// fmt.Printf("%s", client.GetPrompt(0))

statementLoop:
	for {

		isCmd = NO
		cmdLen = 0

		isDone = NO
		sb.Reset()

		for i := 0; i < 1000; i++ {

			if client.time {
				fmt.Printf("%s ", time.Now().Format("15:04:05"))
			}

			if !isCmd {
				fmt.Printf("%s", client.GetPrompt(i))
			} else {
				fmt.Printf("%s", "> ")
			}

			// read data from stdin
			if buf == nil {
				//
				in, err := client.rd.ReadBytes(LINEFEED)
				if err != nil && err == io.EOF {
					ExitNormal()
				}
				input = in
			} else {
				if bufi >= len(buf) {
					// EOF
					// reset
					buf = nil
					bufi = 0
					continue statementLoop
				} else {
					input = buf[bufi]
					bufi++
				}
			}

			// remove left or right blank..., eoo right ?
			input = BlankTrim(input)

			end := len(input) - 1

			f := NO

			// last byte.
			switch input[0] {
			case '\n':
				// only press enter.
				continue statementLoop

			case '/':
				// seconds run query?
				fmt.Println("seconds run query?")

			case '!':
				f = YES
				// run system commands.
				isCmd = YES

			case '@':

				// load local file
				// @file response []byte
				file := string(input[1:end])
				//
				fdata := string(readfile(file))
				// to arr
				for _, data := range strings.Split(fdata, string(LINEFEED)) {
					arr := ([]byte)(data)
					arr = append(arr, LINEFEED)
					buf = append(buf, arr)
				}

				// sb.Write(readfile(file))

				// read done.
				// isDone = YES
				continue statementLoop

			case 'C', 'c':
				f = YES

				key0 := getBlankLeft(input[:end])

				// fmt.Println("connectconnectconnect22222:" + string(input) + "......." + getBlankLeft(input))
				// conn[ect] ?
				switch key0 {
				case "conn", "connect":
					// fmt.Println("connectconnectconnect:" + string(input))
					// fmt.Println("connectconnectconnect2:" + string(input[:end]))
					//
					conn, err := connectDB(RemoveFatBlank(input[len(key0):end]), client)
					if err != nil {
						fmt.Printf("%s\n", err.Error())
					}
					// Update Connection
					client.conn = conn
					continue statementLoop
				}

			case 'D', 'd':

				f = YES

				key0 := getBlankLeft(input[:end])

				// desc ?
				switch key0 {
				case "desc":

					if client.conn == nil || !client.conn.connected {
						fmt.Printf("\ndbcli: You are not currently connected to the database.\n\n")
						continue statementLoop
					}

					switch client.conn.dbtype {
					case DB_TYPE_ORACLE:

						tabinfo := string(RemoveFatBlank(input[4:end]))
						if tabinfo[len(tabinfo)-1:] == ";" {
							tabinfo = tabinfo[:len(tabinfo)-1]
						}
						ss := strings.Split(tabinfo, ".")
						owner := client.conn.user
						tab := ""

						switch len(ss) {
						case 1:
							tab = tabinfo
						case 2:
							owner = ss[0]
							tab = ss[1]
						}

						qry := fmt.Sprintf(`select a.COLUMN_NAME "Field", 
						lower(case a.DATA_TYPE 
						when 'NUMBER' then 'NUMBER(38)'
						when 'VARCHAR2' then a.DATA_TYPE || '(' || a.DATA_LENGTH || ')'
						else a.DATA_TYPE
						end)AS "Type", 
						(case a.NULLABLE 
						when 'Y' then 'YES' else 'NO' end) AS "Null?", b.key AS "Key", a.DATA_DEFAULT AS "Default", '' as "Extra" from ALL_tab_columns A left join
						(
						select t0.column_name, t1.owner, t1.table_name, 'PRI' as "KEY" from user_cons_columns t0 left join user_constraints t1
						on t0.OWNER = t1.OWNER and t0.TABLE_NAME = t1.TABLE_NAME and t0.CONSTRAINT_NAME = t1.CONSTRAINT_NAME and t1.CONSTRAINT_TYPE = 'P'
						) b
						on b.OWNER = a.OWNER and b.TABLE_NAME = a.TABLE_NAME and a.COLUMN_NAME = b.COLUMN_NAME
						where a.owner = '%s' AND a.TABLE_NAME = '%s' order by a.COLUMN_ID`, strings.ToUpper(owner), strings.ToUpper(tab))

						query0(qry, client)

					case DB_TYPE_MYSQL:

						tabinfo := string(RemoveFatBlank(input[4:end]))
						if tabinfo[len(tabinfo)-1:] == ";" {
							tabinfo = tabinfo[:len(tabinfo)-1]
						}
						ss := strings.Split(tabinfo, ".")
						owner := client.conn.dbname
						tab := ""

						switch len(ss) {
						case 1:
							tab = tabinfo
						case 2:
							owner = ss[0]
							tab = ss[1]
						}

						qry := fmt.Sprintf(`select 
						a.COLUMN_NAME as "Field", 
						a.COLUMN_TYPE as "Type", 
						a.IS_NULLABLE as "Null",
						case when b.COLUMN_NAME is NULL then '' else 'PRI' end as "Key", 
						a.COLUMN_DEFAULT as "Default", 
						a.EXTRA as "Extra"
					from information_schema.columns a left join information_schema.KEY_COLUMN_USAGE b
					on b.TABLE_SCHEMA = a.TABLE_SCHEMA and b.table_name = a.table_name and b.COLUMN_NAME = a.COLUMN_NAME
					where a.TABLE_SCHEMA = '%s' and a.TABLE_NAME = '%s' order by a.ORDINAL_POSITION`, owner, tab)

						query0(qry, client)

					case DB_TYPE_POSTGRESQL:

						tabinfo := string(RemoveFatBlank(input[4:end]))
						if tabinfo[len(tabinfo)-1:] == ";" {
							tabinfo = tabinfo[:len(tabinfo)-1]
						}
						ss := strings.Split(tabinfo, ".")
						owner := "public"
						tab := ""

						switch len(ss) {
						case 1:
							tab = tabinfo
						case 2:
							owner = ss[0]
							tab = ss[1]
						}

						qry := fmt.Sprintf(`select a.column_name as "Field", 
						(case a.udt_name when 'varchar' then a.udt_name || '(' || a.character_maximum_length || ')' else a.udt_name end ) as "Type",
						a.is_nullable  AS "Null?", t.key AS "Key", a.column_default AS "Default", '' as "Extra" from information_schema.columns a 
						left join (
						SELECT
							d.nspname as schema,
							a.relname as tabname,
							b.attname AS colname,
							'PRI' as key
						FROM
							pg_constraint c 
							INNER JOIN pg_class a ON c.conrelid = a.oid
							INNER JOIN pg_namespace d ON a.relnamespace = d.oid
							INNER JOIN pg_attribute b ON b.attrelid = a.oid AND b.attnum = c.conkey [ 1 ]
						WHERE
						   d.nspname='%s' and a.relname = '%s'
						AND c.contype = 'p'
						) t
						on a.column_name = t.colname
						where a.table_schema = '%s' and a.table_name = '%s' order by a.ordinal_position`, owner, tab, owner, tab)

						query0(qry, client)

					case DB_TYPE_MSSQL:

						tabinfo := string(RemoveFatBlank(input[4:end]))
						if tabinfo[len(tabinfo)-1:] == ";" {
							tabinfo = tabinfo[:len(tabinfo)-1]
						}
						ss := strings.Split(tabinfo, ".")
						owner := "dbo"
						tab := ""

						switch len(ss) {
						case 1:
							tab = tabinfo
						case 2:
							owner = ss[0]
							tab = ss[1]
						}

						qry := fmt.Sprintf(`select
						c.name as "Field",
						c.data_type as "Type",
						case c.is_nullable when 1 then 'NO' else 'YES' END as "Null?", 
						case when c.is_identity = 1 then 'PRI' else '' END as "Key",
						case when sm.text is NULL then '' else sm.text END as "Default",
						case when c.collation_name is NULL then '' else 'Collation is ' + c.collation_name END as "Extra"
					   from 
					   sys.objects o left join
					   (select b.object_id, a.name, 
						   case when a.scale > 0 then t.name + '(' + cast(a.scale as varchar) + ')' when a.scale is NULL then t.name + '(' + cast(a.length as varchar) + ')' else t.name END  as data_type, a.colid, a.cdefault, b.is_nullable, b.is_identity, b.collation_name 
						   from syscolumns a left join sys.columns b on a.id = b.object_id and a.name = b.name
						   left join sys.types t on b.system_type_id = t.system_type_id and b.user_type_id = t.user_type_id) c
					   on o.object_id = c.object_id
					   left join syscomments sm
					   on c.cdefault = sm.id
					   where o.schema_id = schema_id('%s') and o.object_id = object_id('%s') 
					   order by c.colid`, owner, tab)

						query0(qry, client)
					}

					continue statementLoop
				}

			case 'Q', 'q', 'E', 'e', '\\':
				// quit, exit, \q ?
				f = YES

				key0 := getBlankLeft(input[:end])

				switch key0 {
				case "quit", "exit", "\\q":
					ExitNormal()

				case "\\.":
					// cat file ?
				}
			case 'S', 's':
				f = YES

				key0 := getBlankLeft(input[:end])

				// show, set ?
				switch key0 {
				case "show":

					show0(string(RemoveFatBlank(input[3:end])), client)

					continue statementLoop

				case "set":

					set0(string(RemoveFatBlank(input[3:end])), client)

					continue statementLoop

				}

			// case 'U', 'u':
			// 	f = YES

			// 	key0 := getBlankLeft(input[:end])

			// 	switch key0 {

			// 	case "use":
			// 		use0(string(RemoveFatBlank(input[3:end])), client)

			// 	}

			default:
				if isCmd {
					break
				}

				// fmt.Println("SQL...." + sb.String())

				f = YES
			}

			// append string to sb
			if f && !isCmd {
				// remove \n
				sb.Write(input[:end])
			}

			//
			// last seconds char
			switch input[end-1] {
			case '/':
				isDone = true

			case ';':
				// sql end of ";"
				// fmt.Println("SQLEND...." + sb.String())
				isDone = true

			case 'G':
				// \G ?
				if len(input) >= 2 && input[end-2] == '\\' {
					isDone = true
				}

			}

			//  first char is !
			if isCmd {

				//
				var n int
				if cmdLen == 0 {
					n = 1
				} else {
					n = 0
				}

				var tmp []byte

				// \ & \n
				if input[end-1] == '\\' {
					// last seconds char is \
					// such as:
					// ls \
					// grep 'o'
					tmp = input[n : end-1]

				} else {
					//
					tmp = input[n:end]

					// input sys command done.
					isDone = YES
				}

				sb.Write(tmp)

				cmdLen++
			}

			if isDone {

				if isCmd {
					runCmd(sb.String())
				} else {
					//
					emit(sb.String(), client)
				}

				// fmt.Printf("===========>%s<==========\n", sb.String())
				continue statementLoop
			}

			// continue..
			sb.WriteByte(' ')
		}

	}

}

//
func main() {
	// init...
	initBase()

	// init client
	interactive(client)

}

//
// build command prompt
//
func (client *Client) GetPrompt(line int) string {

	if client.conn == nil || !client.conn.connected {
		return client.prompt0
	}

	// build database default prompt
	switch client.conn.dbtype {
	case DB_TYPE_MSSQL:
		if line < 1 {
			client.prompt1 = "sqlcmd> "
		} else {
			client.prompt1 = "sqlcmd> "
		}

	case DB_TYPE_ORACLE:
		if line < 1 {
			client.prompt1 = "SQL> "
		} else {
			client.prompt1 = fmt.Sprintf("% 4d ", line+1)
		}

	case DB_TYPE_POSTGRESQL:
		if line < 1 {
			client.prompt1 = client.conn.dbname + "=# "
		} else {
			client.prompt1 = client.conn.dbname + "-# "
		}

	case DB_TYPE_MYSQL:
		if line < 1 {
			client.prompt1 = "mysql> "
		} else {
			client.prompt1 = " > "
		}
	default:
		return client.prompt0
	}
	return client.prompt1
}

// change database connection
func connectDB(input []byte, client *Client) (*Connection, error) {

	// log.Printf("\nclient target: \"%s\"\n\n", client.dbtype)

	if input[len(input)-1] == LINEFEED {
		input = input[:len(input)-1]
	}

	connstr := string(input)

	// remove first keyword, ..
	// remove conn or connect
	// idx := strings.Index(connstr, " ")
	if connstr == "" {
		return nil, fmt.Errorf("err: format: conn[ect] user/password@host:port/[service_name|dbname] [as sysdba|sysoper]")
	}

	// connstr = connstr[idx:]

	// set db=mysql
	//
	// oracle: user/password@host:port/service_name [as sysdb] ?
	// mysql:  user/password@host:port/dbname
	// pgsql:  user/password@host:port/dbname
	//

	uidx := strings.Index(connstr, "/")
	didx := strings.LastIndex(connstr, "/")
	pidx := strings.LastIndex(connstr, ":")
	hidx := strings.LastIndex(connstr, "@")

	user := connstr[:uidx]
	password := connstr[uidx+1 : hidx]
	host := connstr[hidx+1 : pidx]
	p, err := strconv.Atoi(connstr[pidx+1 : didx])
	if err != nil {
		return nil, fmt.Errorf("err: invalid port: %s", connstr)
	}
	port := p

	dbname0 := connstr[didx+1:]
	dbname := dbname0
	extra := ""

	if strings.Contains(dbname0, " ") {

		ss := strings.Split(dbname0, " ")
		dbname = ss[0]
		switch len(ss) {
		case 1:
			// dbname := ss[0]
		case 2:
			// invalid
		case 3:
			// as sysdba ?
			// as sysoper ?
			if ss[1] == "as" && ss[2] == "sysdba" {
				if extra == "" {
					extra = "sysdba=1"
				} else {
					extra = extra + " " + "sysdba=1"
				}

			} else if ss[1] == "as" && ss[2] == "sysoper" {
				if extra == "" {
					extra = "sysoper=1"
				} else {
					extra = extra + " " + "sysoper=1"
				}
			}
		}
	}

	var config = &Config{
		user:     user,
		password: password,
		host:     host,
		port:     port,
		dbname:   dbname,
		extra:    extra,
		dbtype:   client.dbtype,
	}

	// Update Connection
	return NewConnection(config)
}

// post query to databse
func emit(statements string, client *Client) {

	// fmt.Println("statements:" + statements)

	// check database connection
	if client.conn == nil {
		fmt.Printf("\ndbcli: You are not currently connected to the database.\n\n")
		return
	}

	for _, stmt := range strings.Split(statements, string(LINEFEED)) {

		if stmt == "" {
			continue
		}

		// remove left or right blank
		stmt = strings.Trim(stmt, "")
		// get index of first blank
		idx := strings.Index(stmt, " ")

		if idx == -1 {
			//
			fmt.Printf("\nUnsupported operation: %s\n\n", stmt)
			continue
		}

		// get first keyword.
		key := stmt[:idx]

		switch strings.ToLower(key) {
		case "select":
			query0(stmt, client)

		default:
			exec0(stmt, client)
		}

		//
		// fmt.Printf("final: %s\n", stmt)

	}

}

func getYesNo(s string) bool {

	var res bool

	switch s {
	case "on", "1", "yes", "true":
		res = true
	case "off", "0", "no", "false":
		res = false
	}

	return res
}

func show0(s string, client *Client) {

	ss := strings.Split(strings.TrimLeft(s, " "), " ")

	// fmt.Printf("=>>>>>>>>>%#v\n", ss)
	switch ss[0] {
	case "time":
		client.time = getYesNo(ss[1])

	case "timing":
		client.timing = getYesNo(ss[1])

	case "feedback":
		client.feedback = getYesNo(ss[1])

	case "slient":
		client.slient = getYesNo(ss[1])

	case "target":
		//
		println()
	}

}

func set0(s string, client *Client) {

	if s == "" {
		fmt.Println("usage: ")
		return
	}

	ss := strings.Split(strings.TrimLeft(s, " "), " ")

	// fmt.Printf("=>>>>>>>>>%#v\n", ss)
	switch ss[0] {
	case "time":
		client.time = getYesNo(ss[1])

	case "timing":
		client.timing = getYesNo(ss[1])

	case "feedback":
		client.feedback = getYesNo(ss[1])

	case "slient":
		client.slient = getYesNo(ss[1])

	case "target":
		//
		client.dbtype = ss[1]

		if dbTypeDriverMap[ss[1]] == "" {
			fmt.Printf("Invalid target: \"%s\"\n", ss[1])
		} else {
			fmt.Printf("Changed to target: \"%s\"\n", client.dbtype)
		}
	}

}

func query0(stmt string, client *Client) {

	drv := false

	switch stmt[len(stmt)-1:] {
	case "G":
		// end of \G ?
		if stmt[len(stmt)-2:len(stmt)-1] == "\\" {
			drv = true
			stmt = stmt[:len(stmt)-2]
		}
	case ";":
		// sql end of ";" ?
		stmt = stmt[:len(stmt)-1]
	}

	// fmt.Println("stmt: " + stmt)

	t0 := time.Now()
	// if client.time || client.timing {

	// }

	rows, err := client.conn.Query(stmt)
	t1 := time.Since(t0)
	if err != nil {
		fmt.Printf("\n%s\n\n", err.Error())
		return
	}

	colTypes, _ := rows.ColumnTypes()
	columnCount := len(colTypes)

	// column name max width
	var width0 int

	// column value max width
	var widtha []int
	if !drv {
		widtha = make([]int, columnCount)
	}

	for order, colType := range colTypes {

		width1 := len(colType.Name())
		if drv {
			if width0 < width1 {
				width0 = width1
			}
		} else {
			widtha[order] = width1
		}
	}

	values := make([]sql.RawBytes, columnCount)
	valuePtrs := make([]interface{}, columnCount)
	for i := 0; i < columnCount; i++ {
		valuePtrs[i] = &values[i]
	}

	// query result set
	var rs [][]string
	var rs0 []string

	var n int64
	var v0 string
	for rows.Next() {
		n++

		if drv {
			fmt.Printf("\n***************************** %d row *****************************\n", n)
		}

		err := rows.Scan(valuePtrs...)
		if err != nil {
			fmt.Printf("\n%s\n\n", err.Error())
			return
		}

		if !drv {
			rs0 = make([]string, columnCount)
		}

		for order, colType := range colTypes {
			v := values[order]

			switch colType.DatabaseTypeName() {
			case "UNIQUEIDENTIFIER":

				if len(v) == 0 {
					v0 = "<NULL>"
					break
				}

				// type UniqueIdentifier [16]byte
				var a mssql.UniqueIdentifier
				// ignore
				for i, v2 := range v {
					a[i] = v2
				}

				v0 = a.String()

			default:
				v0 = string(v)

			}

			if drv {

				fmt.Printf("%"+fmt.Sprintf("%d", width0)+"s: %s\n", colType.Name(), v0)
			} else {
				rs0[order] = v0
				l := len(v0)
				if widtha[order] < l {
					widtha[order] = l
				}
			}
		}

		if !drv {
			rs = append(rs, rs0)
		}
	}

	if !drv {
		// line mode
		fmt.Println("")
		writeBorder(widtha, client)
		writeHeader(widtha, colTypes, client)
		writeBorder(widtha, client)
		writeData(rs, colTypes, widtha, client)
		writeBorder(widtha, client)
	}

	fmt.Println("")

	if client.feedback {
		fmt.Printf("%d rows in set (%.3f sec)\n\n", n, float64(t1.Milliseconds())/float64(1000))
	}
}

//
// execute sql
// update, insert, delete, create, drop, ...
//
func exec0(stmt string, client *Client) {

	// fmt.Println("exec0.stmt:" + stmt)

	t0 := time.Now()
	res, err := client.conn.Exec(stmt)
	t1 := time.Since(t0)
	if err != nil {
		fmt.Printf("\n%s\n\n", err.Error())
		return
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		fmt.Printf("\n%s\n\n", err.Error())
		return
	}

	if !client.slient && client.feedback {
		fmt.Printf("\nQuery Ok, %d row affected (%.3f sec)\n\n", rowsAffected, float64(t1.Milliseconds())/float64(1000))
	}

}

func writeData(resultset [][]string, colTypes []*sql.ColumnType, widtha []int, client *Client) {

	// fmt.Printf("resultset=%#v\n", resultset)
	// fmt.Printf("widtha=%#v\n", widtha)

	for _, row := range resultset {

		if client.colsep {
			// first
			fmt.Printf("%s", client.colsepl)
		}

		for j, cell := range row {

			if client.colsep && j > 0 && j < len(row) {
				// center
				fmt.Printf("%s", client.colsepc)
			}

			fmt.Printf("%-"+fmt.Sprintf("%d", widtha[j])+"s", cell)

		}

		if client.colsep {
			// end
			fmt.Printf("%s\n", client.colsepr)
		} else {
			fmt.Println("")
		}

		// switch colTypes[i].DatabaseTypeName() {
		// case "DATE", "date":
		// 	t, err := time.Parse(TIME_LAYOUT_DT, string(value))
		// 	if err != nil {
		// 		fmt.Printf("%s\n", err.Error())
		// 	} else {
		// 		fmt.Printf("%s", t.Format(client.dtformat))
		// 	}

		// case "TIMESTAMP", "timestamp", "TIMESTAMP WITH TIME ZONE":
		// 	t, err := time.Parse(TIME_LAYOUT_TS, string(value))
		// 	if err != nil {
		// 		fmt.Printf("%s", err.Error())
		// 	} else {
		// 		fmt.Printf("%s", t.Format(client.tsformat))
		// 	}

		// default:
		// fmt.Printf("%-"+fmt.Sprintf("%d", widtha[i])+"s", value)
		// }

	}

	// if !client.colsep {
	// 	fmt.Println(client.colsepr)
	// } else {
	// 	fmt.Println("")
	// }

}

func writeHeader(widtha []int, colTypes []*sql.ColumnType, client *Client) {

	// if client.colsep {
	// fmt.Println("")
	// }

	// print header
	fmt.Printf("%s", client.colsepl)
	for i, colType := range colTypes {
		if i > 0 {
			fmt.Printf("%s", client.colsepc)
		}
		fmt.Printf("%-"+fmt.Sprintf("%d", widtha[i])+"s", colType.Name())
	}

	fmt.Println(client.colsepr)
}

func writeBorder(widtha []int, client *Client) {

	// if client.colsep {
	// 	return
	// }

	fmt.Printf("%s", "+")

	w0 := len(client.colsepc) - 1
	for _, width := range widtha {
		fmt.Printf("%s+", strings.Repeat("-", (int)(width)+w0))
	}

	fmt.Println("")
}

///////////////////////////////////////////////////////////////////////////////////////////
// Connection module.
///////////////////////////////////////////////////////////////////////////////////////////

func GetDataSourceName(config *Config) string {

	var dataSourceName string

	switch config.dbtype {

	case DB_TYPE_MYSQL:
		dataSourceName = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
			config.user,
			config.password,
			config.host,
			config.port,
			config.dbname)

		if config.params != "" {
			dataSourceName = fmt.Sprintf("%s?%s", dataSourceName, config.params)
		}

	case DB_TYPE_POSTGRESQL:
		dataSourceName = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			config.host,
			config.port,
			config.user,
			config.password,
			config.dbname)

	case DB_TYPE_MSSQL:

		query := url.Values{}
		query.Add("database", config.dbname)
		query.Add("encrypt", "disable")
		// query.Add("app name", "MyAppName")

		u := &url.URL{
			Scheme: "sqlserver",
			User:   url.UserPassword(config.user, config.password),
			Host:   fmt.Sprintf("%s:%d", config.host, config.port),
			// Path:     config.dbname,
			RawQuery: query.Encode(),
		}

		dataSourceName = u.String()

		fmt.Println(dataSourceName)

		// dataSourceName = fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d",
		// 	config.host,
		// 	config.user,
		// 	config.password,
		// 	config.port)

	case DB_TYPE_ORACLE:
		// https://pkg.go.dev/github.com/godror/godror@v0.33.0
		// https://godror.github.io/godror/doc/connection.html

		// MacOS: ln -s ../instantclient_19_8 ../instantclient_19_8/lib; export ORACLE_HOME=../instantclient_19_8
		// Linux: export LD_LIBRARY_PATH=../instantclient_19_8

		dbinfo := config.dbname
		// config.params = config.params + "timezone=0"
		if config.params != "" {
			dbinfo += "?" + config.params
		}
		dataSourceName = fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s"`,
			config.user,
			config.password,
			config.host,
			config.port,
			dbinfo)

		// https://download.oracle.com/ocomdocs/global/Oracle-Net-19c-Easy-Connect-Plus.pdf
		// if config.params != "" {
		// 	dataSourceName = fmt.Sprintf("%s?%s", dataSourceName, config.params)
		// }

		if config.extra != "" {
			dataSourceName = fmt.Sprintf("%s %s", dataSourceName, config.extra)
		} else {
			dataSourceName = fmt.Sprintf("%s %s", dataSourceName, "noTimezoneCheck=1")
		}

	}

	return dataSourceName
}

// create a connection
func NewConnection(config *Config) (*Connection, error) {

	// log.Printf("[INFO] Attempt new connection for \"%s\": %s@%s:%d/%s, params: %s, extra: %s\n",
	// 	config.dbtype, config.user, config.host, config.port, config.dbname, config.params, config.extra)
	// log.Printf("%#v\n", config)

	dataSourceName := GetDataSourceName(config)

	// get driver name
	driverName := dbTypeDriverMap[config.dbtype]

	db, err := sql.Open(driverName, dataSourceName)

	if err != nil {
		log.Printf("[OPEN] %s\n", err.Error())
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		log.Printf("[PONG] %s\n", err.Error())
		return nil, err
	}

	// log.Println("[INFO] Database connected successfully")

	return &Connection{db, config, true}, nil
}

// read file
func readfile(file string) []byte {

	// if !strings.HasSuffix(file, ".sql") {
	// 	file = file + ".sql"
	// }

	data, err := os.ReadFile(file)
	if err != nil {
		fmt.Printf("Open file err: %s\n", err.Error())
		return []byte{}
	}

	// for i, d := range data {
	// 	if d == '\n' {
	// 		data[i] = ' '
	// 	}
	// }

	return data
}

// Run current system command
func runCmd(cmd string) {

	// Exec os commands.
	var output bytes.Buffer

	c := exec.Command(os.Getenv("SHELL"), "-c", cmd)
	c.Stdout = &output
	c.Stderr = &output
	c.Run()
	fmt.Printf("%s\n", output.String())

}

// remove ' ' or '\t'
func BlankTrim(data []byte) []byte {

	var l, r int
	r = len(data)

	for {
		// ' ' or \t
		if data[l] != ' ' && data[l] != '\t' {
			break
		}

		if l < r {
			l++
		}
	}

	if l < r {

		for {
			// ' ' or \t
			if data[r-1] != ' ' && data[r-1] != '\t' {
				break
			}

			if r > l {
				r--
			}

		}
	}

	// fmt.Printf("l:%d, r:%d, len:%d\n", l, r, len(data))
	return data[l:r]
}

// Ideally, first is \W\w
func GetBlankLeft(data []byte) string {

	i := 0

	for l := len(data); i < l; i++ {
		// ' ' or \t
		if data[i] == ' ' || data[i] == '\t' {
			break
		}
	}

	if i > 0 {
		return string(data[0:i])
	} else {
		return ""
	}

}

// Ideally, first is \W\w
func getBlankLeft(data []byte) string {
	return strings.ToLower(GetBlankLeft(data))
}

func RemoveFatBlank(data []byte) []byte {

	var arr []byte
	var keep = true

	for _, v := range data {

		if keep {

			if v == ' ' {
				keep = false
			} else if v == '\t' {
				v = ' '
				keep = false
			}

			if len(arr) == 0 && !keep {
				continue
			}

			arr = append(arr, v)

		} else {
			if v != ' ' && v != '\t' {
				arr = append(arr, v)
				keep = true
			}
		}
	}

	return arr
}

func ExitNormal() {
	fmt.Println("Bye.")
	os.Exit(0)
}
