package main

import (
    "database/sql"
    "flag"
    "fmt"
    "os"
    "strconv"
    "sync"
    // "time"
    _ "github.com/go-sql-driver/mysql"
)

// function checkThreads returns the sum of Threads_cached and Threads_running
func checkThreads(dbh *sql.DB) (int, error) {
    threads := 0
    rows, err := dbh.Query("show status like 'Threads%';");
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
    }
    defer rows.Close()

    for rows.Next() {
        var key string
        var val sql.RawBytes

        if err = rows.Scan(&key, &val); err != nil {
            fmt.Fprintln(os.Stderr, err)
        } else if key == "Threads_cached" || key == "Threads_running" {
            fmt.Fprintln(os.Stdout, string(key)+": "+string(val))
            v, _ := strconv.Atoi(string(val))
            threads += v
        }
    }
    return threads, err
}

func main () {
    var (
        host       string
        user       string
        password   string
        port       int
        cache_num  int
    )
    flag.StringVar(&host, "h", "localhost", "mysql host")
    flag.StringVar(&user, "u", "root", "mysql username")
    flag.StringVar(&password, "p", "password", "mysql user password")
    flag.IntVar(&port, "P", 3306, "mysql port")
    flag.IntVar(&cache_num, "n", 3600, "number of cache to create")
    flag.Parse()

    dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?interpolateParams=true", user, password, host, port)
    db, err := sql.Open("mysql", dsn)
    db.SetMaxOpenConns(cache_num)
    db.SetMaxIdleConns(cache_num)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    defer db.Close()

    wg := &sync.WaitGroup{}
    for j := 0; j < 5; j++ {
        threads, err := checkThreads(db)
        if err != nil {
            fmt.Fprintln(os.Stderr, err)
            break
        }
        if threads >= cache_num {
            fmt.Fprintln(os.Stdout, "OK")
            break
        }

        for i := 0; i < cache_num; i++ {
            wg.Add(1)
            go func () {
                db.Query("do select 1; do select sleep(5); do select 1;")
                wg.Done()
            }()
        }
    }
    wg.Wait()
}
