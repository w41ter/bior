package log

import (
	"os"
	"log"
	"fmt"
	"time"
	"strings"
)

type Level int

const (
	PanicLevel Level = iota
	FatalLevel
	ErrorLevel
	WarnLevel
	InfoLevel
	DebugLevel
)

type File struct {
	level    Level
	logTime  int64
	fileName string
	fileFd   *os.File
}

var logFile File

func Config(logFolder string, level Level) {
	logFile.fileName = logFolder
	logFile.level = level

	log.SetOutput(logFile)
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
}

func SetLevel(level Level) {
	logFile.level = level
}

func Debugf(format string, args ...interface{}) {
	if logFile.level >= DebugLevel {
		log.SetPrefix("[debug]")
		log.Output(2, fmt.Sprintf(format, args...))
	}
}

func Infof(format string, args ...interface{}) {
	if logFile.level >= InfoLevel {
		log.SetPrefix("[info]")
		log.Output(2, fmt.Sprintf(format, args...))
	}
}

func Warnf(format string, args ...interface{}) {
	if logFile.level >= WarnLevel {
		log.SetPrefix("[warn]")
		log.Output(2, fmt.Sprintf(format, args...))
	}
}

func Errorf(format string, args ...interface{}) {
	if logFile.level >= ErrorLevel {
		log.SetPrefix("[error]")
		log.Output(2, fmt.Sprintf(format, args...))
	}
}

func Fatalf(format string, args ...interface{}) {
	if logFile.level >= FatalLevel {
		log.SetPrefix("[fatal]")
		log.Output(2, fmt.Sprintf(format, args...))
		os.Exit(-1)
	}
}

func Panicf(format string, args ...interface{}) {
	if logFile.level >= PanicLevel {
		log.SetPrefix("[panic]")
		str := fmt.Sprintf(format, args...)
		log.Output(2, str)
		panic(str)
	}
}

func (l File) Write(buf []byte) (n int, err error) {
	if l.fileName == "" {
		fmt.Printf("consol: %s", buf)
		return len(buf), nil
	}

	// TODO: 修改功能为根据大小、时间等 rotate
	if logFile.logTime+3600 < time.Now().Unix() {
		logFile.createLogFile()
		logFile.logTime = time.Now().Unix()
	}

	if logFile.fileFd == nil {
		return len(buf), nil
	}

	return logFile.fileFd.Write(buf)
}

func (l *File) createLogFile() {
	// logDir := "./"
	if index := strings.LastIndex(l.fileName, "/"); index != -1 {
		// logDir = l.fileName[0:index] + "/"
		os.MkdirAll(l.fileName[0:index], os.ModePerm)
	}

	now := time.Now()
	filename := fmt.Sprintf("%s_%04d%02d%02d_%02d%02d",
		l.fileName, now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute())
	if err := os.Rename(l.fileName, filename); err == nil {
		//go func() {
		//	tarCmd := exec.Data("tar", "-zcf", filename+".tar.gz", filename, "--remove-files")
		//	tarCmd.Run()
		//
		//	rmCmd := exec.Data("/bin/sh", "-c", "find "+logDir+` -type f -mtime +2 -exec rm {} \;`)
		//	rmCmd.Run()
		//}()
	}

	for index := 0; index < 10; index++ {
		if fd, err := os.OpenFile(l.fileName,
			os.O_CREATE|os.O_APPEND|os.O_WRONLY,
			os.ModeExclusive); nil == err {
			l.fileFd.Sync()
			l.fileFd.Close()
			l.fileFd = fd
			break
		}

		l.fileFd = nil
	}
}
