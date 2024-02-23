package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// default level for logger.
const (
	logInfoLabel  = "[INFO] "
	logTraceLabel = "[TRACE] "
	logWarnLabel  = "[WARN] "
	logErrorLabel = "[ERROR] "
)

// the LOG+ which provides connection-based log.
type loggerPlus struct {
	logger *log.Logger
}

func NewLoggerPlus(l *log.Logger) Logger {
	return &loggerPlus{logger: l}
}

func (v *loggerPlus) Println(a ...interface{}) {
	args := v.contextFormat(a...)
	v.doPrintln(args...)
}

func (v *loggerPlus) Printf(format string, a ...interface{}) {
	format, args := v.contextFormatf(format, a...)
	v.doPrintf(format, args...)
}

func (v *loggerPlus) contextFormat(a ...interface{}) []interface{} {
	_, callerFile, line, _ := runtime.Caller(3)
	shortName := filepath.Base(callerFile)

	return append([]interface{}{fmt.Sprintf("[%v][%v] [%s:%d]: ", os.Getpid(), GetGoId(), shortName, line)}, a...)
}

func (v *loggerPlus) contextFormatf(format string, a ...interface{}) (string, []interface{}) {
	_, callerFile, line, _ := runtime.Caller(3)
	shortName := filepath.Base(callerFile)

	return "[%v][%v] [%s:%d]: " + format, append([]interface{}{os.Getpid(), GetGoId(), shortName, line}, a...)
}

func (v *loggerPlus) format(a ...interface{}) []interface{} {
	_, callerFile, line, _ := runtime.Caller(3)
	shortName := filepath.Base(callerFile)

	return append([]interface{}{fmt.Sprintf("[%v][%v] [%s:%d]: ", os.Getpid(), GetGoId(), shortName, line)}, a...)
}

func (v *loggerPlus) formatf(format string, a ...interface{}) (string, []interface{}) {
	_, callerFile, line, _ := runtime.Caller(3)
	shortName := filepath.Base(callerFile)

	return "[%v][%v] [%s:%d]: " + format, append([]interface{}{os.Getpid(), GetGoId(), shortName, line}, a...)
}

var colorYellow = "\033[33m"
var colorRed = "\033[31m"
var colorBlack = "\033[0m"

func (v *loggerPlus) doPrintln(args ...interface{}) {
	if previousCloser == nil {
		if v == ErrorLogger {
			fmt.Fprintf(os.Stdout, colorRed)
			v.logger.Println(args...)
			fmt.Fprintf(os.Stdout, colorBlack)
		} else if v == WarnLogger {
			fmt.Fprintf(os.Stdout, colorYellow)
			v.logger.Println(args...)
			fmt.Fprintf(os.Stdout, colorBlack)
		} else if v == InfoLogger {
			fmt.Fprintf(os.Stdout, colorBlack)
			v.logger.Println(args...)
			fmt.Fprintf(os.Stdout, colorBlack)
		} else if v == TraceLogger {
			fmt.Fprintf(os.Stdout, colorBlack)
			v.logger.Println(args...)
			fmt.Fprintf(os.Stdout, colorBlack)
		} else {
			v.logger.Println(args...)
		}
	} else {
		v.logger.Println(args...)
	}
}

func (v *loggerPlus) doPrintf(format string, args ...interface{}) {
	if previousCloser == nil {
		if v == ErrorLogger {
			fmt.Fprintf(os.Stdout, colorRed)
			v.logger.Printf(format, args...)
			fmt.Fprintf(os.Stdout, colorBlack)
		} else if v == WarnLogger {
			fmt.Fprintf(os.Stdout, colorYellow)
			v.logger.Printf(format, args...)
			fmt.Fprintf(os.Stdout, colorBlack)
		} else if v == InfoLogger {
			fmt.Fprintf(os.Stdout, colorBlack)
			v.logger.Printf(format, args...)
			fmt.Fprintf(os.Stdout, colorBlack)
		} else if v == TraceLogger {
			fmt.Fprintf(os.Stdout, colorBlack)
			v.logger.Printf(format, args...)
			fmt.Fprintf(os.Stdout, colorBlack)
		} else {
			v.logger.Printf(format, args...)
		}
	} else {
		v.logger.Printf(format, args...)
	}
}

var InfoLogger Logger

func Info(a ...interface{}) {
	InfoLogger.Println(a...)
}

func Infof(format string, a ...interface{}) {
	InfoLogger.Printf(format, a...)
}

var TraceLogger Logger

func Trace(a ...interface{}) {
	TraceLogger.Println(a...)
}

func Tracef(format string, a ...interface{}) {
	TraceLogger.Printf(format, a...)
}

var WarnLogger Logger

func Warn(a ...interface{}) {
	WarnLogger.Println(a...)
}

func Warnf(format string, a ...interface{}) {
	WarnLogger.Printf(format, a...)
}

var ErrorLogger Logger

func Error(a ...interface{}) {
	ErrorLogger.Println(a...)
}

func Errorf(format string, a ...interface{}) {
	ErrorLogger.Printf(format, a...)
}

type Logger interface {
	Println(a ...interface{})
	Printf(format string, a ...interface{})
}

func init() {
	InfoLogger = NewLoggerPlus(log.New(os.Stdout, logInfoLabel, log.Ldate|log.Ltime|log.Lmicroseconds))
	TraceLogger = NewLoggerPlus(log.New(os.Stdout, logTraceLabel, log.Ldate|log.Ltime|log.Lmicroseconds))
	WarnLogger = NewLoggerPlus(log.New(os.Stderr, logWarnLabel, log.Ldate|log.Ltime|log.Lmicroseconds))
	ErrorLogger = NewLoggerPlus(log.New(os.Stderr, logErrorLabel, log.Ldate|log.Ltime|log.Lmicroseconds))

	// init writer and closer.
	previousWriter = os.Stdout
	previousCloser = nil
}

func Switch(w io.Writer) io.Writer {
	// TODO: support level, default to trace here.
	InfoLogger = NewLoggerPlus(log.New(w, logInfoLabel, log.Ldate|log.Ltime|log.Lmicroseconds))
	TraceLogger = NewLoggerPlus(log.New(w, logTraceLabel, log.Ldate|log.Ltime|log.Lmicroseconds))
	WarnLogger = NewLoggerPlus(log.New(w, logWarnLabel, log.Ldate|log.Ltime|log.Lmicroseconds))
	ErrorLogger = NewLoggerPlus(log.New(w, logErrorLabel, log.Ldate|log.Ltime|log.Lmicroseconds))

	ow := previousWriter
	previousWriter = w

	if c, ok := w.(io.Closer); ok {
		previousCloser = c
	}

	return ow
}

var previousCloser io.Closer
var previousWriter io.Writer

func Close() (err error) {
	InfoLogger = NewLoggerPlus(log.New(io.Discard, logInfoLabel, log.Ldate|log.Ltime|log.Lmicroseconds))
	TraceLogger = NewLoggerPlus(log.New(io.Discard, logTraceLabel, log.Ldate|log.Ltime|log.Lmicroseconds))
	WarnLogger = NewLoggerPlus(log.New(io.Discard, logWarnLabel, log.Ldate|log.Ltime|log.Lmicroseconds))
	ErrorLogger = NewLoggerPlus(log.New(io.Discard, logErrorLabel, log.Ldate|log.Ltime|log.Lmicroseconds))

	if previousCloser != nil {
		err = previousCloser.Close()
		previousCloser = nil
	}

	return
}

func GetGoId() int64 {
	var (
		buf [64]byte
		n   = runtime.Stack(buf[:], false)
		stk = strings.TrimPrefix(string(buf[:n]), "goroutine")
	)

	idField := strings.Fields(stk)[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		return -1
	}

	return int64(id)
}
