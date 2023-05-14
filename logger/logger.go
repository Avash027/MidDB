package logger

import (
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
)

var logs zerolog.Logger

func LoggerInit(mode string) *os.File {
	file, _ := os.OpenFile("logrus.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		shortFile := file
		count := 0
		for i := len(file) - 1; i >= 0; i-- {
			if file[i] == '/' {
				count++
				if count == 2 {
					shortFile = file[i+1:]
					break
				}
			}
		}

		return fmt.Sprintf("%s:%d", shortFile, line)
	}

	if mode == "LOCAL" {
		output := zerolog.ConsoleWriter{Out: os.Stdout}
		zerolog.SetGlobalLevel(zerolog.DebugLevel)

		output.FormatTimestamp = func(i interface{}) string {
			return ""
		}

		output.FormatCaller = func(i interface{}) string {
			return fmt.Sprintf("[%s]", i)
		}
		logs = zerolog.New(output).With().Logger()

	} else if mode == "PROD" {
		output := zerolog.ConsoleWriter{Out: file, TimeFormat: time.RFC3339}

		output.FormatCaller = func(i interface{}) string {
			return fmt.Sprintf("[%s]", i)
		}
		output.NoColor = true

		zerolog.SetGlobalLevel(zerolog.WarnLevel)
		logs = zerolog.New(output).With().Timestamp().Logger()

	} else {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	return file

}

func GetLogger() zerolog.Logger {
	return logs.With().Caller().Logger()
}
