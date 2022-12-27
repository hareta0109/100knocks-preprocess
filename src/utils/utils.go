package utils

import (
	"os"
	"strings"

	"github.com/go-gota/gota/dataframe"
)

func CsvToDataframe(path string) (dataframe.DataFrame, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return dataframe.New(), err
	}
	ioContent := strings.NewReader(string(content))
	df := dataframe.ReadCSV(ioContent)
	return df, nil
}
