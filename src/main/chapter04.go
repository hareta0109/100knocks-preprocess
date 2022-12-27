//go:build ignore

package main

import (
	"100knocks-preprocess/src/utils"
	"fmt"
	"log"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func main() {
	fmt.Println("=== P-017 ===")
	P017()

	fmt.Println("=== P-018 ===")
	P018()

	fmt.Println("=== P-019 ===")
	P019()

	fmt.Println("=== P-020 ===")
	P020()
}

func P017() {
	dfCustomer, err := utils.CsvToDataframe("../../data/customer.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfCustomer.Arrange(
		dataframe.Sort("birth_day"),
	)
	fmt.Println(df)
	return
}

func P018() {
	dfCustomer, err := utils.CsvToDataframe("../../data/customer.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfCustomer.Arrange(
		dataframe.RevSort("birth_day"),
	)
	fmt.Println(df)
	return
}

func P019() {
	dfReceipt, err := utils.CsvToDataframe("../../data/receipt.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfReceipt.
		Arrange(
			dataframe.RevSort("amount"),
		).Select([]string{"customer_id", "amount"})
	sliceRank := make([]int, df.Nrow())
	sliceAmount, err := df.Col("amount").Int()
	if err != nil {
		return
	}
	for i := 0; i < df.Nrow(); i++ {
		sliceRank[i] = i + 1
		if i != 0 {
			if sliceAmount[i] == sliceAmount[i-1] {
				sliceRank[i] = sliceRank[i-1]
			}
		}
	}
	df = df.Mutate(
		series.New(sliceRank, series.Int, "rank"),
	)
	fmt.Println(df)
	return
}

func P020() {
	dfReceipt, err := utils.CsvToDataframe("../../data/receipt.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfReceipt.
		Arrange(
			dataframe.RevSort("amount"),
		).Select([]string{"customer_id", "amount"})
	sliceRank := make([]int, df.Nrow())
	for i := 0; i < df.Nrow(); i++ {
		sliceRank[i] = i + 1
	}
	df = df.Mutate(
		series.New(sliceRank, series.Int, "rank"),
	)
	fmt.Println(df)
	return
}