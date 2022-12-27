//go:build ignore

package main

import (
	"100knocks-preprocess/src/utils"
	"fmt"
	"log"
	"regexp"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func main() {
	fmt.Println("=== P-010 ===")
	P010()

	fmt.Println("=== P-011 ===")
	P011()

	fmt.Println("=== P-012 ===")
	P012()

	fmt.Println("=== P-013 ===")
	P013()

	fmt.Println("=== P-014 ===")
	P014()

	fmt.Println("=== P-015 ===")
	P015()

	fmt.Println("=== P-016 ===")
	P016()
}

func P010() {
	dfStore, err := utils.CsvToDataframe("../../data/store.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfStore.
		Filter(
			dataframe.F{
				Colname:    "store_cd",
				Comparator: series.CompFunc,
				Comparando: func(el series.Element) bool {
					regex := regexp.MustCompile(`^S14`)
					return regex.MatchString(el.String())
				},
			},
		)
	fmt.Println(df)
	return
}

func P011() {
	dfCustomer, err := utils.CsvToDataframe("../../data/customer.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfCustomer.
		Filter(
			dataframe.F{
				Colname:    "customer_id",
				Comparator: series.CompFunc,
				Comparando: func(el series.Element) bool {
					regex := regexp.MustCompile(`1$`)
					return regex.MatchString(el.String())
				},
			},
		)
	fmt.Println(df)
	return
}

func P012() {
	dfStore, err := utils.CsvToDataframe("../../data/store.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfStore.
		Filter(
			dataframe.F{
				Colname:    "address",
				Comparator: series.CompFunc,
				Comparando: func(el series.Element) bool {
					regex := regexp.MustCompile(`横浜市`)
					return regex.MatchString(el.String())
				},
			},
		)
	fmt.Println(df)
	return
}

func P013() {
	dfCustomer, err := utils.CsvToDataframe("../../data/customer.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfCustomer.
		Filter(
			dataframe.F{
				Colname:    "status_cd",
				Comparator: series.CompFunc,
				Comparando: func(el series.Element) bool {
					regex := regexp.MustCompile(`^[A-F]`)
					return regex.MatchString(el.String())
				},
			},
		)
	fmt.Println(df)
	return
}

func P014() {
	dfCustomer, err := utils.CsvToDataframe("../../data/customer.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfCustomer.
		Filter(
			dataframe.F{
				Colname:    "status_cd",
				Comparator: series.CompFunc,
				Comparando: func(el series.Element) bool {
					regex := regexp.MustCompile(`[1-9]$`)
					return regex.MatchString(el.String())
				},
			},
		)
	fmt.Println(df)
	return
}

func P015() {
	dfCustomer, err := utils.CsvToDataframe("../../data/customer.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfCustomer.
		FilterAggregation(
			dataframe.And,
			dataframe.F{
				Colname:    "status_cd",
				Comparator: series.CompFunc,
				Comparando: func(el series.Element) bool {
					regex := regexp.MustCompile(`^[A-F]`)
					return regex.MatchString(el.String())
				},
			},
			dataframe.F{
				Colname:    "status_cd",
				Comparator: series.CompFunc,
				Comparando: func(el series.Element) bool {
					regex := regexp.MustCompile(`[1-9]$`)
					return regex.MatchString(el.String())
				},
			},
		)
	fmt.Println(df)
	return
}

func P016() {
	dfStore, err := utils.CsvToDataframe("../../data/store.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfStore.
		Filter(
			dataframe.F{
				Colname:    "tel_no",
				Comparator: series.CompFunc,
				Comparando: func(el series.Element) bool {
					regex := regexp.MustCompile(`^[0-9]{3}-[0-9]{3}-[0-9]{4}$`)
					return regex.MatchString(el.String())
				},
			},
		)
	fmt.Println(df)
	return
}
