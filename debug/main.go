package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	checkFile("a.csv", "|")
}

func checkFile(fileName string, sep string) {
	var headerFields int
	file, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	if scanner.Scan() {
		headerFields = len(strings.Split(scanner.Text(), sep))
	} else {
		fmt.Println("Empty file or unable to read header.")
		return
	}

	lineNumber := 1

	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		if len(strings.Split(line, sep)) != headerFields {
			fmt.Printf("Unwanted separator found on line %d\n", lineNumber)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading the file:", err)
	}
}
