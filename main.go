package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/gocarina/gocsv"
	"os"
	"strings"
	"time"
)

type CreditCardStatementRows []CreditCardStatementRow

type CreditCardStatementRaw struct {
	UserID                       string `csv:"userId"`
	StatementDate                string `csv:"statementDate"`
	PdfPath                      string `csv:"pdfPath"`
	ClientInfo                   string `csv:"clientInfo"`
	StatementInfo                string `csv:"statementInfo"`
	CreditInfo                   string `csv:"creditInfo"`
	FinanceInfo                  string `csv:"financeInfo"`
	ComisionInfo                 string `csv:"comisionInfo"`
	BalanceSummaryInfo           string `csv:"balanceSummaryInfo"`
	SummaryInfo                  string `csv:"summaryInfo"`
	TransactionDetailSummaryInfo string `csv:"transactionDetailSummaryInfo"`
}

type CreditCardStatementRow struct {
	UserID         string
	StatementDate  time.Time
	CycleEndDate   time.Time
	CycleStartDate time.Time
	PdfPath        string
	XmlPath        string
	ClientInfo     struct {
		AccountID           string `json:"accountId"`
		ExternalContractID  string `json:"contractId"`
		ExternalCardID      string `json:"cardId"`
		Email               string `json:"email"`
		FirstName           string `json:"firstName"`
		IdentificationType  string `json:"identificationType"`
		IdentificationValue string `json:"identificationValue"`
		LastName            string `json:"lastName"`
		LastName2           string `json:"lastName2"`
		MiddleName          string `json:"middleName"`
		ProductCode         string `json:"productCode"`
		ReferenceCustID     string `json:"referenceCustId"`
	}
	StatementInfo struct {
		Cycle          string  `json:"cycle"`
		DaysInCycle    string  `json:"daysInCycle"`
		DueDate        string  `json:"dueDate"`
		MinimumPayment float32 `json:"miniPayment,string"`
		StatementDate  string  `json:"statementDate"`
		TotalPayment   float32 `json:"endingBalance,string"`
		CycleGroup     string  `json:"cycleGroup"`
	}
	CreditInfo struct {
		AvailableCredit float32 `json:"availableCredit,string"`
		CreditLimit     float32 `json:"creditLimit,string"`
		Overdraft       float32 `json:"overdraft,string"`
		Overdue         float32 `json:"overdue,string"`
		Purchases       float32 `json:"purchases,string"`
	}
	FinanceInfo struct {
		AnnualPercRateInt     float32 `json:"annualPercentageRateOfInterest,string"`
		AvgDailyBalance       float32 `json:"averageDailyBalance,string"`
		CATWithoutVAT         float32 `json:"catWithoutVat,string"`
		MonthlyAmountFinanced float32 `json:"monthlyAmountFinanced,string"`
		MonthlyInterestRate   float32 `json:"monthlyInterestRate,string"`
		MonthWithMinPay       float32 `json:"monthWithMinPay,string"`
		NonTaxableInterests   float32 `json:"nonTaxableInterests,string"`
		TaxableInterests      float32 `json:"taxableInterest,string"`
		TotalChargedInterests float32 `json:"totalChargedInterests,string"`
	}
	ComisionInfo struct {
		AnnualFee          float32 `json:"annualFee,string"`
		CardReplacementFee float32 `json:"cardReplacementFee,string"`
		CashWithdrawalFee  float32 `json:"cashWithdrawalFee,string"`
		LateFee            float32 `json:"lateFee,string"`
		OtherFees          float32 `json:"otherFees,string"`
		TotalFees          float32 `json:"totalFees,string"`
	}
	BalanceSummaryInfo struct {
		CashWithdrawals float32 `json:"cashWithdrawals,string"`
		Credits         float32 `json:"credits,string"`
		Debits          float32 `json:"debits,string"`
		EndingBalance   float32 `json:"endingBalance,string"`
		Fees            float32 `json:"fees,string"`
		InitialBalance  float32 `json:"initialBalance,string"`
		Interest        float32 `json:"interest,string"`
		Payments        float32 `json:"payments,string"`
		Purchases       float32 `json:"purchases,string"`
		VAT             float32 `json:"vat,string"`
	}
	SummaryInfo struct {
		AvailableCredit float32 `json:"availableCredit,string"`
		CreditLine      float32 `json:"creditLine,string"`
		Debits          float32 `json:"debits,string"`
		Fees            float32 `json:"fees,string"`
		Interests       float32 `json:"interests,string"`
		PreviousBalance float32 `json:"previousBalance,string"`
	}
	TransactionDetailSummaryInfo struct {
		TotalCreditAmount     float32 `json:"totalCreditAmount,string"`
		TotalCreditCurrency   string  `json:"totalCreditCurrency"`
		TotalDebitAmount      float32 `json:"totalDebitAmount,string"`
		TotalDebitCurrency    string  `json:"totalDebitCurrency"`
		TransactionDetailList []struct {
			TransactionId           string  `json:"transactionId"`
			OriginalId              string  `json:"originalId"`
			EffectiveTime           uint64  `json:"effectiveTime"`
			EffectiveDay            string  `json:"effectiveDay"`
			PostTime                uint64  `json:"postTime"`
			PostDay                 string  `json:"postDay" validate:"len=8"`
			Type                    string  `json:"type"`
			SubType                 string  `json:"subType"`
			CardId                  string  `json:"cardId"`
			MCC                     string  `json:"mcc"`
			MerchantName            string  `json:"merchantName"`
			MerchantAddress         string  `json:"merchantAddress"`
			TerminalType            string  `json:"terminalType"`
			Status                  string  `json:"status"`
			BillingCurrencyCode     string  `json:"billingCurrencyCode"`
			BillingAmount           float32 `json:"billingAmount,string"`
			ExchangeRate            string  `json:"exchangeRate"`
			Country                 string  `json:"string"`
			TransactionAmount       float32 `json:"transactionAmount,string"`
			TransactionCurrencyCode string  `json:"transactionCurrencyCode"`
			Direction               string  `json:"direction"`
		} `json:"transactionDetailList"`
	}
}

func main() {
	// Open the file
	file, err := os.Open("0ac7fcd34a5401ab199b53b678bcb29f__018_of_021.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Create a new reader
	reader := csv.NewReader(file)
	reader.Comma = '|'
	reader.LazyQuotes = true

	var state = CreditCardStatementRaw{}
	unmarshaller, err := gocsv.NewUnmarshaller(reader, state)
	if err != nil {
		fmt.Println(err)
	}

	lines, err := readBatchOfLines(unmarshaller, 100000)
	if err != nil {
		fmt.Println(err)

	}

	rows, err := cleanRawRows(lines, 5000)
	if err != nil {
		return
	}
	// Process the records
	fmt.Println(len(rows))
}

// readBatchOfLines reads at most `batchSize` lines from a CSV file and appends the processed
// lines to a slice of the given interface in the gocsv.Unmarshaller
func readBatchOfLines(unmarshaller *gocsv.Unmarshaller, batchSize int) ([]any, error) {
	readRows := make([]any, 0, batchSize)
	counter := 0

	for {

		row, err := unmarshaller.Read()

		if err != nil {
			return readRows, err
		}

		readRows = append(readRows, row)
		counter++

		if counter == batchSize {
			break
		}
	}
	return readRows, nil
}

func cleanRawRows(rows []any, batchSize int) (CreditCardStatementRows, error) {

	cleanRows := make(CreditCardStatementRows, 0, batchSize)

	for _, row := range rows {
		cleanRow, err := cleanRawRow(row)
		if err != nil {
			return cleanRows, err
		}
		cleanRows = append(cleanRows, cleanRow)
	}

	return cleanRows, nil
}

func trimPrefixSuffix(chain string, char string) string {
	return strings.TrimSuffix(strings.TrimPrefix(chain, char), char)
}

// Some parameters are JSONs. Which are generated in another language, and it appends by default a `'` character (apostrophe)
// at the begining of each JSON. It is being removed with the `trimPrefixSuffix` function.
func cleanRawRow(row any) (CreditCardStatementRow, error) {
	var ccsr CreditCardStatementRow
	var err error

	err = json.Unmarshal([]byte(trimPrefixSuffix(row.(CreditCardStatementRaw).BalanceSummaryInfo, `"`)), &ccsr.BalanceSummaryInfo)
	if err != nil {
		return CreditCardStatementRow{}, fmt.Errorf("error at json BalanceSummaryInfo : %w", err)
	}

	err = json.Unmarshal([]byte(trimPrefixSuffix(row.(CreditCardStatementRaw).ClientInfo, `"`)), &ccsr.ClientInfo)
	if err != nil {
		return CreditCardStatementRow{}, fmt.Errorf("error at json ClientInfo : %w", err)
	}

	err = json.Unmarshal([]byte(trimPrefixSuffix(row.(CreditCardStatementRaw).ComisionInfo, `"`)), &ccsr.ComisionInfo)
	if err != nil {
		return CreditCardStatementRow{}, fmt.Errorf("error at json ComisionInfo : %w", err)
	}

	err = json.Unmarshal([]byte(trimPrefixSuffix(row.(CreditCardStatementRaw).CreditInfo, `"`)), &ccsr.CreditInfo)
	if err != nil {
		return CreditCardStatementRow{}, fmt.Errorf("error at json CreditInfo : %w", err)
	}

	err = json.Unmarshal([]byte(trimPrefixSuffix(row.(CreditCardStatementRaw).FinanceInfo, `"`)), &ccsr.FinanceInfo)
	if err != nil {
		return CreditCardStatementRow{}, fmt.Errorf("error at json FinanceInfo : %w", err)
	}

	err = json.Unmarshal([]byte(trimPrefixSuffix(row.(CreditCardStatementRaw).StatementInfo, `"`)), &ccsr.StatementInfo)
	if err != nil {
		return CreditCardStatementRow{}, fmt.Errorf("error at json StatementInfo : %w", err)
	}

	err = json.Unmarshal([]byte(trimPrefixSuffix(row.(CreditCardStatementRaw).SummaryInfo, `"`)), &ccsr.SummaryInfo)
	if err != nil {
		return CreditCardStatementRow{}, fmt.Errorf("error at json SummaryInfo : %w", err)
	}

	err = json.Unmarshal([]byte(trimPrefixSuffix(row.(CreditCardStatementRaw).TransactionDetailSummaryInfo, `"`)), &ccsr.TransactionDetailSummaryInfo)
	if err != nil {
		return CreditCardStatementRow{}, fmt.Errorf("error at json TransactionDetailSummaryInfo : %w", err)
	}

	ccsr.PdfPath = row.(CreditCardStatementRaw).PdfPath
	ccsr.UserID = row.(CreditCardStatementRaw).UserID

	// date at which the statement DAG run
	ccsr.StatementDate = time.Now()

	// statement cycle start date
	if ccsr.CycleStartDate, err = dateFromCycle(ccsr.StatementInfo.Cycle, 0); err != nil {
		return CreditCardStatementRow{}, fmt.Errorf("error formatting cycle start date as time.Time : %w", err)
	}

	// statement cycle end date
	if ccsr.CycleEndDate, err = dateFromCycle(ccsr.StatementInfo.Cycle, 1); err != nil {
		return CreditCardStatementRow{}, fmt.Errorf("error formatting cycle end date as time.Time : %w", err)
	}

	return ccsr, nil
}

// dateFromCycle splits the cycle (start-end) as "yyyymmdd-yyyymmdd" and generates a time object from it
// the `startOrEnd` variable is the index after splitting the cycle
func dateFromCycle(cycle string, startOrEnd int) (time.Time, error) {
	date := strings.Split(cycle, "-")[startOrEnd]
	return time.Parse("20060102", date)
}
