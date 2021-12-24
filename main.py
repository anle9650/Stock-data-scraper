import pandas as pd
import numpy as np
import requests
import time

if __name__ == '__main__':

    features = ['ticker',
                'calendarDate',
                'accumulatedOtherComprehensiveIncome',
                'assets',
                'assetsCurrent',
                'assetsNonCurrent',
                'bookValuePerShare',
                'capitalExpenditure',
                'cashAndEquivalents',
                'cashAndEquivalentsUSD',
                'costOfRevenue',
                'consolidatedIncome',
                'currentRatio',
                'debtToEquityRatio',
                'debt',
                'debtCurrent',
                'debtNonCurrent',
                'debtUSD',
                'deferredRevenue',
                'depreciationAmortizationAndAccretion',
                'deposits',
                'dividendYield',
                'dividendsPerBasicCommonShare',
                'earningBeforeInterestTaxes',
                'earningsBeforeInterestTaxesDepreciationAmortization',
                'EBITDAMargin',
                'earningsBeforeInterestTaxesDepreciationAmortizationUSD',
                'earningBeforeInterestTaxesUSD',
                'earningsBeforeTax',
                'earningsPerBasicShare',
                'earningsPerDilutedShare',
                'earningsPerBasicShareUSD',
                'shareholdersEquity',
                'shareholdersEquityUSD',
                'enterpriseValue',
                'enterpriseValueOverEBIT',
                'enterpriseValueOverEBITDA',
                'freeCashFlow',
                'freeCashFlowPerShare',
                'foreignCurrencyUSDExchangeRate',
                'grossProfit',
                'grossMargin',
                'goodwillAndIntangibleAssets',
                'interestExpense',
                'investedCapital',
                'inventory',
                'investments',
                'investmentsCurrent',
                'investmentsNonCurrent',
                'totalLiabilities',
                'currentLiabilities',
                'liabilitiesNonCurrent',
                'marketCapitalization',
                'netCashFlow',
                'netCashFlowBusinessAcquisitionsDisposals',
                'issuanceEquityShares',
                'issuanceDebtSecurities',
                'paymentDividendsOtherCashDistributions',
                'netCashFlowFromFinancing',
                'netCashFlowFromInvesting',
                'netCashFlowInvestmentAcquisitionsDisposals',
                'netCashFlowFromOperations',
                'effectOfExchangeRateChangesOnCash',
                'netIncome',
                'netIncomeCommonStock',
                'netIncomeCommonStockUSD',
                'netLossIncomeFromDiscontinuedOperations',
                'netIncomeToNonControllingInterests',
                'profitMargin',
                'operatingExpenses',
                'operatingIncome',
                'tradeAndNonTradePayables',
                'payoutRatio',
                'priceToBookValue',
                'priceEarnings',
                'priceToEarningsRatio',
                'propertyPlantEquipmentNet',
                'preferredDividendsIncomeStatementImpact',
                'sharePriceAdjustedClose',
                'priceSales',
                'priceToSalesRatio',
                'tradeAndNonTradeReceivables',
                'accumulatedRetainedEarningsDeficit',
                'revenues',
                'revenuesUSD',
                'researchAndDevelopmentExpense',
                'shareBasedCompensation',
                'sellingGeneralAndAdministrativeExpense',
                'shareFactor',
                'shares',
                'weightedAverageShares',
                'weightedAverageSharesDiluted',
                'salesPerShare',
                'tangibleAssetValue',
                'taxAssets',
                'incomeTaxExpense',
                'taxLiabilities',
                'tangibleAssetsBookValuePerShare',
                'workingCapital']

    calculated_features = ["change_leverage", "change_curr_ratio", "change_shares", "change_gross_margin"]

    target_feature = "price_change"

    # try to load financial data from the last stopping point
    try:
        data = pd.read_csv("stock_financials_samples.csv")
        judge = pd.read_csv("stock_financials_judge.csv")

    # if data does not exist, create new dataframes
    except FileNotFoundError:
        data = pd.DataFrame(columns=features + calculated_features + [target_feature])
        judge = pd.DataFrame(columns=features + calculated_features)

    # get stock symbols for all S&P 500 stocks
    sp_500_f = open("sp_500.txt", "r")
    sp_500 = sp_500_f.read()
    symbols = sp_500.split("\n")
    sp_500_f.close()

    if len(judge.index) > 0:
        # get the symbol of the last stock collected
        lastSymbol = judge.iloc[-1, 0]

        # get the index of the last stock collected
        lastIndex = symbols.index(lastSymbol)

        # slice the symbols list after the index of the last stock collected
        symbols = symbols[lastIndex + 1:]

    # collect financial data for each stock left in the list
    print("Stocks scraped: ")
    for symbol in symbols:

        # request the stock's last 22 quarters financial data from API
        response = requests.get("https://api.polygon.io/v2/reference/financials/" + symbol +
                                "?limit=22&type=Q&apiKey=treIjRq9AdkGpNHEdWce1jIBNFCFrHel")

        # if maximum requests reached, wait 1 minute and try again
        if response.status_code == 429:
            time.sleep(60)
            response = requests.get("https://api.polygon.io/v2/reference/financials/" + symbol +
                                    "?limit=22&type=Q&apiKey=treIjRq9AdkGpNHEdWce1jIBNFCFrHel")

        # if response received, get and store financial data
        if response.status_code == 200:
            # get results in json format
            results = response.json()['results']

            # get each year's financial data
            for i in range(len(results) - 1):
                sample = {}

                for feature in features:
                    try:
                        value = results[i][feature]
                    except KeyError:
                        value = np.nan
                    sample[feature] = value

                # change in leverage
                try:
                    curr_leverage = results[i]['debt']
                    prev_leverage = results[i + 1]['debt']
                    change_leverage = curr_leverage - prev_leverage
                except KeyError:
                    change_leverage = np.nan
                sample["change_leverage"] = change_leverage

                # change in current ratio
                try:
                    curr_ratio = results[i]['currentRatio']
                    prev_ratio = results[i + 1]['currentRatio']
                    change_curr_ratio = curr_ratio - prev_ratio
                except KeyError:
                    change_curr_ratio = np.nan
                sample["change_curr_ratio"] = change_curr_ratio

                # change in shares
                try:
                    curr_shares = results[i]['shares']
                    prev_shares = results[i + 1]['shares']
                    change_shares = curr_shares - prev_shares
                except KeyError:
                    change_shares = np.nan
                sample["change_shares"] = change_shares

                # change in gross margin
                try:
                    curr_gross_margin = results[i]['grossMargin']
                    prev_gross_margin = results[i + 1]['grossMargin']
                    change_gross_margin = curr_gross_margin - prev_gross_margin
                except KeyError:
                    change_gross_margin = np.nan
                sample["change_gross_margin"] = change_gross_margin

                # if currently on the most recent financial data, store sample in judgement sample
                if i == 0:
                    # store sample in dataframe
                    judge = judge.append(sample, ignore_index=True)

                # else, calculate price_change, and store sample in the training/testing sample
                else:
                    # change in price (target feature)
                    try:
                        curr_price = results[i]['sharePriceAdjustedClose']
                        next_price = results[i - 1]['sharePriceAdjustedClose']
                        price_change = (next_price - curr_price) / curr_price
                    except KeyError or ZeroDivisionError:
                        price_change = np.nan
                    sample[target_feature] = price_change

                    # store sample in dataframe
                    data = data.append(sample, ignore_index=True)

            # save dataframe to csv
            judge.to_csv('stock_financials_judge.csv', index=False)
            data.to_csv('stock_financials_samples.csv', index=False)

            print(symbol)
