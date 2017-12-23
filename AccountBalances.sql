SELECT Account, SUM(Balance) AS 'Balance (satoshis)', (SUM(Balance)*1.0/100000000) AS 'Balance (BTC)' FROM Addresses WHERE balance > 0
GROUP BY Account
