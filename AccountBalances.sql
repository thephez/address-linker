SELECT Account, Balance, (SUM(Balance)*1.0/100000000) FROM Addresses WHERE balance > 0
GROUP BY Account
