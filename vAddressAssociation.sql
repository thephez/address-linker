CREATE VIEW AddressAssociations AS 
	SELECT (SELECT Account FROM Addresses WHERE AddressInputs.Address = Addresses.Address) AS 'Related to Account', 
	'In' AS 'Direction', 
	InputAddress AS 'In/Out Address', 
	TXID, BlockHeight, 
	Address 
	FROM AddressInputs 
		UNION 
	SELECT (SELECT Account FROM Addresses WHERE AddressOutputs.Address = Addresses.Address) AS 'Related to Account',
	'Out' AS 'Direction',
	OutputAddress AS 'In/Out Address',
	TXID,
	BlockHeight,
	Address 
	FROM AddressOutputs