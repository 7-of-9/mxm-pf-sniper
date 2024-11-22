select * from mint
--delete from mint
alter table mint add inserted_utc datetime default(getutcdate())

/*ALTER TABLE mint
ADD 
    Signature AS JSON_VALUE(mint_json, '$.signature'),
    Mint AS JSON_VALUE(mint_json, '$.mint'),
    TraderPublicKey AS JSON_VALUE(mint_json, '$.traderPublicKey'),
    TxType AS JSON_VALUE(mint_json, '$.txType'),
    InitialBuy AS TRY_CAST(JSON_VALUE(mint_json, '$.initialBuy') AS FLOAT),
    MarketCapSol AS TRY_CAST(JSON_VALUE(mint_json, '$.marketCapSol') AS FLOAT),
    Name AS JSON_VALUE(mint_json, '$.name'),
    Symbol AS JSON_VALUE(mint_json, '$.symbol'),
    Uri AS JSON_VALUE(mint_json, '$.uri');
*/