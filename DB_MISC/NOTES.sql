
--delete from mint

select top 10 dateadd(minute, -10, getutcdate()), inserted_utc, * from mint order by id desc

select count(*) from mint 

--alter table mint add inserted_utc datetime default(getutcdate())

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

-- mint: 9TkqNBEvdhgaEucT8FZRNA8KpGi79i8i4HaU9tzSpump  // WE LOVE MEMES	MEMES

-- budget: ~1m meta req's per month = 30k per ... 

-- strategy: lookup meta ONCE say 1hr after listing, then again 6hrs, etc. // can calc from mints/day if this fits the meta budget?

 SELECT [id], [mint]
 FROM [dbo].[mint]
 WHERE [inserted_utc] BETWEEN DATEADD(MINUTE, -10, GETUTCDATE()) AND DATEADD(MINUTE, -5, GETUTCDATE())
 AND [meta_json_1hr] IS NULL
 