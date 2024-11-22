--
-- todo: harvest 6hr metas...
--		filter for +ve 1hr (vs avg) & 6hr (price up) >> take out the quick rugs, after 6hrs

	SELECT TOP 5 PERCENT * FROM hr1_avg_mc WHERE inserted_utc >= DATEADD(HOUR, -6, GETUTCDATE()) ORDER BY z_score DESC
	-- todo - filter above by +ve price 6hrs later...


--
-- todo: wire up trends for the filtered 6hr winners...
--

--delete from mint

select top 100 
datediff(n, getutcdate(), inserted_utc) 'mins old',
datediff(hh, getutcdate(), inserted_utc) 'hrs old',
* from mint order by id asc

select * from mint where id = 6066

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

 SELECT [id], [mint], inserted_utc, getutcdate()
 FROM [dbo].[mint]
 WHERE [inserted_utc] BETWEEN DATEADD(MINUTE, -2, GETUTCDATE()) AND DATEADD(MINUTE, -1, GETUTCDATE())
 AND [meta_json_1hr] IS NULL
 
/*ALTER TABLE [dbo].[mint]
ADD
    [hr1_supply] AS (TRY_CAST(JSON_VALUE([meta_json_1hr], '$.data.supply') AS BIGINT)),  -- Assuming supply is a large number
    [hr1_address] AS (JSON_VALUE([meta_json_1hr], '$.data.address')),
    [hr1_name] AS (JSON_VALUE([meta_json_1hr], '$.data.name')),
    [hr1_symbol] AS (JSON_VALUE([meta_json_1hr], '$.data.symbol')),
    [hr1_icon] AS (JSON_VALUE([meta_json_1hr], '$.data.icon')),
    [hr1_decimals] AS (TRY_CAST(JSON_VALUE([meta_json_1hr], '$.data.decimals') AS INT)),
    [hr1_holder] AS (TRY_CAST(JSON_VALUE([meta_json_1hr], '$.data.holder') AS INT)),
    [hr1_price] AS (TRY_CAST(JSON_VALUE([meta_json_1hr], '$.data.price') AS FLOAT)),
    [hr1_market_cap] AS (TRY_CAST(JSON_VALUE([meta_json_1hr], '$.data.market_cap') AS FLOAT));
*/

/*
alter table mint add meta_json_1day nvarchar(max)
ALTER TABLE [dbo].[mint]
ADD
    [day1_supply] AS (TRY_CAST(JSON_VALUE([meta_json_1day], '$.data.supply') AS BIGINT)),  -- Assuming supply is a large number
    [day1_address] AS (JSON_VALUE([meta_json_1day], '$.data.address')),
    [day1_name] AS (JSON_VALUE([meta_json_1day], '$.data.name')),
    [day1_symbol] AS (JSON_VALUE([meta_json_1day], '$.data.symbol')),
    [day1_icon] AS (JSON_VALUE([meta_json_1day], '$.data.icon')),
    [day1_decimals] AS (TRY_CAST(JSON_VALUE([meta_json_1day], '$.data.decimals') AS INT)),
    [day1_holder] AS (TRY_CAST(JSON_VALUE([meta_json_1day], '$.data.holder') AS INT)),
    [day1_price] AS (TRY_CAST(JSON_VALUE([meta_json_1day], '$.data.price') AS FLOAT)),
    [day1_market_cap] AS (TRY_CAST(JSON_VALUE([meta_json_1day], '$.data.market_cap') AS FLOAT));
*/

--alter table mint add fetched_utc_6hr datetime
--alter table mint add [meta_json_6hr] nvarchar(max)

ALTER TABLE [dbo].[mint]
ADD
    [hr6_supply] AS (TRY_CAST(JSON_VALUE(meta_json_6hr, '$.data.supply') AS BIGINT)),  -- Assuming supply is a large number
    [hr6_address] AS (JSON_VALUE(meta_json_6hr, '$.data.address')),
    [hr6_name] AS (JSON_VALUE(meta_json_6hr, '$.data.name')),
    [hr6_symbol] AS (JSON_VALUE(meta_json_6hr, '$.data.symbol')),
    [hr6_icon] AS (JSON_VALUE(meta_json_6hr, '$.data.icon')),
    [hr6_decimals] AS (TRY_CAST(JSON_VALUE(meta_json_6hr, '$.data.decimals') AS INT)),
    [hr6_holder] AS (TRY_CAST(JSON_VALUE(meta_json_6hr, '$.data.holder') AS INT)),
    [hr6_price] AS (TRY_CAST(JSON_VALUE(meta_json_6hr, '$.data.price') AS FLOAT)),
    [hr6_market_cap] AS (TRY_CAST(JSON_VALUE(meta_json_6hr, '$.data.market_cap') AS FLOAT));