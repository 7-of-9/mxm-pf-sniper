--
-- todo: harvest 6hr metas...
--		filter for +ve 1hr (vs avg) & 6hr (price up) >> take out the quick rugs, after 6hrs
--

	--update mint set tr1_slope = null, tr1_pvalue = null where tr1_pvalue != -888

	-- WIP: from the accumulated rolling 6hr windows... any clear UPTRENDs??
	select symbol, name, * from mint where tr1_slope > 0.05 and tr1_pvalue > 0.1
		--update mint set tr1_slope = null, tr1_pvalue = null where id in (13596, 13032)
	
		-- OR: (irrespective of confirmed uptrend) -- all these *were* in the 6hr window... a manual review set:
			select symbol, name, hr6_holder, * from mint where tr1_slope is not null and tr1_pvalue is not null and hr6_price is not null 
				and hr6_holder > hr1_holder -- !!
				order by 3 desc
				--order by ((hr6_holder - hr1_holder) * 100.0 / hr1_holder) DESC

	-- 1hr zscores vs 6hr performance :: *current* rolling window (working set for trend-friend) 
		 SELECT TOP 50 PERCENT 
			 id, name, symbol, mean_market_cap, hr1_market_cap,
			 datediff(hh, getutcdate(), inserted_utc) 'hrs old',
			 tr1_slope, tr1_pvalue, mint, z_score, hr6_holder, Uri, hr1_icon, hr6_market_cap,
			 hr6_price, hr1_price, tr1_graph_ipfs, hr6_best_rank
		 FROM hr1_avg_mc 
		 WHERE inserted_utc BETWEEN DATEADD(HOUR, -12, GETUTCDATE()) AND DATEADD(HOUR, -6, GETUTCDATE())
			 AND z_score > 0
			 AND hr6_price IS NOT NULL
			 AND hr6_holder > hr1_holder
			 AND hr6_price > hr1_price
		ORDER BY z_score DESC

			-- 6hr cohort:
			SELECT id, name, symbol, hr1_market_cap, mint from mint where fetched_utc_1hr BETWEEN DATEADD(hour, -12, fetched_utc_1hr) AND fetched_utc_1hr order by hr1_market_cap desc
				-- delete from mint where mint = '8usm7F5hdhjd3dm5YZsLi6uSJi7QsBoVz6QC8ycvpump'
				--create index idx_mint_3 on mint (id, fetched_utc_1hr, hr12_market_cap)
				--select * from mint where mint = 'am1sqwahhakws4h9uwhweeyxcaecr5ydyzix1jd9rvmp'

	-- 6hr zscores vs 12hr performance :: *current* rolling window (working set for trend-friend)
		 SELECT TOP 100 PERCENT 
			 id, name, symbol, 
			 datediff(hh, getutcdate(), inserted_utc) 'hrs old',
			 tr2_slope, tr2_pvalue, mint, z_score, hr12_holder, Uri, hr1_icon, hr12_market_cap, tr2_graph_ipfs, hr12_best_rank,
			 hr6_holder, hr6_price
		 FROM hr6_avg_mc 
		 WHERE inserted_utc BETWEEN DATEADD(HOUR, -24, GETUTCDATE()) AND DATEADD(HOUR, -12, GETUTCDATE())
			 AND z_score > 0
			 AND hr12_price IS NOT NULL
			 AND hr12_holder > hr6_holder
			 AND hr12_price > hr6_price
			 --AND tr2_slope is null 
		ORDER BY z_score DESC
		
		--
	-- TODO: https://countik.com/popular/hashtags ==> import each day; x-ref coins with these ... 
	--	
		--....

		update mint set tr2_slope = null, tr2_pvalue=null where id=14458
		alter table mint add hr12_best_rank int

	select * from mint where id = 6684

	select count(*) from hr1_avg_mc where hr6_price is not null

	select datediff(hh, getutcdate(), inserted_utc) 'hrs old', symbol, name, tr1_slope, tr1_pvalue, * from mint where tr1_slope is not null order by 5 desc
		-- pvalue: WHAT IF WE ALL HOLD -- DUBIOUS: pval 0.03, slope 0.1
	
	select * from mint where id = 10804

-- todo - filter above by +ve price 6hrs later...


--
-- todo: wire up trends for the filtered 6hr winners...
--

--delete from mint

select top 100 
datediff(n, getutcdate(), inserted_utc) 'mins old',
datediff(hh, getutcdate(), inserted_utc) 'hrs old',
* from mint order by id asc

	--select top 10 * from mint where hr6_price is not null

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
   	[hr3_supply]  AS (TRY_CAST(json_value([meta_json_3hr],'$.data.supply') AS [bigint])),
	[hr3_address]  AS (json_value([meta_json_3hr],'$.data.address')),
	[hr3_name]  AS (json_value([meta_json_3hr],'$.data.name')),
	[hr3_symbol]  AS (json_value([meta_json_3hr],'$.data.symbol')),
	[hr3_icon]  AS (json_value([meta_json_3hr],'$.data.icon')),
	[hr3_decimals]  AS (TRY_CAST(json_value([meta_json_3hr],'$.data.decimals') AS [int])),
	[hr3_holder]  AS (TRY_CAST(json_value([meta_json_3hr],'$.data.holder') AS [int])),
	[hr3_price]  AS (TRY_CAST(json_value([meta_json_3hr],'$.data.price') AS [float])),
	[hr3_market_cap]  AS (CONVERT([decimal](18,2),json_value([meta_json_3hr],'$.data.market_cap')))