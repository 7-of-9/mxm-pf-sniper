select top 10 * from hr12_avg_holder order by z_score desc
select top 10 * from hr12_avg_mc order by z_score desc

-- MC
alter view hr12_avg_mc as 
	SELECT m.id, m.symbol, m.name, mint, m.inserted_utc,
		m.fetched_utc_1hr,
		m.hr12_market_cap,
		m.hr12_price,
		CohortStats.mean_market_cap,
		CohortStats.stddev_market_cap,
		(m.hr12_market_cap - CohortStats.mean_market_cap) / NULLIF(CohortStats.stddev_market_cap, 0) AS z_score
	FROM 
		dbo.mint m
	CROSS APPLY
	(
		SELECT 
			AVG(m2.hr12_market_cap) AS mean_market_cap,
			STDEV(m2.hr12_market_cap) AS stddev_market_cap
		FROM 
			dbo.mint m2
		WHERE 
			m2.hr12_market_cap IS NOT NULL
			AND m2.fetched_utc_1hr BETWEEN DATEADD(hour, -12, m.fetched_utc_1hr) AND m.fetched_utc_1hr
			AND m2.id <> m.id  -- Exclude the current row
	) CohortStats
	WHERE 
		m.hr12_market_cap IS NOT NULL

-- # HOLDERS
alter view hr12_avg_holder as 
	SELECT m.id, m.symbol, m.name, mint, m.inserted_utc,
		m.fetched_utc_1hr,
		m.hr12_holder,
		m.hr12_price,
		CohortStats.mean_holder,
		CohortStats.stddev_holder,
		(m.hr12_holder - CohortStats.mean_holder) / NULLIF(CohortStats.stddev_holder, 0) AS z_score
	FROM 
		dbo.mint m
	CROSS APPLY
	(
		SELECT 
			AVG(m2.hr12_holder) AS mean_holder,
			STDEV(m2.hr12_holder) AS stddev_holder
		FROM 
			dbo.mint m2
		WHERE 
			m2.hr12_holder IS NOT NULL
			AND m2.fetched_utc_1hr BETWEEN DATEADD(hour, -12, m.fetched_utc_1hr) AND m.fetched_utc_1hr
			AND m2.id <> m.id  -- Exclude the current row
	) CohortStats
	WHERE 
		m.hr12_holder IS NOT NULL


		