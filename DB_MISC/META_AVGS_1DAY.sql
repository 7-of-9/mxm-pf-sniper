select top 10 * from day1_avg_holder order by z_score desc
select top 10 * from day1_avg_mc order by z_score desc

-- MC
/*alter view day1_avg_mc as 
	SELECT m.id, m.symbol, m.name, mint, m.inserted_utc,
		m.fetched_utc_1hr,
		m.day1_market_cap,
		m.day1_price,
		CohortStats.mean_market_cap,
		CohortStats.stddev_market_cap,
		(m.day1_market_cap - CohortStats.mean_market_cap) / NULLIF(CohortStats.stddev_market_cap, 0) AS z_score
	FROM 
		dbo.mint m
	CROSS APPLY
	(
		SELECT 
			AVG(m2.day1_market_cap) AS mean_market_cap,
			STDEV(m2.day1_market_cap) AS stddev_market_cap
		FROM 
			dbo.mint m2
		WHERE 
			m2.day1_market_cap IS NOT NULL
			AND m2.fetched_utc_1hr BETWEEN DATEADD(hour, -12, m.fetched_utc_1hr) AND m.fetched_utc_1hr
			AND m2.id <> m.id  -- Exclude the current row
	) CohortStats
	WHERE 
		m.day1_market_cap IS NOT NULL*/

	alter view day1_avg_mc as  -- exclude outlyers from cohort set
	SELECT m.id, m.symbol, m.name, mint, m.inserted_utc,
		m.fetched_utc_1hr,
		m.day1_market_cap,
		m.day1_price,
		CohortStats.mean_market_cap,
		CohortStats.stddev_market_cap,
		(m.day1_market_cap - CohortStats.mean_market_cap) / NULLIF(CohortStats.stddev_market_cap, 0) AS z_score
	FROM 
		dbo.mint m
	CROSS APPLY
	(
		SELECT 
			AVG(day1_market_cap) AS mean_market_cap,
			STDEV(day1_market_cap) AS stddev_market_cap
		FROM 
		(
			SELECT 
				m2.day1_market_cap,
				ROW_NUMBER() OVER (ORDER BY m2.day1_market_cap DESC) as rn_desc,
				ROW_NUMBER() OVER (ORDER BY m2.day1_market_cap ASC) as rn_asc,
				COUNT(*) OVER () as total_count
			FROM 
				dbo.mint m2
			WHERE 
				m2.day1_market_cap IS NOT NULL
				AND m2.fetched_utc_1hr BETWEEN DATEADD(hour, -12, m.fetched_utc_1hr) AND m.fetched_utc_1hr
				AND m2.id <> m.id  -- Exclude the current row
		) ranked_data
		WHERE 
			rn_desc > 1/*GREATEST(1, total_count * 0.01)*/  -- Exclude top 1% (minimum 1)
			AND rn_asc > 1/*GREATEST(1, total_count * 0.01)*/  -- Exclude bottom 1% (minimum 1)
	) CohortStats
	WHERE 
		m.day1_market_cap IS NOT NULL


-- # HOLDERS
/*alter view day1_avg_holder as 
	SELECT m.id, m.symbol, m.name, mint, m.inserted_utc,
		m.fetched_utc_1hr,
		m.day1_holder,
		m.day1_price,
		CohortStats.mean_holder,
		CohortStats.stddev_holder,
		(m.day1_holder - CohortStats.mean_holder) / NULLIF(CohortStats.stddev_holder, 0) AS z_score
	FROM 
		dbo.mint m
	CROSS APPLY
	(
		SELECT 
			AVG(m2.day1_holder) AS mean_holder,
			STDEV(m2.day1_holder) AS stddev_holder
		FROM 
			dbo.mint m2
		WHERE 
			m2.day1_holder IS NOT NULL
			AND m2.fetched_utc_1hr BETWEEN DATEADD(hour, -12, m.fetched_utc_1hr) AND m.fetched_utc_1hr
			AND m2.id <> m.id  -- Exclude the current row
	) CohortStats
	WHERE 
		m.day1_holder IS NOT NULL*/

	alter view day1_avg_holder as -- exclude outlyers from cohort set
	SELECT m.id, m.symbol, m.name, mint, m.inserted_utc,
		m.fetched_utc_1hr,
		m.day1_holder,
		m.day1_price,
		CohortStats.mean_holder,
		CohortStats.stddev_holder,
		(m.day1_holder - CohortStats.mean_holder) / NULLIF(CohortStats.stddev_holder, 0) AS z_score
	FROM 
		dbo.mint m
	CROSS APPLY
	(
		SELECT 
			AVG(day1_holder) AS mean_holder,
			STDEV(day1_holder) AS stddev_holder
		FROM 
		(
			SELECT 
				m2.day1_holder,
				ROW_NUMBER() OVER (ORDER BY m2.day1_holder DESC) as rn_desc,
				ROW_NUMBER() OVER (ORDER BY m2.day1_holder ASC) as rn_asc,
				COUNT(*) OVER () as total_count
			FROM 
				dbo.mint m2
			WHERE 
				m2.day1_holder IS NOT NULL
				AND m2.fetched_utc_1hr BETWEEN DATEADD(hour, -12, m.fetched_utc_1hr) AND m.fetched_utc_1hr
				AND m2.id <> m.id  -- Exclude the current row
		) ranked_data
		WHERE 
			rn_desc > 1/*GREATEST(1, total_count * 0.01)*/  -- Exclude top 1% (minimum 1)
			AND rn_asc > 1/*GREATEST(1, total_count * 0.01)*/  -- Exclude bottom 1% (minimum 1)
	) CohortStats
	WHERE 
		m.day1_holder IS NOT NULL
		