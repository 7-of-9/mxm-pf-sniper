select top 100 * from hr6_avg_holder order by z_score desc
	select count(*) from hr6_avg_holder 

select top 100 * from hr6_avg_mc order by z_score desc
	select count(*) from hr6_avg_mc 

-- MC
/*alter view hr6_avg_mc as 
	SELECT m.*, --m.id, m.symbol, m.name, mint, m.inserted_utc,
		--m.fetched_utc_1hr,
		--m.hr6_market_cap,
		--m.hr6_price, m.hr6_holder,
		CohortStats.mean_market_cap,
		CohortStats.stddev_market_cap,
		(m.hr6_market_cap - CohortStats.mean_market_cap) / NULLIF(CohortStats.stddev_market_cap, 0) AS z_score
		--m.tr2_slope, m.tr2_pvalue,
		--m.hr12_price, m.hr12_market_cap, m.hr12_holder, m.uri, m.hr1_icon
	FROM 
		dbo.mint m
	CROSS APPLY
	(
		SELECT 
			AVG(m2.hr6_market_cap) AS mean_market_cap,
			STDEV(m2.hr6_market_cap) AS stddev_market_cap
		FROM 
			dbo.mint m2
		WHERE 
			m2.hr6_market_cap IS NOT NULL
			AND m2.fetched_utc_6hr BETWEEN DATEADD(hour, -12, m.fetched_utc_6hr) AND m.fetched_utc_6hr
			AND m2.id <> m.id  -- Exclude the current row
	) CohortStats
	WHERE 
		m.hr6_market_cap IS NOT NULL*/

		alter view hr6_avg_mc as -- exclude outlyers from cohort set
		SELECT m.*, 
			CohortStats.mean_market_cap,
			CohortStats.stddev_market_cap,
			(m.hr6_market_cap - CohortStats.mean_market_cap) / NULLIF(CohortStats.stddev_market_cap, 0) AS z_score
		FROM 
			dbo.mint m
		CROSS APPLY
		(
			SELECT 
				AVG(hr6_market_cap) AS mean_market_cap,
				STDEV(hr6_market_cap) AS stddev_market_cap
			FROM 
			(
				SELECT 
					m2.hr6_market_cap,
					ROW_NUMBER() OVER (ORDER BY m2.hr6_market_cap DESC) as rn_desc,
					ROW_NUMBER() OVER (ORDER BY m2.hr6_market_cap ASC) as rn_asc,
					COUNT(*) OVER () as total_count
				FROM 
					dbo.mint m2
				WHERE 
					m2.hr6_market_cap IS NOT NULL
					AND m2.fetched_utc_6hr BETWEEN DATEADD(hour, -12, m.fetched_utc_6hr) AND m.fetched_utc_6hr
					AND m2.id <> m.id  -- Exclude the current row
			) ranked_data
			WHERE 
				rn_desc > GREATEST(1, total_count * 0.01)  -- Exclude top 1% (minimum 1)
				AND rn_asc > GREATEST(1, total_count * 0.01)  -- Exclude bottom 1% (minimum 1)
		) CohortStats
		WHERE 
			m.hr6_market_cap IS NOT NULL

-- # HOLDERS
/*alter view hr6_avg_holder as 
	SELECT m.*, --m.id, m.symbol, m.name, mint, m.inserted_utc,
		--m.fetched_utc_1hr,
		--m.hr6_holder,
		--m.hr6_price, 
		CohortStats.mean_holder,
		CohortStats.stddev_holder,
		(m.hr6_holder - CohortStats.mean_holder) / NULLIF(CohortStats.stddev_holder, 0) AS z_score
		--m.tr2_slope,  m.tr2_pvalue,
		--m.hr12_price, m.hr12_market_cap, m.hr12_holder, m.uri, m.hr1_icon
	FROM 
		dbo.mint m
	CROSS APPLY
	(
		SELECT 
			AVG(m2.hr6_holder) AS mean_holder,
			STDEV(m2.hr6_holder) AS stddev_holder
		FROM 
			dbo.mint m2
		WHERE 
			m2.hr6_holder IS NOT NULL
			AND m2.fetched_utc_6hr BETWEEN DATEADD(hour, -12, m.fetched_utc_6hr) AND m.fetched_utc_6hr
			AND m2.id <> m.id  -- Exclude the current row
	) CohortStats
	WHERE 
		m.hr6_holder IS NOT NULL*/

		alter view hr6_avg_holder as -- exclude outlyers from cohort set 
		SELECT m.*, 
			CohortStats.mean_holder,
			CohortStats.stddev_holder,
			(m.hr6_holder - CohortStats.mean_holder) / NULLIF(CohortStats.stddev_holder, 0) AS z_score
		FROM 
			dbo.mint m
		CROSS APPLY
		(
			SELECT 
				AVG(hr6_holder) AS mean_holder,
				STDEV(hr6_holder) AS stddev_holder
			FROM 
			(
				SELECT 
					m2.hr6_holder,
					ROW_NUMBER() OVER (ORDER BY m2.hr6_holder DESC) as rn_desc,
					ROW_NUMBER() OVER (ORDER BY m2.hr6_holder ASC) as rn_asc,
					COUNT(*) OVER () as total_count
				FROM 
					dbo.mint m2
				WHERE 
					m2.hr6_holder IS NOT NULL
					AND m2.fetched_utc_6hr BETWEEN DATEADD(hour, -12, m.fetched_utc_6hr) AND m.fetched_utc_6hr
					AND m2.id <> m.id  -- Exclude the current row
			) ranked_data
			WHERE 
				rn_desc > GREATEST(1, total_count * 0.01)  -- Exclude top 1% (minimum 1)
				AND rn_asc > GREATEST(1, total_count * 0.01)  -- Exclude bottom 1% (minimum 1)
		) CohortStats
		WHERE 
			m.hr6_holder IS NOT NULL

		