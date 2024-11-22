USE [master]
GO
/****** Object:  Database [pf_sniper]    Script Date: 22 Nov 2024 18:43:44 ******/
CREATE DATABASE [pf_sniper]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'pf_sniper', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\pf_sniper.mdf' , SIZE = 73728KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'pf_sniper_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\pf_sniper_log.ldf' , SIZE = 139264KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
 WITH CATALOG_COLLATION = DATABASE_DEFAULT, LEDGER = OFF
GO
ALTER DATABASE [pf_sniper] SET COMPATIBILITY_LEVEL = 160
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [pf_sniper].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [pf_sniper] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [pf_sniper] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [pf_sniper] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [pf_sniper] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [pf_sniper] SET ARITHABORT OFF 
GO
ALTER DATABASE [pf_sniper] SET AUTO_CLOSE OFF 
GO
ALTER DATABASE [pf_sniper] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [pf_sniper] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [pf_sniper] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [pf_sniper] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [pf_sniper] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [pf_sniper] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [pf_sniper] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [pf_sniper] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [pf_sniper] SET  DISABLE_BROKER 
GO
ALTER DATABASE [pf_sniper] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [pf_sniper] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [pf_sniper] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [pf_sniper] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [pf_sniper] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [pf_sniper] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [pf_sniper] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [pf_sniper] SET RECOVERY FULL 
GO
ALTER DATABASE [pf_sniper] SET  MULTI_USER 
GO
ALTER DATABASE [pf_sniper] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [pf_sniper] SET DB_CHAINING OFF 
GO
ALTER DATABASE [pf_sniper] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [pf_sniper] SET TARGET_RECOVERY_TIME = 60 SECONDS 
GO
ALTER DATABASE [pf_sniper] SET DELAYED_DURABILITY = DISABLED 
GO
ALTER DATABASE [pf_sniper] SET ACCELERATED_DATABASE_RECOVERY = OFF  
GO
EXEC sys.sp_db_vardecimal_storage_format N'pf_sniper', N'ON'
GO
ALTER DATABASE [pf_sniper] SET QUERY_STORE = ON
GO
ALTER DATABASE [pf_sniper] SET QUERY_STORE (OPERATION_MODE = READ_WRITE, CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 30), DATA_FLUSH_INTERVAL_SECONDS = 900, INTERVAL_LENGTH_MINUTES = 60, MAX_STORAGE_SIZE_MB = 1000, QUERY_CAPTURE_MODE = AUTO, SIZE_BASED_CLEANUP_MODE = AUTO, MAX_PLANS_PER_QUERY = 200, WAIT_STATS_CAPTURE_MODE = ON)
GO
USE [pf_sniper]
GO
/****** Object:  User [sniper]    Script Date: 22 Nov 2024 18:43:44 ******/
CREATE USER [sniper] FOR LOGIN [sniper] WITH DEFAULT_SCHEMA=[dbo]
GO
/****** Object:  Table [dbo].[mint]    Script Date: 22 Nov 2024 18:43:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mint](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[mint_json] [nvarchar](max) NOT NULL,
	[inserted_utc] [datetime] NULL,
	[Signature]  AS (json_value([mint_json],'$.signature')),
	[Mint]  AS (json_value([mint_json],'$.mint')),
	[TraderPublicKey]  AS (json_value([mint_json],'$.traderPublicKey')),
	[TxType]  AS (json_value([mint_json],'$.txType')),
	[InitialBuy]  AS (TRY_CAST(json_value([mint_json],'$.initialBuy') AS [float])),
	[MarketCapSol]  AS (TRY_CAST(json_value([mint_json],'$.marketCapSol') AS [float])),
	[Name]  AS (json_value([mint_json],'$.name')),
	[Symbol]  AS (json_value([mint_json],'$.symbol')),
	[Uri]  AS (json_value([mint_json],'$.uri')),
	[meta_json_1hr] [nvarchar](max) NULL,
	[hr1_supply]  AS (TRY_CAST(json_value([meta_json_1hr],'$.data.supply') AS [bigint])),
	[hr1_address]  AS (json_value([meta_json_1hr],'$.data.address')),
	[hr1_name]  AS (json_value([meta_json_1hr],'$.data.name')),
	[hr1_symbol]  AS (json_value([meta_json_1hr],'$.data.symbol')),
	[hr1_icon]  AS (json_value([meta_json_1hr],'$.data.icon')),
	[hr1_decimals]  AS (TRY_CAST(json_value([meta_json_1hr],'$.data.decimals') AS [int])),
	[hr1_holder]  AS (TRY_CAST(json_value([meta_json_1hr],'$.data.holder') AS [int])),
	[hr1_price]  AS (TRY_CAST(json_value([meta_json_1hr],'$.data.price') AS [float])),
	[meta_json_1day] [nvarchar](max) NULL,
	[day1_supply]  AS (TRY_CAST(json_value([meta_json_1day],'$.data.supply') AS [bigint])),
	[day1_address]  AS (json_value([meta_json_1day],'$.data.address')),
	[day1_name]  AS (json_value([meta_json_1day],'$.data.name')),
	[day1_symbol]  AS (json_value([meta_json_1day],'$.data.symbol')),
	[day1_icon]  AS (json_value([meta_json_1day],'$.data.icon')),
	[day1_decimals]  AS (TRY_CAST(json_value([meta_json_1day],'$.data.decimals') AS [int])),
	[day1_holder]  AS (TRY_CAST(json_value([meta_json_1day],'$.data.holder') AS [int])),
	[day1_price]  AS (TRY_CAST(json_value([meta_json_1day],'$.data.price') AS [float])),
	[day1_market_cap]  AS (TRY_CAST(json_value([meta_json_1day],'$.data.market_cap') AS [float])),
	[fetched_utc_1hr] [datetime] NULL,
	[fetched_utc_1day] [datetime] NULL,
	[fetched_utc_12hr] [datetime] NULL,
	[meta_json_12hr] [nvarchar](max) NULL,
	[hr12_supply]  AS (TRY_CAST(json_value([meta_json_12hr],'$.data.supply') AS [bigint])),
	[hr12_address]  AS (json_value([meta_json_12hr],'$.data.address')),
	[hr12_name]  AS (json_value([meta_json_12hr],'$.data.name')),
	[hr12_symbol]  AS (json_value([meta_json_12hr],'$.data.symbol')),
	[hr12_icon]  AS (json_value([meta_json_12hr],'$.data.icon')),
	[hr12_decimals]  AS (TRY_CAST(json_value([meta_json_12hr],'$.data.decimals') AS [int])),
	[hr12_holder]  AS (TRY_CAST(json_value([meta_json_12hr],'$.data.holder') AS [int])),
	[hr12_price]  AS (TRY_CAST(json_value([meta_json_12hr],'$.data.price') AS [float])),
	[hr12_market_cap]  AS (TRY_CAST(json_value([meta_json_12hr],'$.data.market_cap') AS [float])),
	[meta_json_6hr] [nvarchar](max) NULL,
	[fetched_utc_6hr] [datetime] NULL,
	[hr6_supply]  AS (TRY_CAST(json_value([meta_json_6hr],'$.data.supply') AS [bigint])),
	[hr6_address]  AS (json_value([meta_json_6hr],'$.data.address')),
	[hr6_name]  AS (json_value([meta_json_6hr],'$.data.name')),
	[hr6_symbol]  AS (json_value([meta_json_6hr],'$.data.symbol')),
	[hr6_icon]  AS (json_value([meta_json_6hr],'$.data.icon')),
	[hr6_decimals]  AS (TRY_CAST(json_value([meta_json_6hr],'$.data.decimals') AS [int])),
	[hr6_holder]  AS (TRY_CAST(json_value([meta_json_6hr],'$.data.holder') AS [int])),
	[hr6_price]  AS (TRY_CAST(json_value([meta_json_6hr],'$.data.price') AS [float])),
	[hr6_market_cap]  AS (TRY_CAST(json_value([meta_json_6hr],'$.data.market_cap') AS [float])),
	[tr1_slope] [float] NULL,
	[tr1_pvalue] [float] NULL,
	[hr1_market_cap]  AS (CONVERT([decimal](18,2),json_value([meta_json_1hr],'$.data.market_cap'))),
 CONSTRAINT [PK_mint] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  View [dbo].[hr1_avg_mc]    Script Date: 22 Nov 2024 18:43:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- MC
CREATE view [dbo].[hr1_avg_mc] as 
	SELECT m.id, m.symbol, m.name, mint, m.inserted_utc,
		m.fetched_utc_1hr,
		m.hr1_market_cap,
		m.hr1_price,
		CohortStats.mean_market_cap,
		CohortStats.stddev_market_cap,
		(m.hr1_market_cap - CohortStats.mean_market_cap) / NULLIF(CohortStats.stddev_market_cap, 0) AS z_score,
		m.tr1_slope
	FROM 
		dbo.mint m
	CROSS APPLY
	(
		SELECT 
			AVG(m2.hr1_market_cap) AS mean_market_cap,
			STDEV(m2.hr1_market_cap) AS stddev_market_cap
		FROM 
			dbo.mint m2
		WHERE 
			m2.hr1_market_cap IS NOT NULL
			AND m2.fetched_utc_1hr BETWEEN DATEADD(hour, -12, m.fetched_utc_1hr) AND m.fetched_utc_1hr
			AND m2.id <> m.id  -- Exclude the current row
	) CohortStats
	WHERE 
		m.hr1_market_cap IS NOT NULL
GO
/****** Object:  View [dbo].[hr1_avg_holder]    Script Date: 22 Nov 2024 18:43:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- # HOLDERS
CREATE view [dbo].[hr1_avg_holder] as 
	SELECT m.id, m.symbol, m.name, mint, m.inserted_utc,
		m.fetched_utc_1hr,
		m.hr1_holder,
		m.hr1_price,
		CohortStats.mean_holder,
		CohortStats.stddev_holder,
		(m.hr1_holder - CohortStats.mean_holder) / NULLIF(CohortStats.stddev_holder, 0) AS z_score,
		m.tr1_slope
	FROM 
		dbo.mint m
	CROSS APPLY
	(
		SELECT 
			AVG(m2.hr1_holder) AS mean_holder,
			STDEV(m2.hr1_holder) AS stddev_holder
		FROM 
			dbo.mint m2
		WHERE 
			m2.hr1_holder IS NOT NULL
			AND m2.fetched_utc_1hr BETWEEN DATEADD(hour, -12, m.fetched_utc_1hr) AND m.fetched_utc_1hr
			AND m2.id <> m.id  -- Exclude the current row
	) CohortStats
	WHERE 
		m.hr1_holder IS NOT NULL

GO
/****** Object:  View [dbo].[hr12_avg_mc]    Script Date: 22 Nov 2024 18:43:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- MC
CREATE view [dbo].[hr12_avg_mc] as 
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
GO
/****** Object:  View [dbo].[hr12_avg_holder]    Script Date: 22 Nov 2024 18:43:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- # HOLDERS
CREATE view [dbo].[hr12_avg_holder] as 
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


		
GO
/****** Object:  View [dbo].[day1_avg_mc]    Script Date: 22 Nov 2024 18:43:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- MC
CREATE view [dbo].[day1_avg_mc] as 
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
		m.day1_market_cap IS NOT NULL
GO
/****** Object:  View [dbo].[day1_avg_holder]    Script Date: 22 Nov 2024 18:43:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- # HOLDERS
CREATE view [dbo].[day1_avg_holder] as 
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
		m.day1_holder IS NOT NULL
GO
/****** Object:  View [dbo].[hr6_avg_mc]    Script Date: 22 Nov 2024 18:43:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- MC
CREATE view [dbo].[hr6_avg_mc] as 
	SELECT m.id, m.symbol, m.name, mint, m.inserted_utc,
		m.fetched_utc_1hr,
		m.hr6_market_cap,
		m.hr6_price,
		CohortStats.mean_market_cap,
		CohortStats.stddev_market_cap,
		(m.hr6_market_cap - CohortStats.mean_market_cap) / NULLIF(CohortStats.stddev_market_cap, 0) AS z_score,
		m.tr1_slope
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
			AND m2.fetched_utc_1hr BETWEEN DATEADD(hour, -12, m.fetched_utc_1hr) AND m.fetched_utc_1hr
			AND m2.id <> m.id  -- Exclude the current row
	) CohortStats
	WHERE 
		m.hr6_market_cap IS NOT NULL
GO
/****** Object:  View [dbo].[hr6_avg_holder]    Script Date: 22 Nov 2024 18:43:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- # HOLDERS
CREATE view [dbo].[hr6_avg_holder] as 
	SELECT m.id, m.symbol, m.name, mint, m.inserted_utc,
		m.fetched_utc_1hr,
		m.hr6_holder,
		m.hr6_price,
		CohortStats.mean_holder,
		CohortStats.stddev_holder,
		(m.hr6_holder - CohortStats.mean_holder) / NULLIF(CohortStats.stddev_holder, 0) AS z_score,
		m.tr1_slope
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
			AND m2.fetched_utc_1hr BETWEEN DATEADD(hour, -12, m.fetched_utc_1hr) AND m.fetched_utc_1hr
			AND m2.id <> m.id  -- Exclude the current row
	) CohortStats
	WHERE 
		m.hr6_holder IS NOT NULL

GO
/****** Object:  Index [IX_mint]    Script Date: 22 Nov 2024 18:43:44 ******/
CREATE NONCLUSTERED INDEX [IX_mint] ON [dbo].[mint]
(
	[inserted_utc] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  Index [IX_mint_1]    Script Date: 22 Nov 2024 18:43:44 ******/
CREATE NONCLUSTERED INDEX [IX_mint_1] ON [dbo].[mint]
(
	[tr1_slope] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ARITHABORT ON
SET CONCAT_NULL_YIELDS_NULL ON
SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET ANSI_PADDING ON
SET ANSI_WARNINGS ON
SET NUMERIC_ROUNDABORT OFF
GO
/****** Object:  Index [ix_mint_inserted_hr1_mc]    Script Date: 22 Nov 2024 18:43:44 ******/
CREATE NONCLUSTERED INDEX [ix_mint_inserted_hr1_mc] ON [dbo].[mint]
(
	[inserted_utc] ASC,
	[hr1_market_cap] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [dbo].[mint] ADD  DEFAULT (getutcdate()) FOR [inserted_utc]
GO
USE [master]
GO
ALTER DATABASE [pf_sniper] SET  READ_WRITE 
GO
