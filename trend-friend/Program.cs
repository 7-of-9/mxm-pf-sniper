using System;
using System.Configuration;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using MathNet.Numerics.Statistics;
using MathNet.Numerics.LinearRegression;
using MathNet.Numerics.Distributions;
using Newtonsoft.Json;
using System.Data.SqlClient;
using System.Collections.Generic;
using static System.Formats.Asn1.AsnWriter;
using System.Globalization;
using System.Diagnostics;
using System.Xml.Linq;
using ScottPlot;
using ScottPlot.Plottable;
using Telegram.Bot.Types.Enums;
using ScottPlot.Plottable.AxisManagers;
using Microsoft.AspNetCore.Server.HttpSys;

public class Program {
    private static readonly string ApifyToken = ConfigurationManager.AppSettings["ApifyApiToken"];
    private static readonly string GoogleTrends_ActorId = "emastra~google-trends-scraper";
    private static readonly string TwitterFollowers_ActorId = "kaitoeasyapi~premium-x-follower-scraper-following-data";
    private static readonly string DbConStr = ConfigurationManager.ConnectionStrings["pf.Properties.Settings.mintDbConnectionString"].ConnectionString;
    private static readonly TelegramBot Bot = new TelegramBot(ConfigurationManager.AppSettings["TelegramBotToken"]);

    private static string CACHE_DIR = Path.Combine("..", "..", "..", "cache");

    private static List<string> X_GOOD_NAMES = new List<string>();

    private static async Task<int> PopulateXGoodNames(string x_account) {
        var input = new { getFollowers = false, getFollowing = true, user_names = new[] { x_account }, maxFollowings = 10000, };
        var json = await GetTwitterFollowersOrFollowing("-", "X_" + x_account, "X_" + x_account, input, x_account);
        var data = JsonConvert.DeserializeObject<List<Twitter_ReturnData>>(json);
        X_GOOD_NAMES.AddRange(data.Select(p => p.Name));
        return data.Count();

    }

    const double TREND_MIN_SLOPE = 0.05;
    const double TREND_MIN_RSQ = 0.1;

    const int MINS_SLEEP = 5;       // should be log enough to let appify calls return, else we will re-enter

    static async Task Main(string[] args) {
        Bot.StartAsync();

        // tests
        //AnalyzeAndSaveTrend("test", "BARD", "", System.IO.File.ReadAllLines(@"..\..\..\cache\NAS_Not_a_Security.csv").Select(p => double.Parse(p)).ToList(), -1); // periodic, no trend
        //AnalyzeAndSaveTrend("test", "CHILLGUY", "", System.IO.File.ReadAllLines(@"..\..\..\cache\CHILLGUY_Chill_Guy.csv").Select(p => double.Parse(p)).ToList(), -1); // uptrend at end, clear

        // get trending tokens into the cache for testing...
        //AnalyzeAndSaveTrend("test", "CHILLGUY", "", await GetGoogleTrends("CHILLGUY", "Chill Guy", new {
        //    isMultiple = false,
        //    isPublic = false,
        //    searchTerms = new[] { "Chill Guy" },
        //    skipDebugScreen = false,
        //    timeRange = "now 7-d",
        //    viewedFrom = "us"
        //}), -1);
        //AnalyzeAndSaveTrend("test", "CHILLFAM", "", await GetGoogleTrends("CHILLFAM", "chill family", new {
        //    isMultiple = false,
        //    isPublic = false,
        //    searchTerms = new[] { "chill family" },
        //    skipDebugScreen = false,
        //    timeRange = "now 7-d",
        //    viewedFrom = "us"
        //}), -1);

        // test from cache
        //TestFromCache();
        //return;

        List<string> hr6_symbolsSeen = new List<string>();
        List<string> hr12_symbolsSeen = new List<string>();
        while (true) {
            try {
                // fetch template accounts: we'll use their x followings to grade coins' followers
                await PopulateXGoodNames("_Sabai_Sabai");
                await PopulateXGoodNames("dubbingwifhat");

                // tr0 -- trend sample 0: taken at 3hrs
                // tr1 -- trend sample 1: taken at 6hrs
                // tr2 -- trend sample 2: taken at 12hrs

                // 1hr zscored with 3hr +ve performance -- "youngest set"
                //string query_3hr = @"
                //    SELECT TOP 100 PERCENT 
                //        id, name, symbol, 
                //        datediff(hh, getutcdate(), inserted_utc) 'hrs old',
                //        tr0_slope, tr0_pvalue, mint, z_score, hr3_holder, Uri, hr1_icon, hr3_market_cap, tr0_graph_ipfs, hr3_best_rank,
                //        hr1_holder, hr1_price
                //    FROM hr1_avg_mc 
                //    WHERE inserted_utc BETWEEN DATEADD(HOUR, -6, GETUTCDATE()) AND DATEADD(HOUR, -3, GETUTCDATE())
                //        AND z_score > 0
                //        AND hr3_price IS NOT NULL
                //        AND hr3_holder > hr1_holder
                //        AND hr3_price > hr1_price
                //    ORDER BY z_score DESC
                //";

                // 1hr zscored with 6hr +ve performance -- "medium set"
                string query_6hr = @"
                    SELECT TOP 100 PERCENT 
                        id, name, symbol, 
                        datediff(hh, getutcdate(), inserted_utc) 'hrs old',
                        tr1_slope, tr1_pvalue, mint, z_score, hr6_holder, Uri, hr1_icon, hr6_market_cap, tr1_graph_ipfs, hr6_best_rank, hr6_x_score,
                        hr1_holder, hr1_price
                    FROM hr1_avg_mc 
                    WHERE inserted_utc BETWEEN DATEADD(HOUR, -12, GETUTCDATE()) AND DATEADD(HOUR, -6, GETUTCDATE())
	                    AND z_score > 0
                        AND hr6_price IS NOT NULL
                        AND hr6_holder > hr1_holder
                        AND hr6_price > hr1_price
                    ORDER BY z_score DESC
                ";

                // 6hr zscored w/ 12hr +ve performance :: "mature set"
                string query_12hr = @"
		             SELECT TOP 100 PERCENT 
			             id, name, symbol, 
			             datediff(hh, getutcdate(), inserted_utc) 'hrs old',
			             tr2_slope, tr2_pvalue, mint, z_score, hr12_holder, Uri, hr1_icon, hr12_market_cap, tr2_graph_ipfs, hr12_best_rank, hr12_x_score,
                         hr6_holder, hr6_price
		             FROM hr6_avg_mc 
		             WHERE inserted_utc BETWEEN DATEADD(HOUR, -24, GETUTCDATE()) AND DATEADD(HOUR, -12, GETUTCDATE())
			             AND z_score > 0
			             AND hr12_price IS NOT NULL
			             AND hr12_holder > hr6_holder
			             AND hr12_price > hr6_price
		            ORDER BY z_score DESC
                ";

                // Process both queries
                Bot.Currents["3HR"] = new List<string>();
                Bot.Currents["6HR"] = new List<string>();
                Bot.Currents["12HR"] = new List<string>();
                //await ProcessQuery("3HR", query_3hr, hr6_symbolsSeen, 3);
                await ProcessQuery("6HR", query_6hr, hr6_symbolsSeen, 8);
                await ProcessQuery("12HR", query_12hr, hr12_symbolsSeen, 8);

                // Sleep between iterations
                await Task.Delay(TimeSpan.FromMinutes(MINS_SLEEP));
            }
            catch (Exception ex) {
                Console.WriteLine($"Error occurred: {ex.Message}");
            }
        }
    }

    private static async Task ProcessQuery(string prefix, string query, List<string> symbolsSeen, int LIMIT_N) {
        if (prefix != "6HR" && prefix != "12HR" && prefix != "3HR") throw new ApplicationException();

        List<(int rowId, string name, string symbol, string mint)> rowsNeedingTrendData = new List<(int, string, string, string)>();
        List<(int rowId, string name, string symbol, string mint, string x_account)> rowsNeedingTwitterScore = new List<(int, string, string, string, string)>();
        
        List<(string symbol, string mint)> rowsDbg = new List<(string, string)>();

        using (var connection = new SqlConnection(DbConStr)) {
            await connection.OpenAsync();

            int pos = 0;
            bool sawNew = false;

            var tableBuilder = new StringBuilder();
            tableBuilder.AppendLine("```");
            tableBuilder.AppendLine($"{prefix} Results (LIMIT_N={LIMIT_N}):");
            tableBuilder.AppendLine("Sym+Age  MC$m Z       Hs   /kMC");
            tableBuilder.AppendLine("-------------------------------");


            using (var command = connection.CreateCommand()) {
                command.CommandText = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED";
                command.ExecuteNonQuery();

                command.CommandText = query;
                Console.WriteLine($"{prefix} {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")} querying shortlist...");
                using (var reader = await command.ExecuteReaderAsync()) {
                    while (await reader.ReadAsync()) {
                        pos++;
                        try {
                            int id = reader.GetInt32(0);
                            string name = reader.GetString(1);
                            string symbol = reader.GetString(2);
                            int hrs_old = reader.GetInt32(3);
                            double? tr_slope = reader.IsDBNull(4) ? null : reader.GetDouble(4);
                            double? tr_pvalue = reader.IsDBNull(5) ? null : reader.GetDouble(5);
                            string mint = reader.GetString(6);
                            double z_score = reader.GetDouble(7);
                            int cur_holders = reader.GetInt32(8);
                            string uri = reader.GetString(9);
                            double cur_mc = reader.IsDBNull(11) ? 0 : (double)reader.GetDecimal(11);
                            string graph_ipfs = reader.IsDBNull(12) ? null : reader.GetString(12);
                            int? best_rank = reader.IsDBNull(13) ? null : reader.GetInt32(13);
                            int? x_score = reader.IsDBNull(14) ? null : reader.GetInt32(14);

                            // maintain best rank (entire shortlist) -- timing out, on hold
                            if (best_rank == null || best_rank > pos) {
                                UpdateBestRank(prefix, id, pos);
                            }

                            // limit shortlist for display/processing
                            if (pos > LIMIT_N) {
                                continue;
                            }

                            // load token URI for metadata fields (socials)
                            var metadata = await TokenMetadataParser.ParseMetadataFromUri(uri);

                            // keep track of rows for which we need to gather trend data
                            rowsDbg.Add((symbol, mint));
                            if (tr_slope == null) {
                                rowsNeedingTrendData.Add((id, name, symbol, mint));
                            }

                            // keep track of rows for which we need to get twitter followers score
                            if (x_score == null && !string.IsNullOrEmpty(metadata.Twitter)) {
                                rowsNeedingTwitterScore.Add((id, name, symbol, mint, metadata.Twitter));
                            }

                            // full info row
                            bool? tr_trend = tr_slope != null && tr_pvalue != null
                                                ? (tr_slope > TREND_MIN_SLOPE && tr_pvalue > TREND_MIN_RSQ) : null;
                            string tr_info = "Google Trends: " + (tr_trend == null ? "tbd" :
                                (tr_trend == true ? "[CONFIRMED!!!]" : "[none found]") + $"({graph_ipfs})"
                                );
                            string web_str = !string.IsNullOrEmpty(metadata.Website) ? $"[website]({metadata.Website})" : "no website";
                            string twitter_str = !string.IsNullOrEmpty(metadata.Twitter) ? $"[twitter]({metadata.Twitter})" : "no twitter";
                            double holders_per_mc = cur_mc > 0 ? cur_holders / (cur_mc / 1000) : 0;
                            string row_info =
                                    $"{prefix} #{pos} [{symbol}](https://dexscreener.com/solana/{mint}) *{name}* ({web_str}) ({twitter_str})"
                                    + $"\n MC = {FormatCurrency(cur_mc)}"
                                    + $"\n age = {hrs_old * -1}h"
                                    + $"\n z\\_score = {z_score.ToString("0.00")}"
                                    + $"\n holders = {cur_holders}"
                                    + $"\n h/k$MC = {holders_per_mc.ToString("0.00")} "
                                    + $"\n {tr_info}"
                                    + "\n_" + (!string.IsNullOrEmpty(metadata.Description) ? metadata.Description.Replace("http", "").Replace("_", " ") : "") + "_";

                            // summary table row
                            tableBuilder.AppendLine(String.Format("{0,-8} {1,4} {2,5}  {3,5} {4,4}",
                                (symbol.Length > 5 ? symbol.Substring(0, 5) : symbol) + $"+{hrs_old * -1}",
                                FormatMillions(cur_mc),
                                (tr_trend == true ? "*" : "/") + FormatZ(z_score), //.ToString("0.0"),
                                FormatThousandsToK(cur_holders),
                                holders_per_mc.ToString("0.0")
                            ));
                    
                            if (!symbolsSeen.Contains($"{prefix}_{symbol}")) {
                                await Bot.BroadcastMessageAsync(row_info); // we want these to arrive in order
                                symbolsSeen.Add($"{prefix}_{symbol}");
                                sawNew = true;
                            }

                            Bot.Currents[prefix].Add(row_info);
                        }
                        catch (Exception ex) {
                            Console.WriteLine(ex.ToString());
                        }
                    }
                }
            }

            tableBuilder.AppendLine("```");
            Bot.Currents[prefix].Add(tableBuilder.ToString());

            if (sawNew) {
                Bot.BroadcastMessageAsync(tableBuilder.ToString());
            }
        }

        // log working set
        var info = $"{prefix} {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")} SET: {(rowsDbg.Count() == 0 ? "empty!" : string.Join(", ", rowsDbg.Select(p => $"{p.symbol}").ToList()).Trim())}";
        Console.WriteLine(info);

        // Process rows for trend data using GetGoogleTrends
        await Task.WhenAll(Parallel.ForEachAsync(rowsNeedingTrendData,
            new ParallelOptions { MaxDegreeOfParallelism = 8 },
            async (row, token) => {
                var input = new {
                    isMultiple = false,
                    isPublic = false,
                    searchTerms = new[] { row.name },
                    skipDebugScreen = false,
                    timeRange = "now 7-d", //"now 1-d",
                    viewedFrom = "us"
                };

                // run google trends
                GetGoogleTrends(prefix, row.symbol, row.name, input) // non-blocking
                .ContinueWith(taskResult => {
                    var values = taskResult.Result;
                    if (values.Any()) {
                        AnalyzeAndSaveTrend(prefix, row.symbol, row.name, values, row.rowId, input.timeRange, row.mint);
                    }
                    else {
                        HandleEmptyData(prefix, row.rowId, -888);
                    }
                }, TaskContinuationOptions.OnlyOnRanToCompletion);
        }));

        // process rows for x follower data
        await Task.WhenAll(Parallel.ForEachAsync(rowsNeedingTwitterScore,
            new ParallelOptions { MaxDegreeOfParallelism = 8 },
            async (row, token) => {
                var input = new {
                    getFollowers = true,
                    getFollowing = false,
                    user_names = new[] { row.x_account },
                    maxFollowers = 1000, // sample size max 1000 last
                    //maxFollowings = 0
                };

                // get followers
                GetTwitterFollowersOrFollowing(prefix, row.symbol, row.name, input, row.x_account)
                    .ContinueWith(taskResult => {
                        var data = JsonConvert.DeserializeObject<List<Twitter_ReturnData>>(taskResult.Result);
                        var x_score = data.Where(p => X_GOOD_NAMES.Contains(p.Name)).Select(p => p.Followers_Count).Sum();
                        AnalyzeAndSaveXScore(prefix, row.symbol, row.name, x_score, row.rowId, row.mint);
                    }, TaskContinuationOptions.OnlyOnRanToCompletion);
        }));
    }

    private static void UpdateBestRank(string prefix, int id, int pos) {
        Console.WriteLine($"{prefix} updating best rank for ID {id}...");
        string hr_prefix = prefix == "6HR"  ? "hr6" 
                         : prefix == "12HR" ? "hr12" 
                                            : "hr3";
        try {
            using (var connection = new SqlConnection(DbConStr)) {
                connection.Open();
                var sql = $@"
                UPDATE [mint]
                SET {hr_prefix}_best_rank = @Position 
                WHERE id = @Id";

                using (var command = new SqlCommand(sql, connection)) {
                    command.Parameters.AddWithValue("@Position", pos);
                    command.Parameters.AddWithValue("@Id", id);
                    command.ExecuteNonQuery();
                }
            }
        }
        catch (Exception ex) {
            Console.WriteLine($"{prefix} Error updating best rank for ID {id}: {ex.Message}");
        }
    }

    private static async Task<List<double>> GetGoogleTrends(string prefix, string symbol, string name, object input) {
        Directory.CreateDirectory(CACHE_DIR);
        var csvPath = Path.Combine(CACHE_DIR, $"{prefix}_{symbol}_{name.Replace(" ", "_")}.csv");

        if (File.Exists(csvPath)) {
            var fileInfo = new FileInfo(csvPath);
            if (DateTime.UtcNow - fileInfo.LastWriteTimeUtc < TimeSpan.FromHours(24)) {
                Console.WriteLine($"{prefix} Using cached data for {symbol} {name} from {csvPath}");
                var lines = await File.ReadAllLinesAsync(csvPath);
                return lines.Select(l => double.Parse(l, CultureInfo.InvariantCulture)).ToList();
            }
        }

        using (var client = new HttpClient()) {
            client.Timeout = TimeSpan.FromMinutes(5);

            string timeRange = input.GetType().GetProperty("timeRange").GetValue(input).ToString();
            var content = new StringContent(
                JsonConvert.SerializeObject(input),
                Encoding.UTF8,
                "application/json"
            );

            Console.WriteLine($"{prefix} {symbol} Calling Google Trends ({timeRange}) actor for search term '{name}'...");
            var response = await client.PostAsync(
                $"https://api.apify.com/v2/acts/{GoogleTrends_ActorId}/run-sync-get-dataset-items?token={ApifyToken}&format=json",
                content
            );

            response.EnsureSuccessStatusCode();
            var responseBody = await response.Content.ReadAsStringAsync();

            var inputDataList = JsonConvert.DeserializeObject<List<GoogleTrends_ReturnData>>(responseBody);
            if (inputDataList == null || !inputDataList.Any()) {
                return new List<double>();
            }

            var values = inputDataList[0].interestOverTime_timelineData?
                .Where(dataPoint => dataPoint.Value != null && dataPoint.Value.Count > 0)
                .Select(dataPoint => (double)dataPoint.Value[0])
                .ToList() ?? new List<double>();

            await File.WriteAllLinesAsync(csvPath, values.Select(v => v.ToString(CultureInfo.InvariantCulture)));

            return values;
        }
    }

    private static async Task<string> GetTwitterFollowersOrFollowing(string prefix, string symbol, string name, object input, string x_account) {
        Directory.CreateDirectory(CACHE_DIR);
        var jsonPath = Path.Combine(CACHE_DIR, $"X_{prefix}_{symbol}_{name.Replace(" ", "_")}.json");

        if (File.Exists(jsonPath)) {
            var fileInfo = new FileInfo(jsonPath);
            if (DateTime.UtcNow - fileInfo.LastWriteTimeUtc < TimeSpan.FromHours(24)) {
                Console.WriteLine($"{prefix} GetTwitterFollowersOrFollowing - Using cached data for {symbol} {name} from {jsonPath}");
                string json = await File.ReadAllTextAsync(jsonPath);
                return json;
                //var data = JsonConvert.DeserializeObject<List<Twitter_ReturnData>>(json);
                //if (data == null) return 0;
                //var old_followers = data.Where(p => DateTime.ParseExact(p.Created_At, "ddd MMM dd HH:mm:ss +0000 yyyy", CultureInfo.InvariantCulture) < DateTime.UtcNow.AddMonths(-1)).ToList();
                //return old_followers.Sum(p => p.Followers_Count); // exclude accounts created in last one month
            }
        }

        using (var client = new HttpClient()) {
            client.Timeout = TimeSpan.FromMinutes(5);

            var content = new StringContent(
                JsonConvert.SerializeObject(input),
                Encoding.UTF8,
                "application/json"
            );

            Console.WriteLine($"{prefix} {symbol} Calling X Followers Scraper actor for search term '{x_account}'...");
            var response = await client.PostAsync(
                $"https://api.apify.com/v2/acts/{TwitterFollowers_ActorId}/run-sync-get-dataset-items?token={ApifyToken}&format=json",
                content
            );

            response.EnsureSuccessStatusCode();
            var responseBody = await response.Content.ReadAsStringAsync();
            if (string.IsNullOrEmpty(responseBody)) {
                return null;
            }

            await File.WriteAllTextAsync(jsonPath, responseBody);

            return responseBody;
            //var data = JsonConvert.DeserializeObject<List<Twitter_ReturnData>>(responseBody);
            //if (data == null) return 0;
            //var old_followers = data.Where(p => DateTime.ParseExact(p.Created_At, "ddd MMM dd HH:mm:ss +0000 yyyy", CultureInfo.InvariantCulture) < DateTime.UtcNow.AddMonths(-1)).ToList();
            //return old_followers.Sum(p => p.Followers_Count); // exclude accounts created in last one month
        }
    }

    private static async void AnalyzeAndSaveTrend(string prefix, string symbol, string name, List<double> values, int rowId, string timeRange, string mint) {
        try {
            string tr_prefix = prefix == "6HR" ? "tr1"
                            : prefix == "12HR" ? "tr2"
                                               : "tr0";

            if (values == null || values.Count == 0) {
                HandleEmptyData(prefix, rowId, -999);
                return;
            }
            if (values.Count < 10) {
                HandleEmptyData(prefix, rowId, -777);
                return;
            }

            var analysisResult = AnalyzeTrend(values);

            using (var connection = new SqlConnection(DbConStr)) {
                connection.Open();
                string updateQuery = @$"
                        UPDATE [dbo].[mint]
                        SET {tr_prefix}_slope = @Slope,
                            {tr_prefix}_pvalue = @TrendScore
                        WHERE id = @RowId";

                using (var command = new SqlCommand(updateQuery, connection)) {
                    command.Parameters.AddWithValue("@Slope", double.IsNaN(analysisResult.Slope) ? 0 : analysisResult.Slope);
                    command.Parameters.AddWithValue("@TrendScore", analysisResult.TrendScore);
                    command.Parameters.AddWithValue("@RowId", rowId);
                    command.ExecuteNonQuery();
                }
            }

            // make graph
            string graphPath = GenerateTrendGraph(
                values,
                analysisResult.Slope,
                analysisResult.TrendScore,
                $"{prefix}_{symbol}",
                timeRange,
                name, 
                mint
            );

            // Upload to IPFS
            string ipfsHash = await IpfsUploader.UploadToIpfsAsync(graphPath, symbol, prefix);
            if (ipfsHash != null) {
                string ipfsUrl = $"https://gateway.pinata.cloud/ipfs/{ipfsHash}";
                using (var connection = new SqlConnection(DbConStr)) {
                    await connection.OpenAsync();
                    var sql = @$"
                        UPDATE [mint]
                            SET {tr_prefix}_graph_ipfs = @ipfs
                        WHERE id = @RowId";
                    using (var command = new SqlCommand(sql, connection)) {
                        command.Parameters.AddWithValue("@ipfs", ipfsUrl);
                        command.Parameters.AddWithValue("@RowId", rowId);
                        command.ExecuteNonQuery();
                        Console.WriteLine($"{prefix} ${symbol} Saved GT graph ipfsUrl: {ipfsUrl}");
                    }
                }
            }

            string info = $"{prefix} *{symbol}* {name} {mint} " +
            $"Slope: {analysisResult.Slope:F3}, Trend Score: {analysisResult.TrendScore:F3} - " +
            $"{(analysisResult.HasUpwardTrend ? "UPTREND!" : "no trend.")}";
            using (var stream = File.OpenRead(graphPath)) {
                Bot.SendPhotoAsync(
                    photo: Telegram.Bot.Types.InputFile.FromStream(stream),
                    caption: info
                ).Wait();
            }

            Console.WriteLine(info);
        }
        catch (Exception ex) {
            Console.WriteLine($"{prefix} ${symbol} #### An error occurred: {ex.Message}");
        }
    }

    private static string GenerateTrendGraph(List<double> values, double slope, double trendScore, string symbol, string timeRange, string name, string mint) {
        // Create plot
        var plot = new Plot(600, 400);

        // Generate x values
        double[] xValues = Enumerable.Range(0, values.Count).Select(x => (double)x).ToArray();
        double[] yValues = values.ToArray();

        // Plot actual values
        plot.AddScatter(xValues, yValues, /*MarkerShape.filledCircle,*/ System.Drawing.Color.Blue, 1.5f);

        // Plot trend line
        double intercept = values.Average() - slope * xValues.Average();
        double[] trendY = xValues.Select(x => slope * x + intercept).ToArray();

        // Add line with start and end points
        plot.AddLine(
            x1: xValues[0],               // start x
            y1: trendY[0],               // start y
            x2: xValues[xValues.Length - 1],// end x
            y2: trendY[^1],              // end y
            color: System.Drawing.Color.Red,
            lineWidth: 2);

        // Customize the plot
        plot.Title($"Google Trends: {symbol} '{name}' @{DateTime.Now.ToString("dd MMM yyyy HH:mm")}");
        plot.XLabel($"{timeRange}");
        plot.YLabel("n");

        // Add stats text
        var stats = $"Slope: {slope:F3}\nR²: {trendScore:F3}";
        plot.AddAnnotation(stats, 10, 10);

        // Save to temp file
        string tempPath = Path.Combine(Path.GetTempPath(), $"trend_{symbol}_{DateTime.Now.Ticks}.png");
        plot.SaveFig(tempPath);

        return tempPath;
    }

    private static TrendAnalysisResult AnalyzeTrend(List<double> values) {
        if (values == null || values.Count < 4) {
            return new TrendAnalysisResult { Slope = 0, TrendScore = 0, HasUpwardTrend = false };
        }

        try {
            int n = values.Count;
            var xValues = Enumerable.Range(0, n).Select(x => (double)x).ToList();

            // Calculate means
            double xMean = xValues.Average();
            double yMean = values.Average();

            // Calculate linear regression coefficients
            double numerator = 0;
            double denominator = 0;

            for (int i = 0; i < n; i++) {
                double xDiff = xValues[i] - xMean;
                double yDiff = values[i] - yMean;
                numerator += xDiff * yDiff;
                denominator += xDiff * xDiff;
            }

            // Calculate slope (beta1)
            double slope = denominator != 0 ? numerator / denominator : 0;
            if (double.IsNaN(slope)) slope = 0;

            // Calculate R-squared
            double rSquared = 0;
            if (denominator != 0) {
                double beta0 = yMean - slope * xMean;

                double totalSS = values.Sum(y => Math.Pow(y - yMean, 2));
                double regressionSS = values.Zip(xValues, (y, x) =>
                    Math.Pow((beta0 + slope * x) - yMean, 2)).Sum();

                rSquared = totalSS != 0 ? regressionSS / totalSS : 0;
                if (double.IsNaN(rSquared)) rSquared = 0;
            }

            // Determine if there's a significant upward trend
            // Considering both slope and R-squared for confidence
            bool hasUpwardTrend = slope > TREND_MIN_SLOPE && rSquared > TREND_MIN_RSQ;

            return new TrendAnalysisResult {
                Slope = slope,
                TrendScore = rSquared,
                HasUpwardTrend = hasUpwardTrend
            };
        }
        catch (Exception) {
            return new TrendAnalysisResult { Slope = 0, TrendScore = 0, HasUpwardTrend = false };
        }
    }

    private class TrendAnalysisResult {
        public double Slope { get; set; }
        public double TrendScore { get; set; }
        public bool HasUpwardTrend { get; set; }
    }

    private static void HandleEmptyData(string prefix, int rowId, double errorCode) {
        string tr_prefix = prefix == "6HR" ? "tr1"
                        : prefix == "12HR" ? "tr2"
                                           : "tr0";

        using (var connection = new SqlConnection(DbConStr)) {
            connection.Open();
            string updateQuery = @$"UPDATE [dbo].[mint] SET {tr_prefix}_slope = @ErrorCode, {tr_prefix}_pvalue = @ErrorCode WHERE id = @RowId";
            using (var command = new SqlCommand(updateQuery, connection)) {
                command.Parameters.AddWithValue("@ErrorCode", errorCode);
                command.Parameters.AddWithValue("@RowId", rowId);
                command.ExecuteNonQuery();
            }
        }
    }

    private class GoogleTrends_ReturnData {
        public string InputUrlOrTerm { get; set; }
        public string SearchTerm { get; set; }
        public List<TimelineData> interestOverTime_timelineData { get; set; }
    }
    private class TimelineData {
        public string Time { get; set; }
        public string FormattedTime { get; set; }
        public string FormattedAxisTime { get; set; }
        public List<int> Value { get; set; }
        public List<bool> HasData { get; set; }
        public List<string> FormattedValue { get; set; }
        public bool? IsPartial { get; set; }
    }

    private class Twitter_ReturnData {
        public string Type { get; set; }
        public string Target_Username { get; set; }
        public long Id { get; set; }
        public string Name { get; set; }
        public string Screen_Name { get; set; }
        public string Description { get; set; }
        public string? Url { get; set; }
        public string Profile_Image_Url_Https { get; set; }
        public string Profile_Banner_Url { get; set; }
        public int Followers_Count { get; set; }
        public int Fast_Followers_Count { get; set; }
        public int Normal_Followers_Count { get; set; }
        public int Friends_Count { get; set; }
        public int Listed_Count { get; set; }
        public int Favourites_Count { get; set; }
        public int Statuses_Count { get; set; }
        public int Media_Count { get; set; }
        public string Created_At { get; set; }
        public bool Verified { get; set; }
        public string Location { get; set; }
        public bool Protected { get; set; }
        public bool Geo_Enabled { get; set; }
        public bool Is_Translator { get; set; }
        public bool Has_Extended_Profile { get; set; }
        public bool Default_Profile { get; set; }
        public bool Default_Profile_Image { get; set; }
        public TwitterFollowers_ReturnData_Status Status { get; set; }

    }
    private class TwitterFollowers_ReturnData_Status {
        public string Created_At { get; set; }
        public long Id { get; set; }
        public string Text { get; set; }
    }

    private static async void AnalyzeAndSaveXScore(string prefix, string symbol, string name, int x_score, int rowId, string mint) {
        try {
            string hr_prefix = prefix == "6HR" ? "hr6"
                                               : "hr12";

            using (var connection = new SqlConnection(DbConStr)) {
                connection.Open();
                string updateQuery = @$"
                        UPDATE [dbo].[mint]
                        SET {hr_prefix}_x_score = @XScore
                        WHERE id = @RowId";

                using (var command = new SqlCommand(updateQuery, connection)) {
                    command.Parameters.AddWithValue("@XScore", x_score);
                    command.Parameters.AddWithValue("@RowId", rowId);
                    command.ExecuteNonQuery();
                }
            }

            var info = $"{prefix} AnalyzeAndSaveXScore {symbol} x_score={x_score} - SAVED.";
            Console.WriteLine(info);
        }
        catch (Exception ex) {
            Console.WriteLine($"{prefix} ${symbol} #### An error occurred: {ex.Message}");
        }
    }

    #region IPFS Graph Saver
    private class PinataResponse {
        [JsonProperty("IpfsHash")]
        public string IpfsHash { get; set; }

        [JsonProperty("PinSize")]
        public long PinSize { get; set; }

        [JsonProperty("Timestamp")]
        public string Timestamp { get; set; }
    }
    private class GraphMetadata {
        public string Symbol { get; set; }
        public string Timeframe { get; set; }
        public DateTime Timestamp { get; set; }
        public string IpfsHash { get; set; }
        public string IpfsUrl { get; set; }
        public double Slope { get; set; }
        public double TrendScore { get; set; }
    }

    private static class IpfsUploader {
        private static readonly string PinataApiKey = ConfigurationManager.AppSettings["PinataApiKey"];
        private static readonly string PinataSecretKey = ConfigurationManager.AppSettings["PinataSecretApiKey"];
        private static readonly string DbConStr = ConfigurationManager.ConnectionStrings["pf.Properties.Settings.mintDbConnectionString"].ConnectionString;

        public static async Task<string> UploadToIpfsAsync(string filePath, string symbol, string timeframe) {
            try {
                using var client = new HttpClient();
                client.DefaultRequestHeaders.Add("pinata_api_key", PinataApiKey);
                client.DefaultRequestHeaders.Add("pinata_secret_api_key", PinataSecretKey);

                using var form = new MultipartFormDataContent();
                using var fileStream = File.OpenRead(filePath);
                using var streamContent = new StreamContent(fileStream);

                form.Add(streamContent, "file", Path.GetFileName(filePath));

                // Add metadata
                var metadata = new {
                    name = $"{timeframe}_{symbol}_trend_graph",
                    keyvalues = new {
                        symbol = symbol,
                        timeframe = timeframe,
                        timestamp = DateTime.UtcNow
                    }
                };

                var metadataContent = new StringContent(
                    JsonConvert.SerializeObject(metadata),
                    Encoding.UTF8,
                    "application/json"
                );
                form.Add(metadataContent, "pinataMetadata");

                var response = await client.PostAsync("https://api.pinata.cloud/pinning/pinFileToIPFS", form);
                var responseContent = await response.Content.ReadAsStringAsync();

                if (!response.IsSuccessStatusCode) {
                    throw new Exception($"Failed to upload to IPFS: {responseContent}");
                }

                var pinataResponse = JsonConvert.DeserializeObject<PinataResponse>(responseContent);
                return pinataResponse.IpfsHash;
            }
            catch (Exception ex) {
                Console.WriteLine($"Error uploading to IPFS: {ex.Message}");
                return null;
            }
        }

        public static async Task SaveGraphMetadataAsync(GraphMetadata metadata) {

        }
    }
    #endregion

    private static void TestFromCache() {
        //var CACHE_DIR = Path.Combine("..", "..", "..", "cache");
        if (!Directory.Exists(CACHE_DIR)) {
            Console.WriteLine($"Cache directory not found: {CACHE_DIR}");
            return;
        }

        var csvFiles = Directory.GetFiles(CACHE_DIR, "*.csv");
        foreach (var csvFile in csvFiles) {
            try {
                // Get symbol and name from filename
                var fileName = Path.GetFileNameWithoutExtension(csvFile);
                var separatorIndex = fileName.IndexOf('_');
                string symbol = separatorIndex >= 0 ? fileName.Substring(0, separatorIndex) : fileName;
                string name = separatorIndex >= 0 ? fileName.Substring(separatorIndex + 1).Replace("_", " ") : "";

                // Read values from CSV
                var lines = File.ReadAllLines(csvFile);
                var values = lines.Select(l => double.Parse(l, CultureInfo.InvariantCulture)).ToList();

                // Call AnalyzeAndSaveTrend
                AnalyzeAndSaveTrend("test", symbol, symbol, values, -1, "", "");
            }
            catch (Exception ex) {
                Console.WriteLine($"Error processing file {csvFile}: {ex.Message}");
            }
        }
    }

    private static string FormatCurrency(double value) {
        if (value >= 1_000_000_000) // Billions
            return $"${value / 1_000_000_000:F1}B";
        if (value >= 1_000_000) // Millions 
            return $"${value / 1_000_000:F1}M";
        if (value >= 1_000) // Thousands
            return $"${value / 1_000:F1}K";

        return $"${value:F0}"; // Less than 1000
    }

    private static string FormatMillions(double value) {
        // Convert to millions
        double inMillions = value / 1_000_000;

        // Handle values 10M and above
        if (inMillions >= 10)
            return $"{Math.Floor(inMillions)}M";

        // Handle values under 10M with one decimal
        return $"{inMillions:F1}M";
    }
    private static string FormatZ(double value) {
        if (value >= 100)
            return Math.Floor(value).ToString();

        return $"{value:F1}";
    }

    private static string FormatThousandsToK(double number) {
        if (number >= 1000)
            return (number / 1000).ToString("0.0") + "k";
        return number.ToString("0");
    }
}
