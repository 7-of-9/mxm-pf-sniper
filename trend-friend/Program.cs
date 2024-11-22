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

class Program {
    private static readonly string ApifyToken = ConfigurationManager.AppSettings["ApifyApiToken"];
    private static readonly string ActorId = "emastra~google-trends-scraper";
    private static readonly string DbConStr = ConfigurationManager.ConnectionStrings["pf.Properties.Settings.mintDbConnectionString"].ConnectionString;
    private static readonly TelegramBot Bot = new TelegramBot(ConfigurationManager.AppSettings["TelegramBotToken"]);

    public class InputData {
        public string InputUrlOrTerm { get; set; }
        public string SearchTerm { get; set; }
        public List<TimelineData> interestOverTime_timelineData { get; set; }
    }
    public class TimelineData {
        public string Time { get; set; }
        public string FormattedTime { get; set; }
        public string FormattedAxisTime { get; set; }
        public List<int> Value { get; set; }
        public List<bool> HasData { get; set; }
        public List<string> FormattedValue { get; set; }
        public bool? IsPartial { get; set; }
    }

    public static void TestFromCache() {
        var cacheDir = Path.Combine("..", "..", "..", "cache");
        if (!Directory.Exists(cacheDir)) {
            Console.WriteLine($"Cache directory not found: {cacheDir}");
            return;
        }

        var csvFiles = Directory.GetFiles(cacheDir, "*.csv");
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
                AnalyzeAndSaveTrend(symbol, values, -1);
            }
            catch (Exception ex) {
                Console.WriteLine($"Error processing file {csvFile}: {ex.Message}");
            }
        }
    }

    static async Task Main(string[] args) {
        Bot.StartAsync();

        // tests
        //AnalyzeAndSaveTrend("BARD", File.ReadAllText(@"..\..\..\bardai.json"), -1); // periodic, no trend
        //AnalyzeAndSaveTrend("HOLD", File.ReadAllText(@"..\..\..\whatifweallhold.json"), -1); // periodic, no trend

        ////AnalyzeAndSaveTrend("HUSKY", File.ReadAllText(@"..\..\..\tiktoktalkinghusky.json"), -1); // one spike, no trend

        //AnalyzeAndSaveTrend("CHILLGUY", File.ReadAllText(@"..\..\..\chillguy.json"), -1); // uptrend at end, clear
        //AnalyzeAndSaveTrend("CHILLFAM", File.ReadAllText(@"..\..\..\chillfamily.json"), -1); // uptrend at end, clear

        // get trending tokens into the cache for testing...
        //AnalyzeAndSaveTrend("CHILLGUY", await RunActorSyncAsync("CHILLGUY", "Chill Guy", new {
        //    isMultiple = false,
        //    isPublic = false,
        //    searchTerms = new[] { "Chill Guy" },
        //    skipDebugScreen = false,
        //    timeRange = "now 7-d",
        //    viewedFrom = "us"
        //}), -1);
        //AnalyzeAndSaveTrend("CHILLFAM", await RunActorSyncAsync("CHILLFAM", "chill family", new {
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

        while (true) {
            try {
                // Query the top 5% of rows with missing tr1_slope, inserted in the last 6 hrs
                List<(int rowId, string name, string symbol)> rowsToUpdate = new List<(int, string, string)>();
                List<(string symbol, string mint)> rowsToCheck = new List<(string, string)>();
                using (var connection = new SqlConnection(DbConStr)) {
                    await connection.OpenAsync();
                    string query = @"
                        SELECT TOP 50 PERCENT 
                            id, name, symbol, 
                            datediff(hh, getutcdate(), inserted_utc) 'hrs old',
                            tr1_slope, mint,
                            tr1_pvalue, hr6_price, hr1_price, *
                        FROM hr1_avg_mc 
                        WHERE inserted_utc BETWEEN DATEADD(HOUR, -12, GETUTCDATE()) AND DATEADD(HOUR, -6, GETUTCDATE())
	                        AND z_score > 0
                            AND hr6_price IS NOT NULL
                            AND hr6_price > hr1_price
                            --AND tr1_slope is null 
                        ORDER BY z_score DESC
                        ";

                    using (var command = new SqlCommand(query, connection)) {
                        using (var reader = await command.ExecuteReaderAsync()) {
                            while (await reader.ReadAsync()) {
                                int id = reader.GetInt32(0);
                                string name = reader.GetString(1);
                                string symbol = reader.GetString(2);
                                double? tr1_slope = reader.IsDBNull(4) ? null : reader.GetDouble(4);
                                string mint = reader.GetString(5);
                                //Console.WriteLine($"{id} {symbol} {name} tr1_slope={tr1_slope}");
                                rowsToCheck.Add((symbol, mint));
                                if (tr1_slope == null) { // if not already got trend1 data
                                    rowsToUpdate.Add((id, name, symbol));
                                }
                            }
                        }
                    }
                }
                var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
                var info = $"{timestamp}\n{string.Join("\n", rowsToCheck.Select(p => $"{p.symbol} [{p.mint}]").ToList()).Trim()}";
                Console.WriteLine(info);
                Bot.BroadcastMessageAsync(info);

                // Process each row using RunActorSyncAsync
                await Task.WhenAll(Parallel.ForEachAsync(rowsToUpdate,
                    new ParallelOptions { MaxDegreeOfParallelism = 8 },
                    async (row, token) => {
                        var input = new {
                            isMultiple = false,
                            isPublic = false,
                            searchTerms = new[] { row.name },
                            skipDebugScreen = false,
                            timeRange = "now 7-d",
                            viewedFrom = "us"
                        };

                        // get trend
                        var values = await RunActorSyncAsync(row.symbol, row.name, input);

                        // save DB
                        if (values.Any()) {
                            AnalyzeAndSaveTrend(row.symbol, values, row.rowId);
                        }
                        else {
                            HandleEmptyData(row.rowId, -888);
                        }
                    }));

                // sleep
                await Task.Delay(TimeSpan.FromMinutes(60));
            }
            catch (Exception ex) {
                Console.WriteLine($"Error occurred: {ex.Message}");
            }
        }
    }

    private static async Task<List<double>> RunActorSyncAsync(string symbol, string name, object input) {
        // Setup cache paths for both JSON and CSV
        var cacheDir = Path.Combine("..", "..", "..", "cache");
        Directory.CreateDirectory(cacheDir);
        var csvPath = Path.Combine(cacheDir, $"{symbol}_{name.Replace(" ", "_")}.csv");

        // Check CSV cache first
        if (File.Exists(csvPath)) {
            var fileInfo = new FileInfo(csvPath);
            if (DateTime.UtcNow - fileInfo.LastWriteTimeUtc < TimeSpan.FromHours(24)) {
                Console.WriteLine($"Using cached data for {symbol} {name} from {csvPath}");
                var lines = await File.ReadAllLinesAsync(csvPath);
                return lines.Select(l => double.Parse(l, CultureInfo.InvariantCulture)).ToList();
            }
        }

        // If not in cache or too old, fetch from API
        using (var client = new HttpClient()) {
            client.Timeout = TimeSpan.FromMinutes(5);

            var content = new StringContent(
                JsonConvert.SerializeObject(input),
                Encoding.UTF8,
                "application/json"
            );

            Console.WriteLine($"Calling Google Trends actor for {symbol} {name}...");
            var response = await client.PostAsync(
                $"https://api.apify.com/v2/acts/{ActorId}/run-sync-get-dataset-items?token={ApifyToken}&format=json",
                content
            );

            response.EnsureSuccessStatusCode();
            var responseBody = await response.Content.ReadAsStringAsync();

            // Parse JSON to extract values
            var inputDataList = JsonConvert.DeserializeObject<List<InputData>>(responseBody);
            if (inputDataList == null || !inputDataList.Any()) return new List<double>();

            var values = inputDataList[0].interestOverTime_timelineData?
                .Where(dataPoint => dataPoint.Value != null && dataPoint.Value.Count > 0)
                .Select(dataPoint => (double)dataPoint.Value[0])
                .ToList() ?? new List<double>();

            // Save to CSV cache
            await File.WriteAllLinesAsync(csvPath, values.Select(v => v.ToString(CultureInfo.InvariantCulture)));
            //Console.WriteLine($"Cached response for {symbol} {name} to {csvPath}");

            return values;
        }
    }

    public static void AnalyzeAndSaveTrend(string symbol, List<double> values, int rowId) {
        try {
            if (values == null || values.Count == 0) {
                HandleEmptyData(rowId, -999);
                return;
            }

            if (values.Count < 10) {
                HandleEmptyData(rowId, -777);
                return;
            }

            var analysisResult = AnalyzeTrend(values);

            //if (symbol == "Bullseye")
            //    Debugger.Break();

            // Save results to database
            using (var connection = new SqlConnection(DbConStr)) {
                connection.Open();
                string updateQuery = @"
                UPDATE [dbo].[mint]
                SET tr1_slope = @Slope,
                    tr1_pvalue = @TrendScore
                WHERE id = @RowId";

                using (var command = new SqlCommand(updateQuery, connection)) {
                    command.Parameters.AddWithValue("@Slope", double.IsNaN(analysisResult.Slope) ? 0 : analysisResult.Slope);
                    command.Parameters.AddWithValue("@TrendScore", analysisResult.TrendScore);
                    command.Parameters.AddWithValue("@RowId", rowId);
                    command.ExecuteNonQuery();
                }
            }

            string info = $"{symbol.PadLeft(20)}\trowId {rowId} //\t" +
                $"Slope: {analysisResult.Slope:F3}, Trend Score: {analysisResult.TrendScore:F3} - " +
                $"{(analysisResult.HasUpwardTrend ? "UPTREND!" : "")}";
            Bot.BroadcastMessageAsync(info);
            Console.WriteLine(info);
        }
        catch (Exception ex) {
            Console.WriteLine($"${symbol} An error occurred: {ex.Message}");
        }
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
            bool hasUpwardTrend = slope > 0.05 && rSquared > 0.1;

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

    private static void HandleEmptyData(int rowId, double errorCode) {
        using (var connection = new SqlConnection(DbConStr)) {
            connection.Open();
            string updateQuery = @"UPDATE [dbo].[mint] SET tr1_slope = @ErrorCode, tr1_pvalue = @ErrorCode WHERE id = @RowId";
            using (var command = new SqlCommand(updateQuery, connection)) {
                command.Parameters.AddWithValue("@ErrorCode", errorCode);
                command.Parameters.AddWithValue("@RowId", rowId);
                command.ExecuteNonQuery();
            }
        }
    }
}
