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

public class Program {
    private static readonly string ApifyToken = ConfigurationManager.AppSettings["ApifyApiToken"];
    private static readonly string ActorId = "emastra~google-trends-scraper";
    private static readonly string DbConStr = ConfigurationManager.ConnectionStrings["pf.Properties.Settings.mintDbConnectionString"].ConnectionString;
    private static readonly TelegramBot Bot = new TelegramBot(ConfigurationManager.AppSettings["TelegramBotToken"]);

    const double TREND_MIN_SLOPE = 0.05;
    const double TREND_MIN_RSQ = 0.1;

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
                AnalyzeAndSaveTrend(symbol, symbol, values, -1);
            }
            catch (Exception ex) {
                Console.WriteLine($"Error processing file {csvFile}: {ex.Message}");
            }
        }
    }

    public static string FormatCurrency(double value) {
        if (value >= 1_000_000_000) // Billions
            return $"${value / 1_000_000_000:F1}B";
        if (value >= 1_000_000) // Millions 
            return $"${value / 1_000_000:F1}M";
        if (value >= 1_000) // Thousands
            return $"${value / 1_000:F1}K";

        return $"${value:F0}"; // Less than 1000
    }

    static async Task Main(string[] args) {
        Bot.StartAsync();

        // tests
        //AnalyzeAndSaveTrend("BARD", "", System.IO.File.ReadAllLines(@"..\..\..\cache\NAS_Not_a_Security.csv").Select(p => double.Parse(p)).ToList(), -1); // periodic, no trend
        //AnalyzeAndSaveTrend("CHILLGUY", "", System.IO.File.ReadAllLines(@"..\..\..\cache\CHILLGUY_Chill_Guy.csv").Select(p => double.Parse(p)).ToList(), -1); // uptrend at end, clear

        // get trending tokens into the cache for testing...
        //AnalyzeAndSaveTrend("CHILLGUY", "", await RunActorSyncAsync("CHILLGUY", "Chill Guy", new {
        //    isMultiple = false,
        //    isPublic = false,
        //    searchTerms = new[] { "Chill Guy" },
        //    skipDebugScreen = false,
        //    timeRange = "now 7-d",
        //    viewedFrom = "us"
        //}), -1);
        //AnalyzeAndSaveTrend("CHILLFAM", "", await RunActorSyncAsync("CHILLFAM", "chill family", new {
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

        List<string> symbolsSeen = new List<string>();
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
                            tr1_slope, tr1_pvalue, mint, z_score, hr6_holder, Uri, hr1_icon, hr6_market_cap, 
                            hr6_price, hr1_price
                        FROM hr1_avg_mc 
                        WHERE inserted_utc BETWEEN DATEADD(HOUR, -12, GETUTCDATE()) AND DATEADD(HOUR, -6, GETUTCDATE())
	                        AND z_score > 0
                            AND hr6_price IS NOT NULL
                            AND hr6_holder > hr1_holder
                            AND hr6_price > hr1_price
                            --AND tr1_slope is null 
                        ORDER BY z_score DESC
                        ";

                    int pos = 0;
                    Bot.Currents = new List<string>();
                    bool sawNew = false;

                    var tableBuilder = new StringBuilder();
                    tableBuilder.AppendLine("```");
                    tableBuilder.AppendLine("Symbol    Name          Age   Score   Holders");
                    tableBuilder.AppendLine("--------------------------------------------");

                    using (var command = new SqlCommand(query, connection)) {
                        using (var reader = await command.ExecuteReaderAsync()) {
                            while (await reader.ReadAsync()) {
                                int id = reader.GetInt32(0);
                                string name = reader.GetString(1);
                                string symbol = reader.GetString(2);
                                int hrs_old = reader.GetInt32(3);
                                double? tr1_slope = reader.IsDBNull(4) ? null : reader.GetDouble(4);
                                double? tr1_pvalue = reader.IsDBNull(5) ? null : reader.GetDouble(5);
                                string mint = reader.GetString(6);
                                double z_score = reader.GetDouble(7);
                                int hr6_holder = reader.GetInt32(8);
                                string uri = reader.GetString(9);
                                //string icon = reader.IsDBNull(10) ? null : reader.GetString(10);
                                double hr6_mc = reader.GetDouble(11);

                                var metadata = await TokenMetadataParser.ParseMetadataFromUri(uri);

                                bool? tr1_trend = tr1_slope != null && tr1_pvalue != null 
                                                    ? (tr1_slope > TREND_MIN_SLOPE && tr1_pvalue > TREND_MIN_RSQ) : null;
                                string tr1_info = "Google Trends: " + (tr1_trend == null ? "tbd" : (tr1_trend == true ? "CONFIRMED !!!" : "none"));

                                //if (metadata != null) {
                                //    Console.WriteLine($"Description: {metadata.Description}");
                                //    Console.WriteLine($"Twitter: {metadata.Twitter}");
                                //    Console.WriteLine($"Telegram: {metadata.Telegram}");
                                //    Console.WriteLine($"Website: {metadata.Website}");
                                //}

                                //Console.WriteLine($"{id} {symbol} {name} tr1_slope={tr1_slope}");
                                rowsToCheck.Add((symbol, mint));
                                if (tr1_slope == null) { // if not already got trend1 data
                                    rowsToUpdate.Add((id, name, symbol));
                                }
                                pos++;
                                string row_info =
                                     $"#{pos} [{symbol}](https://dexscreener.com/solana/{mint}) *{name}* ([website]({metadata.Website})) ([twitter]({metadata.Twitter}))"
                                      + $" / MC={FormatCurrency(hr6_mc)} / age={hrs_old * -1} hrs / z\\_score={z_score.ToString("0.00")} / hr6\\_holders={hr6_holder}"
                                      + $" / {tr1_info}"
                                      + "\n_" + metadata.Description + "_";

                                // Add formatted table row
                                tableBuilder.AppendLine(String.Format("{0,-9} {1,-13} {2,4}h {3,7:F1} {4,8:N0}",
                                    (symbol.Length > 8 ? symbol.Substring(0, 8) : symbol),
                                    name.Length > 12 ? name.Substring(0, 12) : name,
                                    hrs_old * -1,
                                    z_score,
                                    hr6_holder
                                ));

                                if (!symbolsSeen.Contains(symbol)) {
                                    Bot.BroadcastMessageAsync(row_info);
                                    symbolsSeen.Add(symbol);
                                    sawNew = true; 
                                }

                                Bot.Currents.Add(row_info);
                            }
                        }
                    }

                    // After the loop, close the code block and send the table
                    tableBuilder.AppendLine("```");
                    Bot.Currents.Add(tableBuilder.ToString());

                    if (sawNew) {
                        Bot.BroadcastMessageAsync(tableBuilder.ToString());
                    }
                }
                var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
                //var info = $"{timestamp}\n{string.Join("\n", rowsToCheck.Select(p => $"{p.symbol} [{p.mint}]").ToList()).Trim()}";
                var info = $"{timestamp} {string.Join(", ", rowsToCheck.Select(p => $"{p.symbol}").ToList()).Trim()}";
                Console.WriteLine(info);
                //Bot.BroadcastMessageAsync(info);

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
                            AnalyzeAndSaveTrend(row.symbol, row.name, values, row.rowId);
                        }
                        else {
                            HandleEmptyData(row.rowId, -888);
                        }
                    }));

                // sleep
                await Task.Delay(TimeSpan.FromMinutes(1));
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

    public static void AnalyzeAndSaveTrend(string symbol, string name, List<double> values, int rowId) {
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

            // Generate and save the graph
            string graphPath = 
                GenerateTrendGraph(
                values,
                analysisResult.Slope,
                analysisResult.TrendScore,
                symbol
            );

            // broadcast result
            string info = $"*{symbol}* {name} " +
                $"Slope: {analysisResult.Slope:F3}, Trend Score: {analysisResult.TrendScore:F3} - " +
                $"{(analysisResult.HasUpwardTrend ? "UPTREND!" : "no trend.")}";

            // Send both text and image
            using (var stream = File.OpenRead(graphPath)) {
                Bot.SendPhotoAsync(
                    photo: Telegram.Bot.Types.InputFile.FromStream(stream),
                    caption: info
                ).Wait();
            }
            //Bot.BroadcastMessageAsync(info);

            Console.WriteLine(info);
        }
        catch (Exception ex) {
            Console.WriteLine($"${symbol} An error occurred: {ex.Message}");
        }
    }

    private static string GenerateTrendGraph(List<double> values, double slope, double trendScore, string symbol) {
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
        plot.Title($"Google Trends: {symbol}");
        plot.XLabel("7 DAYS");
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
