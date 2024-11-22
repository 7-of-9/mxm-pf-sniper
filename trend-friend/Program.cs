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

class Program {
    private static readonly string ApifyToken = ConfigurationManager.AppSettings["ApifyApiToken"];
    private static readonly string ActorId = "emastra~google-trends-scraper";
    private static readonly string _connectionString = ConfigurationManager.ConnectionStrings["pf.Properties.Settings.mintDbConnectionString"].ConnectionString;

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

    static async Task Main(string[] args) {
        
        // test 
        AnalyzeAndSaveTrend("BARD", File.ReadAllText(@"..\..\..\bardai.json"), -1); // periodic, no trend
        AnalyzeAndSaveTrend("HOLD", File.ReadAllText(@"..\..\..\whatifweallhold.json"), -1); // periodic, no trend

        //AnalyzeAndSaveTrend("HUSKY", File.ReadAllText(@"..\..\..\tiktoktalkinghusky.json"), -1); // one spike, no trend

        AnalyzeAndSaveTrend("CHILLGUY", File.ReadAllText(@"..\..\..\chillguy.json"), -1); // uptrend at end, clear
        AnalyzeAndSaveTrend("CHILLFAM", File.ReadAllText(@"..\..\..\chillfamily.json"), -1); // uptrend at end, clear

        /*while (true) {
            try {
                // Query the top 5% of rows with missing tr1_slope, inserted in the last 6 hrs
                List<(int rowId, string name, string symbol)> rowsToUpdate = new List<(int, string, string)>();
                using (var connection = new SqlConnection(_connectionString)) {
                    await connection.OpenAsync();
                    string query = @"
                        SELECT TOP 5 PERCENT id, name, symbol
                        FROM hr1_avg_mc
                        WHERE inserted_utc >= DATEADD(HOUR, -6, GETUTCDATE())
                        AND tr1_slope IS NULL
                        ORDER BY z_score DESC";

                    using (var command = new SqlCommand(query, connection)) {
                        using (var reader = await command.ExecuteReaderAsync()) {
                            while (await reader.ReadAsync()) {
                                rowsToUpdate.Add((reader.GetInt32(0), reader.GetString(1), reader.GetString(2)));
                            }
                        }
                    }
                }

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
                        var jsonResponse = await RunActorSyncAsync(row.symbol, row.name, input);

                        // save DB
                        if (!string.IsNullOrEmpty(jsonResponse)) {
                            AnalyzeAndSaveTrend(row.symbol, jsonResponse, row.rowId);
                        }
                    }));

                // Poll every minute
                await Task.Delay(TimeSpan.FromMinutes(1));
            }
            catch (Exception ex) {
                Console.WriteLine($"Error occurred: {ex.Message}");
            }
        }*/
    }

    private static async Task<string> RunActorSyncAsync(string symbol, string name, object input) {
        using (var client = new HttpClient()) {
            client.Timeout = TimeSpan.FromMinutes(5); // Set timeout to 5 minutes to handle long-running requests

            var content = new StringContent(
                Newtonsoft.Json.JsonConvert.SerializeObject(input),
                Encoding.UTF8,
                "application/json"
            );

            Console.WriteLine($"Calling Google Trends actor: ${symbol} {name}...");
            var response = await client.PostAsync(
                $"https://api.apify.com/v2/acts/{ActorId}/run-sync-get-dataset-items?token={ApifyToken}&format=json",
                content
            );

            response.EnsureSuccessStatusCode();
            var responseBody = await response.Content.ReadAsStringAsync();
            return responseBody;
        }
    }

    public static void AnalyzeAndSaveTrend(string symbol, string jsonData, int rowId) {
        try {
            // Deserialize and validation checks remain the same...
            var inputDataList = JsonConvert.DeserializeObject<List<InputData>>(jsonData);
            if (inputDataList == null || !inputDataList.Any()) {
                HandleEmptyData(rowId, -999);
                return;
            }

            var timelineData = inputDataList[0].interestOverTime_timelineData;
            if (timelineData == null || !timelineData.Any()) {
                HandleEmptyData(rowId, -888);
                return;
            }

            // Extract values
            var values = timelineData
                .Where(dataPoint => dataPoint.Value != null && dataPoint.Value.Count > 0)
                .Select(dataPoint => (double)dataPoint.Value[0])
                .ToList();

            if (values.Count < 10) // Require minimum points for reliable analysis
            {
                HandleEmptyData(rowId, -777);
                return;
            }

            // Calculate multiple indicators
            var analysisResult = AnalyzeTrend(values);

            // Save results to database
            using (var connection = new SqlConnection(_connectionString)) {
                connection.Open();
                string updateQuery = @"
                UPDATE [dbo].[mint]
                SET tr1_slope = @Slope,
                    tr1_pvalue = @TrendScore
                WHERE id = @RowId";

                using (var command = new SqlCommand(updateQuery, connection)) {
                    command.Parameters.AddWithValue("@Slope", analysisResult.Slope);
                    command.Parameters.AddWithValue("@TrendScore", analysisResult.TrendScore);
                    command.Parameters.AddWithValue("@RowId", rowId);
                    command.ExecuteNonQuery();
                }
            }

            Console.WriteLine($"{symbol}\trowId {rowId} //\t" +
                $"Slope: {analysisResult.Slope:F3}, Trend Score: {analysisResult.TrendScore:F3} - " +
                $"{(analysisResult.HasUpwardTrend ? "Upward trend detected!" : "No clear upward trend.")}");
        }
        catch (Exception ex) {
            Console.WriteLine($"An error occurred: {ex.Message}");
        }
    }

    private static TrendAnalysisResult AnalyzeTrend(List<double> values) {
        int n = values.Count;

        // Split the series into segments for more granular analysis
        int segmentSize = n / 4; // Analyze quarters of the data
        var segments = new List<List<double>>();
        for (int i = 0; i < n; i += segmentSize) {
            segments.Add(values.Skip(i).Take(segmentSize).ToList());
        }

        // Calculate baseline (average of first segment) and peak (max of any later segment)
        double baseline = segments.First().Average();
        double peak = segments.Skip(1).SelectMany(s => s).Max();

        // Calculate relative increase from baseline
        double relativeIncrease = (peak - baseline) / Math.Max(baseline, 1.0);

        // Calculate sustained elevation
        // (what proportion of later values are significantly above baseline?)
        double threshold = baseline + (baseline * 0.5); // 50% above baseline
        int elevatedPoints = values.Skip(segmentSize)  // Skip first segment
                                  .Count(v => v > threshold);
        double sustainedElevation = (double)elevatedPoints / (n - segmentSize);

        // Calculate final trend score
        // Combine relative increase and sustained elevation
        double trendScore = (relativeIncrease * 0.7) + (sustainedElevation * 0.3);

        // Determine if there's a meaningful upward trend
        // More lenient thresholds, focused on sustained increase from baseline
        bool hasUpwardTrend =
            (trendScore > 20.0) || // Clear strong trend
            (relativeIncrease > 30.0 && trendScore > 15.0); // Strong rise with decent sustainability

        return new TrendAnalysisResult {
            Slope = relativeIncrease,
            TrendScore = trendScore,
            HasUpwardTrend = hasUpwardTrend
        };
    }

    private class TrendAnalysisResult {
        public double Slope { get; set; }
        public double TrendScore { get; set; }
        public bool HasUpwardTrend { get; set; }
    }

    private static void HandleEmptyData(int rowId, double errorCode) {
        using (var connection = new SqlConnection(_connectionString)) {
            connection.Open();
            string updateQuery = @"UPDATE [dbo].[mint] SET tr1_slope = @ErrorCode, tr1_pvalue = @ErrorCode WHERE id = @RowId";
            using (var command = new SqlCommand(updateQuery, connection)) {
                command.Parameters.AddWithValue("@ErrorCode", errorCode);
                command.Parameters.AddWithValue("@RowId", rowId);
                command.ExecuteNonQuery();
            }
        }
    }

    public static (double Tau, double PValue) MannKendallTrendTest(List<double> values) {
        int n = values.Count;
        int s = 0; // Mann-Kendall statistic
        int varS = 0; // Variance of S

        // Compute S statistic
        for (int i = 0; i < n - 1; i++) {
            for (int j = i + 1; j < n; j++) {
                if (values[j] > values[i]) {
                    s += 1;
                }
                else if (values[j] < values[i]) {
                    s -= 1;
                }
                // Ties (values[j] == values[i]) do not contribute to S
            }
        }

        // Handle ties in the data
        var uniqueValues = values.GroupBy(v => v).Select(g => g.Count()).ToList();
        bool hasTies = uniqueValues.Any(count => count > 1);

        // Compute variance of S
        if (n <= 10) {
            // For small samples, use exact tables (not implemented here)
            throw new InvalidOperationException("Sample size too small for normal approximation.");
        }
        else {
            // For larger samples, use normal approximation
            if (hasTies) {
                // Adjust variance for ties
                var tieCounts = uniqueValues.Where(count => count > 1).ToList();
                double tieSum = tieCounts.Sum(count => count * (count - 1) * (2 * count + 5));
                varS = (int)((n * (n - 1) * (2 * n + 5) - tieSum) / 18);
            }
            else {
                varS = n * (n - 1) * (2 * n + 5) / 18;
            }
        }

        // Compute Z statistic
        double z = 0;
        if (s > 0) {
            z = (s - 1) / Math.Sqrt(varS);
        }
        else if (s < 0) {
            z = (s + 1) / Math.Sqrt(varS);
        }
        else {
            z = 0;
        }

        // Compute p-value (two-tailed test)
        double pValue = 2 * (1 - Normal.CDF(0, 1, Math.Abs(z)));

        // Compute Tau
        double tau = s / (0.5 * n * (n - 1));

        return (tau, pValue);
    }

}
