using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace meta_getter {
    class Program {
        private static readonly string _connectionString =
            ConfigurationManager.ConnectionStrings[
                "pf.Properties.Settings.ethosConnectionString"
            ].ConnectionString;
        private static readonly HttpClient _client = new HttpClient();
        private static readonly int _pollIntervalSeconds = 5;  // Poll interval
        private static readonly string _solScanApiUrl =
            "https://pro-api.solscan.io/v2.0/token/meta";
        private static readonly string _apiKey =
            ConfigurationManager.AppSettings["SolScanApiKey"];
        private static readonly int _maxRetries = 5;
        private static readonly int _retryDelayMilliseconds = 2000;

        public enum MetaTimeframe {
            OneHour,
            SixHours,
            TwelveHours,
            OneDay
        }

        static async Task Main(string[] args) {
            while (true) {
                try {
                    await PollAndProcessAsync(MetaTimeframe.OneHour);   // 1-hour
                    await PollAndProcessAsync(MetaTimeframe.SixHours);  // 6-hour
                    await PollAndProcessAsync(MetaTimeframe.TwelveHours); // 12-hour
                    //await PollAndProcessAsync(MetaTimeframe.OneDay);    // 1-day
                }
                catch (Exception ex) {
                    Console.WriteLine($"Error occurred: {ex.Message}");
                }
                // Wait before polling again
                await Task.Delay(_pollIntervalSeconds * 1000);
            }
        }

        private static async Task PollAndProcessAsync(MetaTimeframe timeframe) {
            List<(int rowId, string mint)> rowsToUpdate = new List<(int, string)>();
            string metaType = timeframe switch {
                MetaTimeframe.OneHour => "1-hour",
                MetaTimeframe.SixHours => "6-hour",
                MetaTimeframe.TwelveHours => "12-hour",
                MetaTimeframe.OneDay => "1-day",
                _ => "unknown"
            };

            try {
                using (var connection = new SqlConnection(_connectionString)) {
                    await connection.OpenAsync();
                    string query;
                    if (timeframe == MetaTimeframe.OneHour) {
                        query = @"
                            SELECT [id], [mint]
                            FROM [dbo].[mint]
                            WHERE [inserted_utc] BETWEEN
                                DATEADD(MINUTE, -120, GETUTCDATE())
                                AND DATEADD(MINUTE, -60, GETUTCDATE())
                            AND [meta_json_1hr] IS NULL";
                    }
                    else if (timeframe == MetaTimeframe.SixHours) {
                        query = @"
                            SELECT [id], [mint]
                            FROM [dbo].[mint]
                            WHERE [inserted_utc] BETWEEN
                                DATEADD(HOUR, -12, GETUTCDATE())
                                AND DATEADD(HOUR, -6, GETUTCDATE())
                            AND [meta_json_6hr] IS NULL";
                    }
                    else if (timeframe == MetaTimeframe.TwelveHours) {
                        query = @"
                            SELECT [id], [mint]
                            FROM [dbo].[mint]
                            WHERE [inserted_utc] BETWEEN
                                DATEADD(HOUR, -24, GETUTCDATE())
                                AND DATEADD(HOUR, -12, GETUTCDATE())
                            AND [meta_json_12hr] IS NULL";
                    }
                    else if (timeframe == MetaTimeframe.OneDay) {
                        query = @"
                            SELECT [id], [mint]
                            FROM [dbo].[mint]
                            WHERE [inserted_utc] BETWEEN
                                DATEADD(HOUR, -48, GETUTCDATE())
                                AND DATEADD(DAY, -24, GETUTCDATE())
                            AND [meta_json_1day] IS NULL";
                    }
                    else {
                        throw new ArgumentException("Invalid timeframe");
                    }

                    using (var command = new SqlCommand(query, connection)) {
                        using (var reader = await command.ExecuteReaderAsync()) {
                            while (await reader.ReadAsync()) {
                                rowsToUpdate.Add((reader.GetInt32(0), reader.GetString(1)));
                            }
                        }
                    }
                }

                if (rowsToUpdate.Count == 0) {
                    Console.Write(".");
                    return;
                }

                await Task.WhenAll(Parallel.ForEachAsync(rowsToUpdate,
                    new ParallelOptions { MaxDegreeOfParallelism = 2 },
                    async (row, token) => {
                        await ProcessRowAsync(row.rowId, row.mint, timeframe);
                    }));

                Console.WriteLine($"Processing completed for {metaType} meta.");
            }
            catch (Exception ex) {
                Console.WriteLine($"Error during polling for {metaType} meta: {ex.Message}");
            }
        }

        private static async Task ProcessRowAsync(int rowId, string mint, MetaTimeframe timeframe) {
            try {
                var responseContent = await FetchMetaWithRetryAsync(mint);

                if (!string.IsNullOrEmpty(responseContent)) {
                    await UpdateRowAsync(rowId, responseContent, timeframe);
                    Console.WriteLine($"Successfully updated row {rowId} for {timeframe} meta");
                }
            }
            catch (Exception ex) {
                Console.WriteLine($"Failed to process row {rowId}: {ex.Message}");
            }
        }

        private static async Task<string?> FetchMetaWithRetryAsync(string mint) {
            for (int attempt = 1; attempt <= _maxRetries; attempt++) {
                try {
                    return await FetchMetaFromApiAsync(mint);
                }
                catch (HttpRequestException ex) when (attempt < _maxRetries) {
                    Console.WriteLine($"Attempt {attempt} failed: {ex.Message}. Retrying...");
                    await Task.Delay(_retryDelayMilliseconds);
                }
                catch (Exception ex) {
                    Console.WriteLine($"Error fetching metadata: {ex.Message}");
                    return null;
                }
            }
            Console.WriteLine("Max retry attempts reached. Giving up.");
            return null;
        }

        private static async Task<string?> FetchMetaFromApiAsync(string mint) {
            string urlWithParams = $"{_solScanApiUrl}?address={mint}";
            var request = new HttpRequestMessage(HttpMethod.Get, urlWithParams);
            request.Headers.Add("token", _apiKey);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            var response = await _client.SendAsync(request);
            response.EnsureSuccessStatusCode();

            if (response.Content != null) {
                return await response.Content.ReadAsStringAsync();
            }
            else {
                throw new HttpRequestException("The response content is null.");
            }
        }

        private static async Task UpdateRowAsync(int rowId, string metaJson, MetaTimeframe timeframe) {
            using (var connection = new SqlConnection(_connectionString)) {
                await connection.OpenAsync();
                string query;
                if (timeframe == MetaTimeframe.OneHour) {
                    query = @"
                        UPDATE [dbo].[mint]
                        SET [meta_json_1hr] = @MetaJson,
                            [fetched_utc_1hr] = @FetchedUtc
                        WHERE [id] = @Id";
                }
                else if (timeframe == MetaTimeframe.SixHours) {
                    query = @"
                        UPDATE [dbo].[mint]
                        SET [meta_json_6hr] = @MetaJson,
                            [fetched_utc_6hr] = @FetchedUtc
                        WHERE [id] = @Id";
                }
                else if (timeframe == MetaTimeframe.TwelveHours) {
                    query = @"
                        UPDATE [dbo].[mint]
                        SET [meta_json_12hr] = @MetaJson,
                            [fetched_utc_12hr] = @FetchedUtc
                        WHERE [id] = @Id";
                }
                else if (timeframe == MetaTimeframe.OneDay) {
                    query = @"
                        UPDATE [dbo].[mint]
                        SET [meta_json_1day] = @MetaJson,
                            [fetched_utc_1day] = @FetchedUtc
                        WHERE [id] = @Id";
                }
                else {
                    throw new ArgumentException("Invalid timeframe");
                }

                using (var command = new SqlCommand(query, connection)) {
                    command.Parameters.AddWithValue("@MetaJson", metaJson);
                    command.Parameters.AddWithValue("@FetchedUtc", DateTime.UtcNow);
                    command.Parameters.AddWithValue("@Id", rowId);

                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        static void LogEvent(string message, bool isDebug = false, bool isStats = false) {
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            var logLevel = isDebug ? "DEBUG" : (isStats ? "STATS" : "INFO");
            Console.WriteLine($"[{timestamp}] [{logLevel}] {message}");
        }

        static void LogError(string message) {
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            Console.WriteLine($"[{timestamp}] [ERROR] {message}");
        }
    }
}
