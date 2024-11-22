using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;  // Assuming you used System.Data.SqlClient from NuGet
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace meta_getter {
    class Program {
        private static readonly string _connectionString = ConfigurationManager.ConnectionStrings["pf.Properties.Settings.ethosConnectionString"].ConnectionString;
        private static readonly HttpClient _client = new HttpClient();
        private static readonly int _pollIntervalSeconds = 5;  // Poll interval in seconds
        private static readonly string _solScanApiUrl = "https://pro-api.solscan.io/v2.0/token/meta";
        private static readonly string _apiKey = ConfigurationManager.AppSettings["SolScanApiKey"];

        static async Task Main(string[] args) {
            Console.WriteLine("Starting polling...");

            while (true) {
                try {
                    await PollAndProcessAsync();
                }
                catch (Exception ex) {
                    Console.WriteLine($"Error occurred: {ex.Message}");
                }

                // Wait for the poll interval before polling again
                await Task.Delay(_pollIntervalSeconds * 1000);
            }
        }
        private static async Task PollAndProcessAsync() {
            List<(int rowId, string mint)> rowsToUpdate = new List<(int, string)>();

            try {
                // Log start of polling
                LogEvent("Starting polling operation...");

                using (var connection = new SqlConnection(_connectionString)) {
                    await connection.OpenAsync();

                    // Query rows inserted 1-2hrs ago
                    string query = @"
                SELECT [id], [mint]
                FROM [dbo].[mint]
                WHERE [inserted_utc] BETWEEN DATEADD(MINUTE, -120, GETUTCDATE()) AND DATEADD(MINUTE, -60, GETUTCDATE())
                AND [meta_json_1hr] IS NULL";

                    using (var command = new SqlCommand(query, connection)) {
                        using (var reader = await command.ExecuteReaderAsync()) {
                            while (await reader.ReadAsync()) {
                                rowsToUpdate.Add((reader.GetInt32(0), reader.GetString(1)));
                            }
                        }
                    }
                }

                // Log the number of rows found
                LogEvent($"Polling completed. Found {rowsToUpdate.Count} rows to update.");

                if (rowsToUpdate.Count == 0) {
                    LogEvent("No rows found for updating. Polling complete.");
                    return;
                }

                // Process each row concurrently
                await Task.WhenAll(Parallel.ForEachAsync(rowsToUpdate, new ParallelOptions { MaxDegreeOfParallelism = 8 }, async (row, token) => {
                    await ProcessRowAsync(row.rowId, row.mint);
                }));

                // Log after processing rows
                LogEvent("Processing of rows completed successfully.");
            }
            catch (Exception ex) {
                LogError($"Error occurred during polling and processing: {ex.Message}");
            }
        }

        private static async Task ProcessRowAsync(int rowId, string mint) {
            try {
                var responseContent = await FetchMetaFromApiAsync(mint);

                if (!string.IsNullOrEmpty(responseContent)) {
                    // Update the row in the database with the API response
                    await UpdateRowAsync(rowId, responseContent);
                    Console.WriteLine($"Successfully updated row {rowId}");
                }
            }
            catch (Exception ex) {
                Console.WriteLine($"Failed to process row {rowId}: {ex.Message}");
            }
        }

        private static async Task<string?> FetchMetaFromApiAsync(string mint) {
            try {
                // Add query parameter "address" to the URL
                string address = "FUGCJuUmbRQionrJxEHiEp5Dw8a7hcQQc4dwPNXwpump";
                string urlWithParams = $"{_solScanApiUrl}?address={mint}";

                var client = new HttpClient();
                var request = new HttpRequestMessage(HttpMethod.Get, urlWithParams);
                request.Headers.Add("token", _apiKey);

                client.DefaultRequestHeaders
                      .Accept
                      .Add(new MediaTypeWithQualityHeaderValue("application/json"));

                var content = new StringContent(string.Empty);
                content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                request.Content = content;

                // Send the request and ensure the status code is successful
                var response = await client.SendAsync(request);
                response.EnsureSuccessStatusCode();

                // Read and return the response content
                return await response.Content.ReadAsStringAsync();
            }
            catch (Exception ex) {
                Console.WriteLine($"Error fetching metadata: {ex.Message}");
                return null;
            }
        }

        private static async Task UpdateRowAsync(int rowId, string metaJson) {
            using (var connection = new SqlConnection(_connectionString)) {
                await connection.OpenAsync();

                string query = @"
                    UPDATE [dbo].[mint]
                    SET [meta_json_1hr] = @MetaJson
                    WHERE [id] = @Id";

                using (var command = new SqlCommand(query, connection)) {
                    command.Parameters.AddWithValue("@MetaJson", metaJson);
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
