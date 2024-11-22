using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Net.Http.Headers;
using Websocket.Client;
using System.Threading.Tasks;
using System.Text.Json;

namespace pf_scraper {
    class Program {
        static async Task Main(string[] args) {
            string connectionString = ConfigurationManager.ConnectionStrings["pf.Properties.Settings.ethosConnectionString"].ConnectionString;
            var url = new Uri("wss://pumpportal.fun/api/data");
            using (var client = new WebsocketClient(url)) {

                // Configure reconnection
                client.ReconnectTimeout = TimeSpan.FromSeconds(30);
                client.ReconnectionHappened.Subscribe(info => {
                    LogEvent($"Reconnection happened, type: {info.Type}");
                    // Resubscribe after reconnection
                    client.Send("{\"method\":\"subscribeNewToken\"}");
                });

                // Handle disconnection
                client.DisconnectionHappened.Subscribe(info => {
                    LogEvent($"Disconnection happened, type: {info.Type}");
                });

                // Message handler with detailed logging
                client.MessageReceived.Subscribe(msg =>
                {
                    if (msg.Text.Contains("Successfully subscribed")) {
                        LogEvent("Subscription confirmed");
                        return;
                    }

                    try {
                        var tokenData = JsonSerializer.Deserialize<dynamic>(msg.Text);
                        LogEvent($"Received token: {tokenData.GetProperty("symbol")}");

                        SaveToDatabase(connectionString, msg.Text);
                        LogEvent($"Saved token {tokenData.GetProperty("symbol")} to database");
                    }
                    catch (JsonException jex) {
                        LogError($"Failed to parse message as JSON: {jex.Message}");
                    }
                    catch (Exception ex) {
                        LogError($"Failed to save message to database: {ex.Message}");
                    }
                });

                // Start the client with error handling
                try {
                    await client.Start();
                    LogEvent("WebSocket client started successfully");

                    // Initial subscription
                    client.Send("{\"method\":\"subscribeNewToken\"}");
                    LogEvent("Sent subscription request");
                }
                catch (Exception ex) {
                    LogError($"Failed to start WebSocket client: {ex.Message}");
                    return;
                }

                // Keep the connection alive with periodic ping
                var pingTimer = new System.Threading.Timer((_) => {
                    if (client.IsRunning) {
                        try {
                            client.Send("ping");
                            LogEvent("Ping sent", isDebug: true);
                        }
                        catch (Exception ex) {
                            LogError($"Failed to send ping: {ex.Message}");
                        }
                    }
                }, null, TimeSpan.Zero, TimeSpan.FromSeconds(30));

                Console.WriteLine("Press Enter to exit...");
                Console.ReadLine();

                // Cleanup
                await pingTimer.DisposeAsync();
                await client.Stop(System.Net.WebSockets.WebSocketCloseStatus.NormalClosure, "Application shutting down");
            }
        }

        static void SaveToDatabase(string connectionString, string data) {
            using (var connection = new SqlConnection(connectionString)) {
                connection.Open();
                string query = "INSERT INTO mint (mint_json, inserted_utc) VALUES (@Data, @CreatedAt)";
                using (var command = new SqlCommand(query, connection)) {
                    command.Parameters.AddWithValue("@Data", data);
                    command.Parameters.AddWithValue("@CreatedAt", DateTime.Now);
                    command.ExecuteNonQuery();
                }
            }
        }

        static void LogEvent(string message, bool isDebug = false) {
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            var logLevel = isDebug ? "DEBUG" : "INFO";
            Console.WriteLine($"[{timestamp}] [{logLevel}] {message}");
        }

        static void LogError(string message) {
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            Console.WriteLine($"[{timestamp}] [ERROR] {message}");
        }
    }
}