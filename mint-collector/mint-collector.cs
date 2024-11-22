using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Net.Http.Headers;
using Websocket.Client;
using System.Threading.Tasks;
using System.Text.Json;
using System.Threading;

namespace pf_scraper {
    class Program {
        private static WebsocketClient _client;
        private static readonly Uri _wsUrl = new Uri("wss://pumpportal.fun/api/data");
        private static string _connectionString;
        private static System.Threading.Timer _pingTimer;
        private static System.Threading.Timer _statsTimer;
        private static readonly object _reconnectLock = new object();
        private static bool _isReconnecting = false;
        private static readonly Random _random = new Random();

        // Stats tracking
        private static readonly DateTime _startTime = DateTime.Now;
        private static int _tokenCount = 0;
        private static readonly object _statsLock = new object();

        static async Task Main(string[] args) {
            _connectionString = ConfigurationManager.ConnectionStrings["pf.Properties.Settings.ethosConnectionString"].ConnectionString;

            await InitializeWebsocketClient();
            InitializeStatsTimer();

            Console.WriteLine("Press Enter to exit...");
            Console.ReadLine();

            await CleanupAsync();
        }

        private static async Task InitializeWebsocketClient() {
            _client = new WebsocketClient(_wsUrl) {
                ReconnectTimeout = null  // Disable auto-reconnect to handle it manually
            };

            SetupWebSocketHandlers();
            await ConnectWithRetryAsync();
            InitializePingTimer();
        }

        private static void SetupWebSocketHandlers() {
            _client.DisconnectionHappened.Subscribe(async info => {
                LogEvent($"Disconnection happened, type: {info.Type}");
                // Don't trigger reconnect on normal closure
                if (info.Type != DisconnectionType.Exit && info.Type != DisconnectionType.ByUser) {
                    await HandleDisconnectionAsync();
                }
            });

            _client.ReconnectionHappened.Subscribe(info => {
                LogEvent($"Reconnection happened, type: {info.Type}");
                SubscribeToFeed();
            });

            _client.MessageReceived.Subscribe(HandleMessage);
        }

        private static async Task HandleDisconnectionAsync() {
            if (_isReconnecting) return;

            lock (_reconnectLock) {
                if (_isReconnecting) return;
                _isReconnecting = true;
            }

            int retryCount = 0;
            while (true) {
                try {
                    // Dispose old client and create new one
                    if (_client != null) {
                        await _client.Stop(System.Net.WebSockets.WebSocketCloseStatus.NormalClosure, "Reconnecting");
                        _client.Dispose();
                    }

                    _client = new WebsocketClient(_wsUrl) {
                        ReconnectTimeout = null
                    };
                    SetupWebSocketHandlers();

                    LogEvent("Attempting to reconnect...");
                    await _client.Start();
                    await Task.Delay(1000); // Give it a moment to establish

                    if (_client.IsRunning) {
                        LogEvent("Reconnected successfully");
                        SubscribeToFeed();
                        break;
                    }
                }
                catch (Exception ex) {
                    LogError($"Reconnection attempt failed: {ex.Message}");
                }

                // Exponential backoff with jitter
                int baseDelay = 5000; // 5 seconds
                int maxDelay = 30000; // 30 seconds max

                int delay = Math.Min(baseDelay * (int)Math.Pow(1.5, Math.Min(retryCount, 5)), maxDelay);
                delay += _random.Next(-delay / 4, delay / 4); // Add jitter

                LogEvent($"Waiting {delay / 1000} seconds before next reconnection attempt...");
                await Task.Delay(delay);
                retryCount++;
            }

            lock (_reconnectLock) {
                _isReconnecting = false;
            }
        }

        private static void InitializeStatsTimer() {
            _statsTimer = new System.Threading.Timer((_) => {
                LogStats();
            }, null, TimeSpan.Zero, TimeSpan.FromMinutes(1));
        }

        private static void LogStats() {
            var runningTime = DateTime.Now - _startTime;
            var tokenCount = _tokenCount; // Atomic read

            if (runningTime.TotalMinutes < 1) return; // Skip if less than a minute

            var tokensPerDay = (24 * 60 * tokenCount) / runningTime.TotalMinutes;
            var avgMinutesBetweenTokens = runningTime.TotalMinutes / Math.Max(1, tokenCount);

            LogEvent($"Stats: {tokenCount} tokens in {runningTime.ToString(@"hh\:mm\:ss")}, " +
                     $"avg {tokensPerDay:F1} tokens/day " +
                     $"({avgMinutesBetweenTokens:F1} mins between tokens)", isStats: true);
        }

        private static void HandleMessage(ResponseMessage msg) {
            if (msg.Text.Contains("Successfully subscribed")) {
                LogEvent("Subscription confirmed");
                return;
            }
            if (msg.Text.Contains("Invalid message")) {
                return;
            }

            try {
                var tokenData = JsonSerializer.Deserialize<dynamic>(msg.Text);
                var symbol = tokenData.GetProperty("symbol").ToString();
                LogEvent($"Received token: {symbol}");

                SaveToDatabase(_connectionString, msg.Text);
                LogEvent($"Saved token {symbol} to database");

                // Increment token counter
                Interlocked.Increment(ref _tokenCount);
            }
            catch (JsonException jex) {
                LogError($"Failed to parse message as JSON: {jex.Message}");
            }
            catch (Exception ex) {
                LogError($"Failed to save message to database: {ex.Message}");
            }
        }

        private static async Task ConnectWithRetryAsync() {
            while (true) {
                try {
                    await _client.Start();
                    await Task.Delay(1000); // Give it a moment to establish

                    if (_client.IsRunning) {
                        LogEvent("WebSocket client started successfully");
                        SubscribeToFeed();
                        break;
                    }
                }
                catch (Exception ex) {
                    LogError($"Failed to start WebSocket client: {ex.Message}");
                    await Task.Delay(5000); // Wait 5 seconds before retrying
                }
            }
        }

        private static void SubscribeToFeed() {
            _client.Send("{\"method\":\"subscribeNewToken\"}");
            LogEvent("Sent subscription request");
        }

        private static void InitializePingTimer() {
            _pingTimer = new System.Threading.Timer((_) => {
                if (_client.IsRunning) {
                    try {
                        _client.Send("ping");
                        LogEvent("Ping sent", isDebug: true);
                    }
                    catch (Exception ex) {
                        LogError($"Failed to send ping: {ex.Message}");
                    }
                }
            }, null, TimeSpan.Zero, TimeSpan.FromSeconds(30));
        }

        private static async Task CleanupAsync() {
            if (_statsTimer != null) {
                await _statsTimer.DisposeAsync();
                LogStats(); // Final stats
            }

            if (_pingTimer != null) {
                await _pingTimer.DisposeAsync();
            }

            if (_client != null && _client.IsRunning) {
                await _client.Stop(System.Net.WebSockets.WebSocketCloseStatus.NormalClosure, "Application shutting down");
                _client.Dispose();
            }
        }

        static void SaveToDatabase(string connectionString, string data) {
            using (var connection = new SqlConnection(connectionString)) {
                connection.Open();
                string query = "INSERT INTO mint (mint_json, inserted_utc) VALUES (@Data, @CreatedAt)";
                using (var command = new SqlCommand(query, connection)) {
                    command.Parameters.AddWithValue("@Data", data);
                    command.Parameters.AddWithValue("@CreatedAt", DateTime.UtcNow);
                    command.ExecuteNonQuery();
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