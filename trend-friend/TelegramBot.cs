using System.Text.Json;
using Telegram.Bot;
using Telegram.Bot.Exceptions;
using Telegram.Bot.Polling;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

public class TelegramBot {
    private readonly ITelegramBotClient _botClient;
    private HashSet<long> _subscribedChats;
    private const string STORAGE_FILE = "subscribers.json";
    private CancellationTokenSource _cts;

    public List<string> Currents = new List<string>();

    public TelegramBot(string token) {
        _botClient = new TelegramBotClient(token);
        _subscribedChats = LoadSubscribers();
        _cts = new CancellationTokenSource();
        this.BroadcastMessageAsync("Bot starting...").Wait();
    }

    public async Task StartAsync() {
        try {
            var me = await _botClient.GetMeAsync();
            Console.WriteLine($"Bot started: @{me.Username}");

            int offset = 0;
            while (!_cts.Token.IsCancellationRequested) {
                try {
                    var updates = await _botClient.GetUpdatesAsync(offset);
                    foreach (var update in updates) {
                        offset = update.Id + 1;
                        if (update.Message is not { } message)
                            continue;

                        Console.WriteLine($"Received message: {message.Text} from {message.Chat.Id}");

                        switch (message.Text?.ToLower()) {
                            case "/start":
                                _subscribedChats.Add(message.Chat.Id);
                                SaveSubscribers();
                                await _botClient.SendTextMessageAsync(
                                    chatId: message.Chat.Id,
                                    text: "GM!\n\n" +
                                    "This bot tracks young PF coins trending by MC, and runs Google Trends searches for new entries." +
                                    "\n" +
                                    "\nAvailable commands:\n/current - Show all ranked current trending\n/help - Show available commands",
                                    parseMode: ParseMode.Markdown);
                                break;

                            case "/current":
                                foreach (string s in Currents) {
                                    await _botClient.SendTextMessageAsync(
                                        chatId: message.Chat.Id,
                                        text: s,
                                        parseMode: ParseMode.Markdown);
                                }
                                break;

                            default:
                                await _botClient.SendTextMessageAsync(
                                    chatId: message.Chat.Id,
                                    text: "Available commands:\n" +
                                          "/current - Show current trending\n" +
                                          "/help - Show this help message",
                                    parseMode: ParseMode.Markdown);
                                break;
                        }
                    }

                    await Task.Delay(1000, _cts.Token); // Add a small delay between polls
                }
                catch (TaskCanceledException) {
                    break;
                }
                catch (Exception ex) {
                    Console.WriteLine($"Error polling updates: {ex.Message}");
                    await Task.Delay(5000, _cts.Token); // Longer delay on error
                }
            }
        }
        catch (Exception ex) {
            Console.WriteLine($"Error starting bot: {ex.Message}");
            throw;
        }
    }

    private Task HandlePollingErrorAsync(ITelegramBotClient botClient, Exception exception, CancellationToken cancellationToken) {
        var errorMessage = exception switch {
            ApiRequestException apiRequestException
                => $"Telegram API Error:\n[{apiRequestException.ErrorCode}]\n{apiRequestException.Message}",
            _ => exception.ToString()
        };

        Console.WriteLine(errorMessage);
        return Task.CompletedTask;
    }

    private HashSet<long> LoadSubscribers() {
        try {
            if (System.IO.File.Exists(STORAGE_FILE)) {
                string json = System.IO.File.ReadAllText(STORAGE_FILE);
                return JsonSerializer.Deserialize<HashSet<long>>(json) ?? new HashSet<long>();
            }
        }
        catch (Exception ex) {
            Console.WriteLine($"Error loading subscribers: {ex.Message}");
        }
        return new HashSet<long>();
    }

    private void SaveSubscribers() {
        try {
            string json = JsonSerializer.Serialize(_subscribedChats);
            System.IO.File.WriteAllText(STORAGE_FILE, json);
        }
        catch (Exception ex) {
            Console.WriteLine($"Error saving subscribers: {ex.Message}");
        }
    }

    public async Task BroadcastMessageAsync(string message) {
        foreach (var chatId in _subscribedChats.ToList()) {
            try {
                await _botClient.SendTextMessageAsync(
                    chatId: chatId,
                    text: message,
                    parseMode: ParseMode.Markdown,
                    cancellationToken: _cts.Token);
            }
            catch (Exception ex) {
                Console.WriteLine($"Failed to broadcast to {chatId}: {ex.Message}");
                _subscribedChats.Remove(chatId);
                SaveSubscribers();
            }
        }
    }

    public async Task SendPhotoAsync(InputFile photo, string caption = null) {
        foreach (var chatId in _subscribedChats.ToList()) {
            try {
                await _botClient.SendPhotoAsync(
                    chatId: chatId,
                    photo: photo,
                    caption: caption,
                    parseMode: ParseMode.Markdown,
                    cancellationToken: _cts.Token
                );
            }
            catch (Exception ex) {
                Console.WriteLine($"Failed to send photo to {chatId}: {ex.Message}");
                _subscribedChats.Remove(chatId);
                SaveSubscribers();
            }
        }
    }

    public void Stop() {
        _cts.Cancel();
    }
}