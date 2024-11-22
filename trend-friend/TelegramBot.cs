using System.Text.Json;
using Telegram.Bot;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

public class TelegramBot {
    private readonly ITelegramBotClient _botClient;
    private HashSet<long> _subscribedChats;
    private const string STORAGE_FILE = "subscribers.json";

    public TelegramBot(string token) {
        _botClient = new TelegramBotClient(token);
        _subscribedChats = LoadSubscribers();
        this.BroadcastMessageAsync("Bot starting...");
    }

    public async Task StartAsync() {
        var me = await _botClient.GetMeAsync();
        Console.WriteLine($"Bot started: @{me.Username}");

        int offset = 0;
        while (true) {
            var updates = await _botClient.GetUpdatesAsync(offset);

            foreach (var update in updates) {
                offset = update.Id + 1;

                if (update.Type == UpdateType.Message) {
                    var message = update.Message;
                    await HandleMessageAsync(message);
                }
            }
        }
    }

    private HashSet<long> LoadSubscribers() {
        if (System.IO.File.Exists(STORAGE_FILE)) {
            string json = System.IO.File.ReadAllText(STORAGE_FILE);
            return JsonSerializer.Deserialize<HashSet<long>>(json) ?? new HashSet<long>();
        }
        return new HashSet<long>();
    }

    private void SaveSubscribers() {
        string json = JsonSerializer.Serialize(_subscribedChats);
        System.IO.File.WriteAllText(STORAGE_FILE, json);
    }

    private async Task HandleMessageAsync(Message message) {
        if (message.Text == "/start") {
            _subscribedChats.Add(message.Chat.Id);
            SaveSubscribers();
            await _botClient.SendTextMessageAsync(
                chatId: message.Chat.Id,
                text: "Subscribed successfully!"
            );
        }
    }

    // Broadcast method
    public async Task BroadcastMessageAsync(string message) {
        foreach (var chatId in _subscribedChats.ToList()) {
            try {
                await _botClient.SendTextMessageAsync(chatId, message);
            }
            catch {
                _subscribedChats.Remove(chatId);
                SaveSubscribers();
            }
        }
    }
}