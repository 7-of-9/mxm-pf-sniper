using System;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Text.RegularExpressions;

public class TokenMetadata
{
    public string Description { get; set; }
    public string Twitter { get; set; }
    public string Telegram { get; set; }
    public string Website { get; set; }
}

public static class TokenMetadataParser
{
    private static readonly HttpClient client = new HttpClient() 
    { 
        Timeout = TimeSpan.FromSeconds(10) 
    };

    public static async Task<TokenMetadata> ParseMetadataFromUri(string uri)
    {
        try
        {
            // Check if URI is valid
            if (string.IsNullOrWhiteSpace(uri) || !Uri.IsWellFormedUriString(uri, UriKind.Absolute))
            {
                return new TokenMetadata();
            }

            // Fetch JSON
            string jsonContent;
            try
            {
                jsonContent = await client.GetStringAsync(uri);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching URI {uri}: {ex.Message}");
                return new TokenMetadata();
            }

            // Parse JSON
            dynamic jsonObject;
            try
            {
                jsonObject = JsonConvert.DeserializeObject<dynamic>(jsonContent);
                if (jsonObject == null)
                {
                    return new TokenMetadata();
                }
            }
            catch (JsonException ex)
            {
                Console.WriteLine($"Error parsing JSON from {uri}: {ex.Message}");
                return new TokenMetadata();
            }

            // Extract fields safely
            return new TokenMetadata
            {
                Description = SafeGetString(jsonObject, "description"),
                Twitter = SafeGetValidUrl(jsonObject, "twitter"),
                Telegram = SafeGetValidUrl(jsonObject, "telegram"),
                Website = SafeGetValidUrl(jsonObject, "website")
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Unexpected error processing {uri}: {ex.Message}");
            return new TokenMetadata();
        }
    }

    private static string SafeGetString(dynamic json, string property)
    {
        try
        {
            string value = json[property]?.ToString();
            return !string.IsNullOrWhiteSpace(value) ? value.Trim() : null;
        }
        catch
        {
            return null;
        }
    }

    private static string SafeGetValidUrl(dynamic json, string property)
    {
        try
        {
            string url = json[property]?.ToString();
            if (string.IsNullOrWhiteSpace(url)) return null;

            url = url.Trim();

            // Basic URL validation
            if (!Uri.IsWellFormedUriString(url, UriKind.Absolute)) return null;

            return url;
        }
        catch
        {
            return null;
        }
    }
}
