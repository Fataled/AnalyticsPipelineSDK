using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;

namespace AnalyticsPipeline;

public class AnalyticsClient
{
    private readonly HttpClient _httpClient;
    public AnalyticsClient(string baseUrl)
    {
        _httpClient = new HttpClient { BaseAddress = new Uri(baseUrl) };
    }

    public async Task<JsonElement> IngestEvent(string eventName, string userId, DateTime? timestamp = null, Dictionary<string, object>? properties = null)
    {
        var actualTimestamp = timestamp ?? DateTime.UtcNow;
        
        properties ??= new Dictionary<string, object>();

        var response = await _httpClient.PostAsJsonAsync("/events", new
        {
            event_name = eventName,
            user_id = userId,
            timestamp =  actualTimestamp.ToString("O"),
            properties
        });
        
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<JsonElement>();
    }
}