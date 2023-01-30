namespace SalesforceIngestor.SalesforceAuthentication;
public class Settings
{
    public bool PrivateKeyIsPasswordProtected { get; set; }
    public bool PrivateKeyIsBase64Encoded { get; set; }
    public string? PrivateKey { get; set; }
    public string? PrivateKeyPassword { get; set; }
    public string? ClientId { get; set; }
    public string? Audience { get; set; }
    public string? UserName { get; set; }
    public Uri? LoginUri { get; set; }

    public int TokenCachingInMinutes { get; set; } = 60;
}