using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.OpenSsl;
using Org.BouncyCastle.Security;

namespace SalesforceIngestor.SalesforceAuthentication;

public interface ISalesforceTokenClient
{
    Task<SalesforceTokenResponse> GetTokenResponseAsync(bool refresh = false);
}

internal class SalesforceTokenClient : ISalesforceTokenClient
{
    private static string SALESFORCE_TOKENRESPONSE_KEY = $"{typeof(SalesforceTokenClient).FullName.ToLower()}.salesforcetokenresponse";
    private readonly HttpClient _client;
    private readonly IMemoryCache _memoryCache;
    private readonly Settings _settings;
    private static readonly SemaphoreSlim Locker = new SemaphoreSlim(1);

    public SalesforceTokenClient(HttpClient client, IOptions<Settings> settings, IMemoryCache memoryCache)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _memoryCache = memoryCache ?? throw new ArgumentNullException(nameof(memoryCache));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
    }

    public async Task<SalesforceTokenResponse> GetTokenResponseAsync(bool refresh = false)
    {
        Lazy<Task<SalesforceTokenResponse>>? cacheItem;
        
        try
        {
            await Locker.WaitAsync().ConfigureAwait(false);

            if (refresh)
            {
                _memoryCache.Remove(SALESFORCE_TOKENRESPONSE_KEY);
            }

            cacheItem = _memoryCache.GetOrCreate(SALESFORCE_TOKENRESPONSE_KEY, entry =>
            {
                entry.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(_settings.TokenCachingInMinutes);
                return new Lazy<Task<SalesforceTokenResponse>>(
                    () => Task.Factory.StartNew(GetSalesforceTokenResponseAsyncImpl).Unwrap(),
                    LazyThreadSafetyMode.ExecutionAndPublication);
            });
        }
        finally
        {
            Locker.Release();
        }

        return await cacheItem!.Value.ConfigureAwait(false);

    }

    private async Task<SalesforceTokenResponse> GetSalesforceTokenResponseAsyncImpl()
    {
        var grant_type = "urn:ietf:params:oauth:grant-type:jwt-bearer";
        var assertion = GetAssertion();


        using (var req = new HttpRequestMessage(HttpMethod.Post, "services/oauth2/token"))
        {
            req.Content = new FormUrlEncodedContent(new List<KeyValuePair<string, string>>()
            {
                new(nameof(grant_type), grant_type),
                new(nameof(assertion), assertion)
            });

            using (var resp = await _client.SendAsync(req).ConfigureAwait(false))
            {
                var raw = await resp.Content.ReadAsStringAsync().ConfigureAwait(false);
                return JsonSerializer.Deserialize<SalesforceTokenResponse>(raw) ??
                       throw new InvalidOperationException(
                           "Could not deserialize Salesforce Authentication response");
            }
        }
    }

    private string GetAssertion()
    {
        var keyContents = _settings.PrivateKeyIsBase64Encoded
            ? Encoding.UTF8.GetString(Convert.FromBase64String(_settings.PrivateKey))
            : _settings.PrivateKey;

        var finder = _settings.PrivateKeyIsPasswordProtected
            ? new MyPasswordFinder(_settings.PrivateKeyPassword)
            : null;
        AsymmetricCipherKeyPair? pair;

        using (var sReader = new StringReader(keyContents))
        {
            pair = (new PemReader(sReader, finder).ReadObject()) as AsymmetricCipherKeyPair;
        }

        if (pair == null)
            throw new InvalidOperationException("could not get private key");

        var cryptoServiceProvider = new RSACryptoServiceProvider();
        var rsaParameters = DotNetUtilities.ToRSAParameters((RsaPrivateCrtKeyParameters)pair.Private);
        cryptoServiceProvider.ImportParameters(rsaParameters);
        var rsaSecurityKey = new RsaSecurityKey(cryptoServiceProvider);

        var tokenDescriptor = new SecurityTokenDescriptor()
        {
            Expires = DateTime.UtcNow.AddMinutes(10),
            Subject = new ClaimsIdentity(new List<Claim>() { new("sub", _settings.UserName) }),
            Audience = _settings.Audience,
            Issuer = _settings.ClientId,
            SigningCredentials =
                new SigningCredentials(rsaSecurityKey, "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256")
        };

        var securityTokenHandler = new JwtSecurityTokenHandler();
        var token = securityTokenHandler.CreateToken(tokenDescriptor);
        return securityTokenHandler.WriteToken(token);
    }
}