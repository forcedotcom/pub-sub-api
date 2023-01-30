using FluentValidation;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace SalesforceIngestor.App.Config;

/// <summary>
/// settings to control fetching and retrying
/// </summary>
public class SubscriptionSettings
{
    /// <summary>
    /// The max is 100.  Any number over 100 is treated as 100.  Cannot set lower than 1
    /// </summary>
    public int FetchRequestCount { get; set; } = 50;

    /// <summary>
    /// I've found the API will typically return ever 60 seconds when not events have occurred. The documentation states
    /// it should return every 270 seconds.  Set it greater than what SF returns at.  
    /// greater than 60 
    /// </summary>
    public TimeSpan FetchResponseTimeout { get; set; } = TimeSpan.FromSeconds(90); 
    /// <summary>
    /// Amount of time to wait between retrys
    /// </summary>
    public TimeSpan SubscriptionRetryWait { get; set; } = TimeSpan.FromSeconds(5);
    /// <summary>
    /// Indicates if the attempts should be use to backoff the retry
    /// </summary>
    public bool UseAttemptMultiplier { get; set; } = true;
}

internal class SubscriptionSettingsConfigurer : IValidateOptions<SubscriptionSettings>, IConfigureOptions<SubscriptionSettings>
{
    private readonly IConfiguration _config;

    public SubscriptionSettingsConfigurer(IConfiguration config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }
    
    public ValidateOptionsResult Validate(string? name, SubscriptionSettings options)
    {
        var validator = new SubscriptionSettingsValidator();
        var res = validator.Validate(options);

        //TODO: format the error messages better if needed
        return res.IsValid
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(res.Errors.Select(x => x.ErrorMessage).ToList());
    }

    public void Configure(SubscriptionSettings options)
    {
        _config.GetSection("subscriptionSettings").Bind(options);
    }
}

internal class SubscriptionSettingsValidator : AbstractValidator<SubscriptionSettings>
{
    public SubscriptionSettingsValidator()
    {
        RuleFor(x => x.FetchRequestCount).GreaterThan(0);
        RuleFor(x => x.SubscriptionRetryWait).GreaterThan(TimeSpan.Zero);
        RuleFor(x => x.FetchResponseTimeout).GreaterThan(TimeSpan.Zero);
    }
}