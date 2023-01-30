using FluentValidation;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace SalesforceIngestor.App.Config;

public class SalesforcePubSubApiSettings
{
    public Uri ApiUri { get; set; }
    
}

internal class SalesforcePubSubApiSettingsConfigurer : IValidateOptions<SalesforcePubSubApiSettings>, IConfigureOptions<SalesforcePubSubApiSettings>
{
    private readonly IConfiguration _config;

    public SalesforcePubSubApiSettingsConfigurer(IConfiguration config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }
    
    public ValidateOptionsResult Validate(string? name, SalesforcePubSubApiSettings options)
    {
        var validator = new SalesforcePubSubApiSettingsValidator();
        var res = validator.Validate(options);

        //TODO: format the error messages better if needed
        return res.IsValid
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(res.Errors.Select(x => x.ErrorMessage).ToList());
    }

    public void Configure(SalesforcePubSubApiSettings options)
    {
        _config.GetSection("salesforcePubSubApiSettings").Bind(options);
    }
}

internal class SalesforcePubSubApiSettingsValidator : AbstractValidator<SalesforcePubSubApiSettings>
{
    public SalesforcePubSubApiSettingsValidator()
    {
        RuleFor(x => x.ApiUri).NotEmpty();
    }
}

