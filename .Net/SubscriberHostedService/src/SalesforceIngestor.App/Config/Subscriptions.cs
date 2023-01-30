using FluentValidation;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace SalesforceIngestor.App.Config;

public class Subscriptions
{
    public List<string> Topics { get; set; }    
}

internal class SubscriptionsConfigurer : IValidateOptions<Subscriptions>, IConfigureOptions<Subscriptions>
{
    private readonly IConfiguration _config;

    public SubscriptionsConfigurer(IConfiguration config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }
    
    public ValidateOptionsResult Validate(string? name, Subscriptions options)
    {
        var validator = new SubscriptionsValidator();
        var res = validator.Validate(options);

        //TODO: format the error messages better if needed
        return res.IsValid
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(res.Errors.Select(x => x.ErrorMessage).ToList());
    }

    public void Configure(Subscriptions options)
    {
        _config.GetSection("subscriptions").Bind(options);
    }
}

internal class SubscriptionsValidator : AbstractValidator<Subscriptions>
{
    public SubscriptionsValidator()
    {
        RuleFor(x => x.Topics).NotEmpty();
        When(x => x.Topics != null && x.Topics.Any(), () =>
        {
            RuleForEach(x => x.Topics).NotEmpty();
        });
    }
}

