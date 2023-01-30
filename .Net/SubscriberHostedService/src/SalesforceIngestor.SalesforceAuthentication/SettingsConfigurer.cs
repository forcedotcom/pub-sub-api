using Microsoft.Extensions.Options;

namespace SalesforceIngestor.SalesforceAuthentication;

internal class SettingsConfigurer : IValidateOptions<Settings>
{
    public ValidateOptionsResult Validate(string? name, Settings options)
    {
        var validator = new SettingsValidator();
        var res = validator.Validate(options);

        //TODO: format the error messages better if needed
        return res.IsValid
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(res.Errors.Select(x => x.ErrorMessage).ToList());

    }
}