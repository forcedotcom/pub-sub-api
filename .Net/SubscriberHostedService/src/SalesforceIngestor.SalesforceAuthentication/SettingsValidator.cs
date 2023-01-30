using FluentValidation;

namespace SalesforceIngestor.SalesforceAuthentication;

internal class SettingsValidator : AbstractValidator<Settings>
{
    public SettingsValidator()
    {
        RuleFor(x => x.PrivateKey).NotEmpty();
        RuleFor(x => x.ClientId).NotEmpty();
        RuleFor(x => x.Audience).NotEmpty();
        RuleFor(x => x.UserName).NotEmpty();
        RuleFor(x => x.LoginUri).NotEmpty();

        When(x => x.PrivateKeyIsPasswordProtected, () =>
        {
            RuleFor(x => x.PrivateKeyPassword).NotEmpty();
        });
    }
}