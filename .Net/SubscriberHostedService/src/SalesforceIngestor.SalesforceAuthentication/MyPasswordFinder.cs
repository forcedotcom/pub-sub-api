using Org.BouncyCastle.OpenSsl;

namespace SalesforceIngestor.SalesforceAuthentication;
internal class MyPasswordFinder : IPasswordFinder
{
    private readonly string _password;

    public MyPasswordFinder(string password)
    {
        if (string.IsNullOrWhiteSpace(password))
            throw new ArgumentNullException(nameof(password), "can not be null/whitespace/empty");

        _password = password;
    }

    public char[] GetPassword()
    {
        return _password.ToCharArray();
    }
}