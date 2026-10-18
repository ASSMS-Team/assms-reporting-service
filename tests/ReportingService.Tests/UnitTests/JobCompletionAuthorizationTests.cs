using Microsoft.AspNetCore.Authorization;
using ReportingService.Controllers;
using ReportingService.Security;

namespace ReportingService.Tests;

public class JobCompletionAuthorizationTests
{
    [Fact]
    public void JobCompletions_RequiresManagerRole()
    {
        var action = typeof(ReportsController).GetMethod(nameof(ReportsController.GetJobCompletions))!;
        var authorize = Assert.Single(action.GetCustomAttributes(typeof(AuthorizeAttribute), inherit: true)
            .Cast<AuthorizeAttribute>());

        Assert.Equal(StaffRoles.ReportViewers, authorize.Roles);
    }
}
