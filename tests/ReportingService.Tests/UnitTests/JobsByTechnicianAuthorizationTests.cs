using Microsoft.AspNetCore.Authorization;

using ReportingService.Controllers;
using ReportingService.Security;

namespace ReportingService.Tests;

public class JobsByTechnicianAuthorizationTests
{
    [Fact]
    public void JobsByTechnician_RequiresManagerRole()
    {
        var action = typeof(ReportsController).GetMethod(nameof(ReportsController.GetJobsByTechnician))!;
        var authorize = Assert.Single(action.GetCustomAttributes(typeof(AuthorizeAttribute), inherit: true)
            .Cast<AuthorizeAttribute>());

        Assert.Equal(StaffRoles.ReportViewers, authorize.Roles);
    }
}
