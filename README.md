[![Build status](https://ci.appveyor.com/api/projects/status/xrkqyg27iuy4g1h7?svg=true)](https://ci.appveyor.com/project/SurajGupta/obeautifulcode-database)

[![Nuget status](https://img.shields.io/nuget/v/OBeautifulCode.Database.Recipes.DatabaseHelper.svg)](https://www.nuget.org/packages/OBeautifulCode.Database.Recipes.DatabaseHelper)  OBeautifulCode.Database.Recipes.DatabaseHelper

Database Helpers
================
This library contains methods that reduce much of the boilerplate ADO.net code required to interact with a database.
The library was built to be agonostic to database provider, it operates over `IDbConnection`, `IDbTransaction`, `IDb...`

For example, you can read a single column of data from a table in SQLServer by doing this:
`Collection<object> result = DbHelper.ReadSingleColumn<SqlConnection>(ConnectionString, "Select [MyCol] From [MyDb]")`

Most methods support two overloads:
- Use the generic overload, as in the example above, when you have a connection string and you want the method to handle building the connection, command, etc.  Transactions are not supported in this mode because it is expected that you are only executing a single statement.
- Use the non-generic overload when you want to use a pre-constructed `IDbConnection`.  In this mode, you can optionally pass an `IDbTransaction`

Transactions can also be used implicitly by wrapping call to this library in a `TransactionScope`