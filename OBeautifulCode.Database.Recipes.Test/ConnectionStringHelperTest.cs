// --------------------------------------------------------------------------------------------------------------------
// <copyright file="ConnectionStringHelperTest.cs" company="OBeautifulCode">
//   Copyright (c) OBeautifulCode 2018. All rights reserved.
// </copyright>
// --------------------------------------------------------------------------------------------------------------------

namespace OBeautifulCode.Database.Recipes.Test
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;

    using FakeItEasy;

    using OBeautifulCode.Assertion.Recipes;
    using OBeautifulCode.AutoFakeItEasy;
    using OBeautifulCode.CodeAnalysis.Recipes;

    using Xunit;

    public static class ConnectionStringHelperTest
    {
        [Fact]
        public static void BuildConnectionString___Should_throw_ArgumentNullException___When_parameter_serverName_is_null()
        {
            // Arrange, Act
            var actual = Record.Exception(() => ConnectionStringHelper.BuildConnectionString(null));

            // Assert
            actual.AsTest().Must().BeOfType<ArgumentNullException>();
            actual.Message.AsTest().Must().ContainString("serverName");
        }

        [Fact]
        public static void BuildConnectionString___Should_throw_ArgumentException___When_parameter_serverName_is_white_space()
        {
            // Arrange, Act
            var actual = Record.Exception(() => ConnectionStringHelper.BuildConnectionString(" \r\n "));

            // Assert
            actual.AsTest().Must().BeOfType<ArgumentException>();
            actual.Message.AsTest().Must().ContainString("serverName");
            actual.Message.AsTest().Must().ContainString("white space");
        }

        [Fact]
        [SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly", MessageId = "portis", Justification = ObcSuppressBecause.CA1702_CompoundWordsShouldBeCasedCorrectly_AnalyzerIsIncorrectlyDetectingCompoundWords)]
        public static void BuildConnectionString___Should_throw_ArgumentOutOfRangeException___When_parameter_port_is_malformed()
        {
            // Arrange
            var serverName = A.Dummy<string>();

            var ports = new[]
            {
                int.MinValue,
                A.Dummy<NegativeInteger>(),
                -1,
                0,
                65536,
                int.MaxValue,
            };

            // Act
            var actuals = ports.Select(_ => Record.Exception(() => ConnectionStringHelper.BuildConnectionString(serverName, _)));

            // Assert
            actuals.AsTest().Must().Each().BeOfType<ArgumentOutOfRangeException>();
            actuals.Select(_ => _.Message).AsTest().Must().Each().ContainString("is not a valid port number");
        }

        [Fact]
        public static void BuildConnectionString___Should_build_connection_string___When_credentials_not_specified()
        {
            // Arrange
            var serverName = "myserver.com";
            var port = 414;
            var instanceName = "InstanceName";
            var databaseName = "my-database";
            var connectionTimeoutInSeconds = 12;
            var userName = "my-user-name";
            var clearTextPassword = "my-password";

            var expected = new[]
            {
                "Data Source=myserver.com;Initial Catalog=master;Integrated Security=True",
                "Data Source=myserver.com:414;Initial Catalog=master;Integrated Security=True",
                "Data Source=myserver.com\\InstanceName;Initial Catalog=master;Integrated Security=True",
                "Data Source=myserver.com:414\\InstanceName;Initial Catalog=my-database;Integrated Security=True",
                "Data Source=myserver.com;Initial Catalog=my-database;Integrated Security=False;User ID=my-user-name;Password=my-password;Connect Timeout=12",
            };

            // Act
            var actual = new[]
            {
                ConnectionStringHelper.BuildConnectionString(serverName),
                ConnectionStringHelper.BuildConnectionString(serverName, port),
                ConnectionStringHelper.BuildConnectionString(serverName, instanceName: instanceName),
                ConnectionStringHelper.BuildConnectionString(serverName, port, instanceName, databaseName),
                ConnectionStringHelper.BuildConnectionString(serverName, databaseName: databaseName, userName: userName, clearTextPassword: clearTextPassword, connectionTimeoutInSeconds: connectionTimeoutInSeconds),
            };

            // Assert
            actual.AsTest().Must().BeEqualTo(expected);
        }

        [Fact]
        public static void AddOrUpdateInitialCatalogInConnectionString___Should_throw_ArgumentNullException___When_parameter_connectionString_is_null()
        {
            // Arrange, Act
            var actual = Record.Exception(() => ConnectionStringHelper.AddOrUpdateInitialCatalogInConnectionString(null, A.Dummy<string>()));

            // Assert
            actual.AsTest().Must().BeOfType<ArgumentNullException>();
            actual.Message.AsTest().Must().ContainString("connectionString");
        }

        [Fact]
        public static void AddOrUpdateInitialCatalogInConnectionString___Should_throw_ArgumentException___When_parameter_connectionString_is_white_space()
        {
            // Arrange, Act
            var actual = Record.Exception(() => " \r\n ".AddOrUpdateInitialCatalogInConnectionString(A.Dummy<string>()));

            // Assert
            actual.AsTest().Must().BeOfType<ArgumentException>();
            actual.Message.AsTest().Must().ContainString("connectionString");
            actual.Message.AsTest().Must().ContainString("white space");
        }

        [Fact]
        public static void AddOrUpdateInitialCatalogInConnectionString___Should_throw_ArgumentNullException___When_parameter_databaseName_is_null()
        {
            // Arrange
            var connectionString = A.Dummy<string>();

            // Act
            var actual = Record.Exception(() => connectionString.AddOrUpdateInitialCatalogInConnectionString(null));

            // Assert
            actual.AsTest().Must().BeOfType<ArgumentNullException>();
            actual.Message.AsTest().Must().ContainString("databaseName");
        }

        [Fact]
        public static void AddOrUpdateInitialCatalogInConnectionString___Should_throw_ArgumentException___When_parameter_databaseName_is_white_space()
        {
            // Arrange
            var connectionString = A.Dummy<string>();

            // Act
            var actual = Record.Exception(() => connectionString.AddOrUpdateInitialCatalogInConnectionString(" \r\n "));

            // Assert
            actual.AsTest().Must().BeOfType<ArgumentException>();
            actual.Message.AsTest().Must().ContainString("databaseName");
            actual.Message.AsTest().Must().ContainString("white space");
        }

        [Fact]
        public static void AddOrUpdateInitialCatalogInConnectionString___Should_update_database_in_connection_string___When_database_specified_in_connection_string()
        {
            // Arrange
            var serverName = "myserver.com";
            var port = 414;
            var instanceName = "InstanceName";
            var database = "my-database";
            var connectionTimeoutInSeconds = 12;
            var userName = "my-user-name";
            var clearTextPassword = "my-password";

            var replacementDatabase = "replacement-database";

            var connectionString = ConnectionStringHelper.BuildConnectionString(serverName, port, instanceName, database, userName, clearTextPassword, connectionTimeoutInSeconds);

            var expected = connectionString.Replace(database, replacementDatabase);

            // Act
            var actual = connectionString.AddOrUpdateInitialCatalogInConnectionString(replacementDatabase);

            // Assert
            actual.AsTest().Must().BeEqualTo(expected);
        }

        [Fact]
        public static void AddOrUpdateInitialCatalogInConnectionString___Should_add_database_in_connection_string___When_database_not_specified_in_connection_string()
        {
            // Arrange
            var databaseName = "my-database";

            var connectionString = "Data Source=myserver.com;Integrated Security=False;User ID=my-user-name;Password=my-password;Connect Timeout=12";

            var expected = "Data Source=myserver.com;Initial Catalog=my-database;Integrated Security=False;User ID=my-user-name;Password=my-password;Connect Timeout=12";

            // Act
            var actual = connectionString.AddOrUpdateInitialCatalogInConnectionString(databaseName);

            // Assert
            actual.AsTest().Must().BeEqualTo(expected);
        }

        [Fact]
        public static void ObfuscateCredentialsInConnectionString___Should_throw_ArgumentNullException___When_parameter_connectionString_is_null()
        {
            // Arrange, Act
            var actual = Record.Exception(() => ConnectionStringHelper.ObfuscateCredentialsInConnectionString(null));

            // Assert
            actual.AsTest().Must().BeOfType<ArgumentNullException>();
            actual.Message.AsTest().Must().ContainString("connectionString");
        }

        [Fact]
        public static void ObfuscateCredentialsInConnectionString___Should_throw_ArgumentException___When_parameter_connectionString_is_white_space()
        {
            // Arrange, Act
            var actual = Record.Exception(() => " \r\n ".ObfuscateCredentialsInConnectionString());

            // Assert
            actual.AsTest().Must().BeOfType<ArgumentException>();
            actual.Message.AsTest().Must().ContainString("connectionString");
            actual.Message.AsTest().Must().ContainString("white space");
        }

        [Fact]
        public static void ObfuscateCredentialsInConnectionString___Should_return_same_connection_string___When_connection_string_does_not_contain_credentials()
        {
            // Arrange
            var expected = "Data Source=myserver.com:414\\InstanceName;Initial Catalog=my-database;Integrated Security=True;Connect Timeout=12";

            // Act
            var actual1 = expected.ObfuscateCredentialsInConnectionString(obfuscateUserName: true);
            var actual2 = expected.ObfuscateCredentialsInConnectionString(obfuscateUserName: false);

            // Assert
            actual1.AsTest().Must().BeEqualTo(expected);
            actual2.AsTest().Must().BeEqualTo(expected);
        }

        [Fact]
        public static void ObfuscateCredentialsInConnectionString___Should_obfuscate_credentials___When_connection_string_contains_credentials()
        {
            // Arrange
            var connectionStrings = new[]
            {
                "Data Source=myserver.com;Initial Catalog=my-database;Integrated Security=False;Password=my-password;Connect Timeout=12",
                "Data Source=myserver.com;Initial Catalog=my-database;Integrated Security=False;User ID=my-user-name;Connect Timeout=12",
                "Data Source=myserver.com;Initial Catalog=my-database;Integrated Security=False;User ID=my-user-name;Password=my-password;Connect Timeout=12",
            };

            var expected = new[]
            {
                "Data Source=myserver.com;Initial Catalog=my-database;Integrated Security=False;Password=*****;Connect Timeout=12",
                "Data Source=myserver.com;Initial Catalog=my-database;Integrated Security=False;User ID=*****;Connect Timeout=12",
                "Data Source=myserver.com;Initial Catalog=my-database;Integrated Security=False;User ID=*****;Password=*****;Connect Timeout=12",
                "Data Source=myserver.com;Initial Catalog=my-database;Integrated Security=False;Password=*****;Connect Timeout=12",
                "Data Source=myserver.com;Initial Catalog=my-database;Integrated Security=False;User ID=my-user-name;Connect Timeout=12",
                "Data Source=myserver.com;Initial Catalog=my-database;Integrated Security=False;User ID=my-user-name;Password=*****;Connect Timeout=12",
            };

            // Act
            var actual = new string[0]
                .Concat(connectionStrings.Select(_ => _.ObfuscateCredentialsInConnectionString(obfuscateUserName: true)))
                .Concat(connectionStrings.Select(_ => _.ObfuscateCredentialsInConnectionString(obfuscateUserName: false)))
                .ToArray();

            // Assert
            actual.AsTest().Must().BeEqualTo(expected);
        }
    }
}
