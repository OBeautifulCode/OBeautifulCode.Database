// --------------------------------------------------------------------------------------------------------------------
// <copyright file="DbHelperTest.cs" company="OBeautifulCode">
//   Copyright 2015 OBeautifulCode
// </copyright>
// --------------------------------------------------------------------------------------------------------------------

namespace OBeautifulCode.Database.Test
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Data;
    using System.Data.OleDb;
    using System.Data.SqlClient;
    using System.Globalization;
    using System.IO;
    using System.Transactions;

    using OBeautifulCode.Reflection;

    using Xunit;

    using IsolationLevel = System.Data.IsolationLevel;

    /// <summary>
    /// Tests the <see cref="DbHelper"/> class.
    /// </summary>
    /// <remarks>
    /// This class was ported from an older library that used a poor style of unit testing.
    /// It had a few monolithic test methods instead of many smaller, single purpose methods.
    /// Because of the volume of test code, I was only able to break-up a few of these monolithic tests.
    /// The rest remain as-is.
    /// </remarks>
    public class DbHelperTest : IDisposable
    {
        /// <summary>
        /// Appveyor SQL Server admin user id.
        /// </summary>
        private const string UserId = "sa";

        /// <summary>
        /// Appveyor SQL Server admin password.
        /// </summary>
        private const string Password = "Password12!";

        /// <summary>
        /// Appveyor SQL Server instance name.
        /// </summary>
        private const string Server = @"(local)\SQL2014";

        /// <summary>
        /// Initializes a new instance of the <see cref="DbHelperTest"/> class.
        /// </summary>
        /// <remarks>
        /// Creates a fresh database of stock quotes and seeds with stock quote data.
        /// </remarks>
        public DbHelperTest()
        {
            this.DatabaseName = this.CreatedSeededDatabase();
        }

        /// <summary>
        /// Gets the database connection string.
        /// </summary>
        private string ConnectionString
        {
            get
            {
                return this.BuildConnectionString(this.DatabaseName);
            }
        }

        /// <summary>
        /// Gets or sets the path to the database file.
        /// </summary>
        private string DatabaseName { get; set; }

        // ReSharper disable InconsistentNaming
        // ReSharper disable CoVariantArrayConversion        
        [Fact]
        public static void ToBit_Returns1ForTrue0ForFalse()
        {
            // Arrange, Act, Assert
            Assert.Equal("1", true.ToBit(), StringComparer.CurrentCulture);
            Assert.Equal("0", false.ToBit(), StringComparer.CurrentCulture);
        }

        [Fact]
        public static void CreateParameter_NameIsNull_ThrowsArgumentNullException()
        {
            // Arrange, Act, Assert
            Assert.Throws<ArgumentNullException>(() => DbHelper.CreateParameter<SqlParameter>(null, DbType.String, "asdf"));
        }

        [Fact]
        public static void CreateParameter_NameLessThanTwoCharactersInLength_ThrowsArgumentException()
        {
            // Arrange, Act, Assert
            Assert.Throws<ArgumentException>(() => DbHelper.CreateParameter<SqlParameter>(string.Empty, DbType.String, "asdf"));
            Assert.Throws<ArgumentException>(() => DbHelper.CreateParameter<SqlParameter>("A", DbType.String, "asdf"));
        }

        [Fact]
        public static void CreateParameter_NameDoesNotBeginWithAtSign_ThrowsArgumentException()
        {
            // Arrange, Act, Assert
            Assert.Throws<ArgumentException>(() => DbHelper.CreateParameter<SqlParameter>("AB", DbType.String, "asdf"));
            Assert.Throws<ArgumentException>(() => DbHelper.CreateParameter<SqlParameter>("ABCDE", DbType.String, "asdf"));
        }

        [Fact]
        public static void CreateParameter_NameIsNotAlphaNumeric_ThrowsArgumentException()
        {
            // Arrange, Act, Assert
            Assert.Throws<ArgumentException>(() => DbHelper.CreateParameter<SqlParameter>("@*", DbType.String, "asdf"));
            Assert.Throws<ArgumentException>(() => DbHelper.CreateParameter<SqlParameter>("@Para Meter", DbType.String, "asdf"));
            Assert.Throws<ArgumentException>(() => DbHelper.CreateParameter<SqlParameter>("@     ", DbType.String, "asdf"));
            Assert.Throws<ArgumentException>(() => DbHelper.CreateParameter<SqlParameter>("@  \r\n   ", DbType.String, "asdf"));
            Assert.Throws<ArgumentException>(() => DbHelper.CreateParameter<SqlParameter>("@KDS12*#324KDFJLS", DbType.String, "asdf"));
            Assert.Throws<ArgumentException>(() => DbHelper.CreateParameter<SqlParameter>("@[][][", DbType.String, "asdf"));
        }

        [Fact]
        public static void CreateParameter_StoresAllParametersPassedToConstructorInTheRightDbParameterProperty()
        {
            // Arrange
            const string Name = "@MyParameter";
            const DbType ParamDbType = DbType.Decimal;
            object value = 54m;
            const ParameterDirection ParamDirection = ParameterDirection.ReturnValue;
            int? size = 10;
            byte? precision = 5;
            byte? scale = 3;
            const bool IsNullable = true;

            // Act
            var parameter = (IDbDataParameter)DbHelper.CreateParameter<SqlParameter>(Name, ParamDbType, value, ParamDirection, size, precision, scale, IsNullable);

            // Assert
            Assert.Equal(Name, parameter.ParameterName);
            Assert.Equal(ParamDbType, parameter.DbType);
            Assert.Equal(value, parameter.Value);
            Assert.Equal(ParamDirection, parameter.Direction);
            Assert.Equal(size, parameter.Size);
            Assert.Equal(precision, parameter.Precision);
            Assert.Equal(scale, parameter.Scale);
            Assert.Equal(IsNullable, parameter.IsNullable);            
        }

        [Fact]
        public static void CreateParameter_ValueIsNull_StoresValueAsDbNull()
        {
            // Arrange
            const string Name = "@MyParameter";
            const DbType ParamDbType = DbType.Decimal;

            // Act
            var parameter = (IDbDataParameter)DbHelper.CreateParameter<SqlParameter>(Name, ParamDbType, null);

            // Assert
            Assert.Equal(DBNull.Value, parameter.Value);            
        }

        [Fact]
        public static void CreateParameter_UseDefaultParameterValues_DoesNotThrow()
        {
            // Arrange
            const string Name = "@MyParameter";
            const DbType ParamDbType = DbType.Decimal;

            // Act
            var ex = Record.Exception(() => DbHelper.CreateParameter<SqlParameter>(Name, ParamDbType, null));

            // Assert
            Assert.Null(ex);
        }

        [Fact]
        public void OpenConnection_ConnetionStringIsNull_ThrowsArgumentNullException()
        {
            // Arrange, Act, Assert
            Assert.Throws<ArgumentNullException>(() => DbHelper.OpenConnection<SqlConnection>(null));
        }

        [Fact]
        public void OpenConnection_ConnetionStringIsWhitespace_ThrowsArgumentException()
        {
            // Arrange, Act, Assert
            Assert.Throws<ArgumentException>(() => DbHelper.OpenConnection<SqlConnection>(string.Empty));
            Assert.Throws<ArgumentException>(() => DbHelper.OpenConnection<SqlConnection>("  "));
            Assert.Throws<ArgumentException>(() => DbHelper.OpenConnection<SqlConnection>("  \r\n  "));
        }

        [Fact]
        public void OpenConnection_ConnetionStringNotProperlyConstructed_ThrowsArgumentException()
        {
            // Arrange, Act, Assert
            Assert.Throws<ArgumentException>(() => DbHelper.OpenConnection<SqlConnection>("connetionstring"));
        }

        [Fact]
        public void OpenConnection_ConnectionCannotBeEstablished_ThrowsSqlException()
        {
            // Arrange, Act, Assert
            Assert.Throws<SqlException>(() => DbHelper.OpenConnection<SqlConnection>("Data Source=DOESNTEXIST;Initial Catalog=StockQuotes;Integrated Security=SSPI;"));
        }

        [Fact]
        public void OpenConnection_ConnectionStringIsValidAndDatabaseIsAccessible_ReturnsOpenConnection()
        {
            // Arrange, Act, Assert
            using (var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                Assert.Equal(ConnectionState.Open, connection.State);
                connection.Close();
            }
        }

        [Fact]
        public void OpenConnection_UsingOleDbConnection_ReturnsOpenConnection()
        {
            // Arrange
            string connectionString = string.Format("Provider=SQLNCLI11;Server={0};Database={1};Trusted_Connection=yes;User Id={2};Password={3};Timeout=3600", Server, this.DatabaseName, UserId, Password);

            // Act, Assert
            using (var connection = DbHelper.OpenConnection<OleDbConnection>(connectionString))
            {
                Assert.Equal(ConnectionState.Open, connection.State);
                connection.Close();
            }
        }

        [Fact]
        public void BuildCommand_ConnectionIsNull_ThrowsArgumentNullException()
        {
            // Arrange, Act, Assert
            Assert.Throws<ArgumentNullException>(() => DbHelper.BuildCommand(null, "Select * From [Whatever]"));
        }

        [Fact]
        public void BuildCommand_ConnectionIsNotOpen_ThrowsArgumentException()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            const string SqlStatement = "Select * From StockQuotes";
            connection.Close();

            // Act, Assert
            var actualException = Assert.Throws<ArgumentException>(() => DbHelper.BuildCommand(connection, SqlStatement));
            Assert.Equal("connection is in an invalid state: " + connection.State + ".  Must be Open.", actualException.Message);

            // Cleanup
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_ConnectionIsDisposed_ThrowsArgumentException()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            const string SqlStatement = "Select * From StockQuotes";
            connection.Dispose();

            // Act, Assert
            var actualException = Assert.Throws<ArgumentException>(() => DbHelper.BuildCommand(connection, SqlStatement));
            Assert.Equal("connection is in an invalid state: " + connection.State + ".  Must be Open.", actualException.Message);
        }

        [Fact]
        public void BuildCommand_CommandTextIsNull_ThrowsArgumentNullException()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);

            // Act, Assert
            Assert.Throws<ArgumentNullException>(() => DbHelper.BuildCommand(connection, null));

            // Cleanup
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_CommandTextIsWhitespace_ThrowsArgumentException()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);

            // Act, Assert
            Assert.Throws<ArgumentException>(() => DbHelper.BuildCommand(connection, string.Empty));
            Assert.Throws<ArgumentException>(() => DbHelper.BuildCommand(connection, "   "));
            Assert.Throws<ArgumentException>(() => DbHelper.BuildCommand(connection, "  \r\n  "));

            // Cleanup
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_TimeoutIsLessThanZero_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            const string SqlStatement = "Select * From StockQuotes";

            // Act, Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => DbHelper.BuildCommand(connection, SqlStatement, null, CommandType.Text, null, false, -1));
            Assert.Throws<ArgumentOutOfRangeException>(() => DbHelper.BuildCommand(connection, SqlStatement, null, CommandType.Text, null, false, int.MinValue));

            // Cleanup            
            connection.Close();
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_TransactionIsInvalidBecauseItHasBeenRolledBack_ThrowsArgumentException()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            const string SqlStatement = "Select * From StockQuotes";
            var transaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted);
            transaction.Rollback();

            // Act, Assert
            Assert.Null(transaction.Connection);
            var actualException = Assert.Throws<ArgumentException>(() => DbHelper.BuildCommand(connection, SqlStatement, null, CommandType.Text, transaction));
            Assert.Equal("transaction is invalid.", actualException.Message);

            // Cleanup            
            transaction.Dispose();
            connection.Close();
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_TransactionIsInvalidBecauseItHasBeenComitted_ThrowsArgumentException()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            const string SqlStatement = "Select * From StockQuotes";
            var transaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted);
            transaction.Commit();

            // Act, Assert
            Assert.Null(transaction.Connection);
            var actualException = Assert.Throws<ArgumentException>(() => DbHelper.BuildCommand(connection, SqlStatement, null, CommandType.Text, transaction));
            Assert.Equal("transaction is invalid.", actualException.Message);

            // Cleanup
            transaction.Dispose();
            connection.Close();
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_TransactionUsesDifferentConnectionThenTheSpecifiedOne_ThrowsArgumentException()
        {
            // Arrange
            var connection1 = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            string databaseFilePath2 = this.CreatedSeededDatabase();
            var connection2 = DbHelper.OpenConnection<SqlConnection>(this.BuildConnectionString(databaseFilePath2));
            var transaction = connection1.BeginTransaction();
            const string SqlStatement = "Select * From StockQuotes";

            // Act, Assert
            var actualException = Assert.Throws<ArgumentException>(() => DbHelper.BuildCommand(connection2, SqlStatement, null, CommandType.Text, transaction));
            Assert.Equal("transaction is using a different connection than the specified connection.", actualException.Message);

            // Cleanup
            transaction.Dispose();
            connection1.Close();
            connection1.Dispose();
            connection2.Close();
            connection2.Dispose();
        }

        [Fact]
        public void BuildCommand_UseParameterOfDifferentTypeThanOneSupportedByDataProvider_ThrowsInvalidOperationException()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            var parameter = new OleDbParameter("@symbol", OleDbType.VarChar, 10);
            var parameters = new IDataParameter[] { parameter };
            const string SqlStatement = "Select * From [StockQuotes] Where [Symbol] = @symbol";

            // Act, Assert
            var actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.BuildCommand(connection, SqlStatement, parameters));
            Assert.Equal("Attempting to set a parameter of type OleDbParameter that was designed for a data provider other than the provider represented by the specified connection.", actualException.Message);

            // Cleanup
            connection.Close();
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_SomeParametersAreNull_NullParametersAreIgnored()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            var parameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 15);
            var parameters = new IDataParameter[] { null, parameter, null, null };
            const string SqlStatement = "Select * From [StockQuotes] Where [Symbol] = @symbol";

            // Act
            var ex = Record.Exception(() => DbHelper.BuildCommand(connection, SqlStatement, parameters).Dispose());

            // Assert
            Assert.Null(ex);

            // Cleanup
            connection.Close();
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_ValidTextCommand_ReturnsExpectedData()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            const string CommandText = "Select [Open] From [StockQuotes] Where [Date]='1/5/2009' AND [Symbol]='msft'";

            // Act
            IDbCommand command = DbHelper.BuildCommand(connection, CommandText);
            object actual = command.ExecuteScalar();

            // Assert
            Assert.IsType<decimal>(actual);
            Assert.Equal(19.4519m, (decimal)actual);

            // Cleanup
            command.Dispose();
            connection.Close();
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_ValidTextCommandAndPrepareCommandWithNoParameters_ReturnsExpectedData()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            const string CommandText = "Select [Open] From [StockQuotes] Where [Date]='1/5/2009' AND [Symbol]='msft'";

            // Act
            IDbCommand command = DbHelper.BuildCommand(connection, CommandText, null, CommandType.Text, null, true);
            object actual = command.ExecuteScalar();

            // Assert
            Assert.IsType<decimal>(actual);
            Assert.Equal(19.4519m, (decimal)actual);

            // Cleanup
            command.Dispose();
            connection.Close();
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_ValidTextCommandWithParameters_ReturnsExpectedData()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            const string CommandText = "Select [Low] From [StockQuotes] Where [Date]=@date AND [Symbol]=@symbol";
            var dateParameter = new SqlParameter("@date", new DateTime(2009, 1, 5));
            var symbolParameter = new SqlParameter("@symbol", "msft");
            var parameters = new[] { dateParameter, symbolParameter };

            // Act
            IDbCommand command = DbHelper.BuildCommand(connection, CommandText, parameters);
            object actual = command.ExecuteScalar();

            // Assert
            Assert.IsType<decimal>(actual);
            Assert.Equal(19.317m, (decimal)actual);

            // Cleanup
            command.Dispose();
            connection.Close();
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_ValidTextCommandWithParametersAndPrepareCommand_ReturnsExpectedData()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            const string CommandText = "Select [Close] From [StockQuotes] Where [Date]=@date AND [Symbol]=@symbol";
            var dateParameter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
            var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            var parameters = new[] { dateParameter, symbolParameter };

            // Act
            IDbCommand command = DbHelper.BuildCommand(connection, CommandText, parameters, CommandType.Text, null, true);
            object actual = command.ExecuteScalar();

            // Assert
            Assert.IsType<decimal>(actual);
            Assert.Equal(19.76m, (decimal)actual);

            // Cleanup
            command.Dispose();
            connection.Close();
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_ValidStoredProcedure_ReturnsExpectedData()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            const string CommandText = "FetchClosePrice";
            var dateParameter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
            var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            var closePriceOutputParameter = new SqlParameter { ParameterName = "@close", Direction = ParameterDirection.Output, SqlDbType = SqlDbType.Money };
            var parameters = new[] { dateParameter, symbolParameter, closePriceOutputParameter };

            // Act
            IDbCommand command = DbHelper.BuildCommand(connection, CommandText, parameters, CommandType.StoredProcedure);
            command.ExecuteScalar();

            // Assert
            Assert.IsType<decimal>(closePriceOutputParameter.Value);
            Assert.Equal(19.76m, (decimal)closePriceOutputParameter.Value);

            // Cleanup
            command.Dispose();
            connection.Close();
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_ValidStoredProcedureAndPrepareParameters_ReturnsExpectedData()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            const string CommandText = "FetchClosePrice";
            var dateParameter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 2, 5) };
            var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            var closePriceOutputParameter = new SqlParameter { ParameterName = "@close", Direction = ParameterDirection.Output, SqlDbType = SqlDbType.Money };
            var parameters = new[] { dateParameter, symbolParameter, closePriceOutputParameter };

            // Act
            IDbCommand command = DbHelper.BuildCommand(connection, CommandText, parameters, CommandType.StoredProcedure, null, true);
            command.ExecuteScalar();

            // Assert
            Assert.IsType<decimal>(closePriceOutputParameter.Value);
            Assert.Equal(18.34m, (decimal)closePriceOutputParameter.Value);

            // Cleanup
            command.Dispose();
            connection.Close();
            connection.Dispose();
        }

        [Fact(Skip = "Haven't researched how to do a TableDirect query")]
        public void BuildCommand_ValidTableDirectQuery_ReturnsExpectedData()
        {
        }

        [Fact]
        public void BuildCommand_ValidTextCommandToInsertRowAndUseTransaction_ReturnsNumberOfRowsAffected()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            const string InsertValueSql = @"INSERT [StockQuotes] ([Symbol], [Date], [Open], [High], [Low], [Close], [Volume], [OpenInterest]) VALUES (@Symbol, @Date, 1,2,3,4,5, NULL)";
            var day = new DateTime(2014, 1, 1);
            var dateParameter = new SqlParameter("@Date", SqlDbType.SmallDateTime) { Value = day };
            var symbolParameter = new SqlParameter("@Symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            var parameters = new[] { dateParameter, symbolParameter };
            var transaction = connection.BeginTransaction();

            // Act
            IDbCommand command = DbHelper.BuildCommand(connection, InsertValueSql, parameters, CommandType.Text, transaction, true);
            object actual = command.ExecuteNonQuery();

            // Assert
            Assert.Equal(1, actual);

            // Cleanup            
            transaction.Dispose();
            command.Dispose();
            connection.Close();
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_ValidTextCommandToRetrieveInsertedRowAndUseTransactionForBoth_ReturnsExpectedData()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            const string InsertValueSql = @"INSERT [StockQuotes] ([Symbol], [Date], [Open], [High], [Low], [Close], [Volume], [OpenInterest]) VALUES (@Symbol, @Date, 1,2,3,4,506, NULL)";
            const string VerifyValueSql = "Select [Volume] From [StockQuotes] Where (Date = @Date) And (Symbol = @Symbol)";
            var day = new DateTime(2014, 1, 1);
            var dateParameter1 = new SqlParameter("@Date", SqlDbType.SmallDateTime) { Value = day };
            var symbolParameter1 = new SqlParameter("@Symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            var dateParameter2 = new SqlParameter("@Date", SqlDbType.SmallDateTime) { Value = day };
            var symbolParameter2 = new SqlParameter("@Symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            var parameters1 = new[] { dateParameter1, symbolParameter1 };
            var parameters2 = new[] { dateParameter2, symbolParameter2 };
            var transaction = connection.BeginTransaction();

            // Act
            IDbCommand command1 = DbHelper.BuildCommand(connection, InsertValueSql, parameters1, CommandType.Text, transaction, true);
            command1.ExecuteNonQuery();
            IDbCommand command2 = DbHelper.BuildCommand(connection, VerifyValueSql, parameters2, CommandType.Text, transaction, true);
            var actual = command2.ExecuteScalar();

            // Assert
            Assert.IsType<long>(actual);
            Assert.Equal(506, (long)actual);

            // Cleanup
            transaction.Dispose();
            command1.Dispose();
            command2.Dispose();
            connection.Close();
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_ValidTextCommandToInsertedRowUseTransactionAndThenRollback_InsertedRowIsRemovedFromDatabase()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString);
            const string InsertValueSql = @"INSERT [StockQuotes] ([Symbol], [Date], [Open], [High], [Low], [Close], [Volume], [OpenInterest]) VALUES (@Symbol, @Date, 1,2,3,4,506, NULL)";
            const string VerifyValueSql = "Select [Volume] From [StockQuotes] Where (Date = @Date) And (Symbol = @Symbol)";
            var day = new DateTime(2014, 1, 1);
            var dateParameter1 = new SqlParameter("@Date", SqlDbType.SmallDateTime) { Value = day };
            var symbolParameter1 = new SqlParameter("@Symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            var dateParameter2 = new SqlParameter("@Date", SqlDbType.SmallDateTime) { Value = day };
            var symbolParameter2 = new SqlParameter("@Symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            var parameters1 = new[] { dateParameter1, symbolParameter1 };
            var parameters2 = new[] { dateParameter2, symbolParameter2 };
            var transaction = connection.BeginTransaction();

            // Act
            IDbCommand command1 = DbHelper.BuildCommand(connection, InsertValueSql, parameters1, CommandType.Text, transaction, true);
            command1.ExecuteNonQuery();
            transaction.Rollback();
            IDbCommand command2 = DbHelper.BuildCommand(connection, VerifyValueSql, parameters2, CommandType.Text, null, true);
            var actual = command2.ExecuteScalar();

            // Assert
            Assert.Null(actual);

            // Cleanup
            transaction.Dispose();
            command1.Dispose();
            command2.Dispose();
            connection.Close();
            connection.Dispose();
        }

        [Fact]
        public void BuildCommand_MonolithicTest()
        {
            // issues with parameters
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                // type not specified
                IDbDataParameter parameter = new SqlParameter("@symbol", "msft");
                Exception actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.BuildCommand(sqlConnection, "Select * From [StockQuotes] Where [Symbol] = @symbol", new IDataParameter[] { parameter }, CommandType.Text, null, true, 30));
                Assert.Equal("SqlCommand.Prepare method requires all parameters to have an explicitly set type.", actualException.Message);

                // length parameters not specified
                parameter = new SqlParameter("@symbol", SqlDbType.VarChar);
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.BuildCommand(sqlConnection, "Select * From [StockQuotes] Where [Symbol] = @symbol", new IDataParameter[] { parameter }, CommandType.Text, null, true, 30));
                Assert.Equal("SqlCommand.Prepare method requires all variable length parameters to have an explicitly set non-zero Size.", actualException.Message);

                // parameter is contained by another collection
                parameter.Size = 10;
                actualException = Assert.Throws<ArgumentException>(() => DbHelper.BuildCommand(sqlConnection, "Select * From [StockQuotes] Where [Symbol] = @symbol", new IDataParameter[] { parameter }, CommandType.Text, null, true, 30));
                Assert.Equal("The SqlParameter is already contained by another SqlParameterCollection.", actualException.Message);

                // cannot use prepare when connection is pending a local transaction
                using (SqlTransaction sqlTransaction = sqlConnection.BeginTransaction())
                {
                    parameter = new SqlParameter("@symbol", SqlDbType.VarChar, 10);
                    actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.BuildCommand(sqlConnection, "Select * From [StockQuotes] Where [Symbol] = @symbol", new IDataParameter[] { parameter }, CommandType.Text, null, true, 30));
                    Assert.Equal("Prepare requires the command to have a transaction when the connection assigned to the command is in a pending local transaction.  The Transaction property of the command has not been initialized.", actualException.Message);
                    sqlTransaction.Rollback();
                }

                // mismatch of parameter type and provider type
                parameter = new OleDbParameter("@symbol", OleDbType.VarChar, 10);
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.BuildCommand(sqlConnection, "Select * From [StockQuotes] Where [Symbol] = @symbol", new IDataParameter[] { parameter }, CommandType.Text, null, true, 30));
                Assert.Equal("Attempting to set a parameter of type OleDbParameter that was designed for a data provider other than the provider represented by the specified connection.", actualException.Message);
            }

            // valid command with ambient transaction - rollback
            DateTime now = DateTime.Now;
            SqlParameter dateParameter;
            const string InsertSql = "Insert Into [DbHelper] (Date, Value) Values (@Date, @Value)";
            const string VerifyValueSql = "Select [Value] From [DbHelper] Where Date = @Date";
            using (new TransactionScope())
            {
                using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
                {
                    // insert value
                    dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
                    var valueParameter = new SqlParameter("@Value", SqlDbType.Decimal) { Value = 15.4, Precision = 10, Scale = 5 };
                    using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, InsertSql, new[] { dateParameter, valueParameter }, CommandType.Text, null, true, 30))
                    {
                        Assert.Equal(1, command.ExecuteNonQuery());
                    }
                }

                using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
                {
                    // verify exists
                    dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
                    using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, VerifyValueSql, new[] { dateParameter }, CommandType.Text, null, true, 30))
                    {
                        object result = command.ExecuteScalar();
                        Assert.IsType<decimal>(result);
                        Assert.Equal(15.4m, (decimal)result);
                    }
                }
            }

            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                // scope was not comitted - data should have been discarded
                dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
                using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, VerifyValueSql, new[] { dateParameter }, CommandType.Text, null, true, 30))
                {
                    Assert.Null(command.ExecuteScalar());
                }
            }

            // valid command with ambient transaction - commit
            now = DateTime.Now;
            using (var scope = new TransactionScope())
            {
                using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
                {
                    // insert value
                    dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
                    var valueParameter = new SqlParameter("@Value", SqlDbType.Decimal) { Value = 15.4, Precision = 10, Scale = 5 };
                    using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, InsertSql, new[] { dateParameter, valueParameter }, CommandType.Text, null, true, 30))
                    {
                        Assert.Equal(1, command.ExecuteNonQuery());
                    }
                }

                using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
                {
                    // verify exists
                    dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
                    using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, VerifyValueSql, new[] { dateParameter }, CommandType.Text, null, true, 30))
                    {
                        object result = command.ExecuteScalar();
                        Assert.IsType<decimal>(result);
                        Assert.Equal(15.4m, (decimal)result);
                    }
                }

                scope.Complete();
            }

            // verify exists
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
                using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, VerifyValueSql, new[] { dateParameter }, CommandType.Text, null, true, 30))
                {
                    object result = command.ExecuteScalar();
                    Assert.IsType<decimal>(result);
                    Assert.Equal(15.4m, (decimal)result);
                }
            }

            // // transaction not provided - connection is pending a local transaction
            // using ( SqlConnection connection = DbHelper.OpenSqlConnection( this.ConnectionString ) )
            // {
            //     using ( connection.BeginTransaction( IsolationLevel.ReadUncommitted ) )
            //     {
            //         Assert.Throws<InvalidOperationException>( () => DbHelper.BuildDataReader( connection , sqlQuery , 30 ) );
            //     }
            // }            
        }

        [Fact]
        public void ExecuteReader_ConnectionObjectProvided_MonolithicTest()
        {
            // not testing: all exceptions generated from BuildCommand

            // exception executing command
            Exception actualException;
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<SqlException>(() => DbHelper.ExecuteReader(sqlConnection, "Select * "));
                Assert.Equal("Must specify table to select from.", actualException.Message);
                Assert.Equal(ConnectionState.Open, sqlConnection.State);
                sqlConnection.Close();
            }

            // parameter is missing
            const string SqlQueryParameterized = "Select [Open] From [StockQuotes] Where [Date] = @date AND [Symbol] = @symbol";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };                
                actualException = Assert.Throws<SqlException>(() => DbHelper.ExecuteReader(sqlConnection, SqlQueryParameterized, new[] { dateParamter }));                
                Assert.Equal("Must declare the scalar variable \"@symbol\".", actualException.Message);
                Assert.Equal(ConnectionState.Open, sqlConnection.State);
                sqlConnection.Close();
            }

            // transaction not provided - connection is pending a local transaction
            const string SqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] = '1/5/2009' AND [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                using (sqlConnection.BeginTransaction(IsolationLevel.ReadUncommitted))
                {
                    actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ExecuteReader(sqlConnection, SqlQueryNonParameterized));
                    Assert.Equal("ExecuteReader requires the command to have a transaction when the connection assigned to the command is in a pending local transaction.  The Transaction property of the command has not been initialized.", actualException.Message);
                }

                sqlConnection.Close();
            }

            // there is already an open datareader associated with the connection
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                using (IDataReader reader = DbHelper.ExecuteReader(sqlConnection, SqlQueryNonParameterized))
                {
                    actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ExecuteReader(sqlConnection, SqlQueryNonParameterized));
                    Assert.Equal("There is already an open DataReader associated with this Command which must be closed first.", actualException.Message);
                    reader.Close();
                }

                sqlConnection.Close();
            }

            // parameter datatypes are wrong - this does not throw an SqlException.  It simply returns a reader with no rows.  Calling Read() on reader will throw.
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                using (IDataReader reader = DbHelper.ExecuteReader(sqlConnection, SqlQueryParameterized, new[] { dateParamter, symbolParameter }, CommandType.Text, null, CommandBehavior.CloseConnection))
                {
                    actualException = Assert.Throws<SqlException>(() => reader.Read());
                    Assert.Equal("Conversion failed when converting character string to smalldatetime data type.", actualException.Message);
                    reader.Close();
                }

                sqlConnection.Close();
            }

            // behavior is used in executing reader
            // reader matches connection type
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                using (IDataReader reader = DbHelper.ExecuteReader(sqlConnection, SqlQueryNonParameterized, null, CommandType.Text, null, CommandBehavior.CloseConnection))
                {
                    Assert.IsType<SqlDataReader>(reader);
                    reader.Close();
                    Assert.Equal(ConnectionState.Closed, sqlConnection.State);
                }
            }

            // get a reader with actual data
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
                using (IDataReader reader = DbHelper.ExecuteReader(sqlConnection, SqlQueryParameterized, new[] { dateParamter, symbolParameter }, CommandType.Text, null, CommandBehavior.Default, true))
                {
                    Assert.Equal(1, reader.FieldCount);
                    Assert.True(reader.Read());
                    Assert.IsType<decimal>(reader["Open"]);
                    Assert.Equal(19.4519m, (decimal)reader["Open"]);
                    Assert.False(reader.Read());
                    reader.Close();
                }
            }
        }

        [Fact]
        public void ExecuteReader_ConnectionStringProvided_MonolithicTest()
        {
            // not testing: all exceptions generated from BuildCommand
            // not testing: all exceptions from OpenConnection
            // not testing: all exceptions generated from ExecuteReader with connection

            // exception executing command
            Exception actualException = Assert.Throws<SqlException>(() => DbHelper.ExecuteReader<SqlConnection>(this.ConnectionString, "SELECT * "));
            Assert.Equal("Must specify table to select from.", actualException.Message);

            // parameter datatypes are wrong - this does not throw an SqlException.  It simply returns a reader with no rows. Call Read() on reader will throw.
            string sqlQueryParameterized = "Select [Open] From [StockQuotes] Where [Date] = @date AND [Symbol] = @symbol";
            var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
            var symbolParameter = new SqlParameter("@symbol", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
            using (IDataReader reader = DbHelper.ExecuteReader<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { dateParamter, symbolParameter }, CommandType.Text, CommandBehavior.CloseConnection))
            {
                actualException = Assert.Throws<SqlException>(() => reader.Read());
                Assert.Equal("Conversion failed when converting character string to smalldatetime data type.", actualException.Message);
                reader.Close();
            }

            // IDataReader type matches connection type
            const string SqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] = '1/5/2009' AND [Symbol] = 'msft'";
            using (IDataReader reader = DbHelper.ExecuteReader<SqlConnection>(this.ConnectionString, SqlQueryNonParameterized, null, CommandType.Text, CommandBehavior.CloseConnection))
            {
                Assert.IsType<SqlDataReader>(reader);
                reader.Close();
            }

            // get a reader with actual data
            // behavior is used in executing reader (can't get Connection from data reader)
            sqlQueryParameterized = "Select [Open] From [StockQuotes] Where [Date] >= @date AND [Symbol] = @symbol Order By [Date]";
            dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
            symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            using (IDataReader reader = DbHelper.ExecuteReader<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { dateParamter, symbolParameter }, CommandType.Text, CommandBehavior.SingleRow | CommandBehavior.CloseConnection, true))
            {
                Assert.Equal(1, reader.FieldCount);
                Assert.True(reader.Read());
                Assert.IsType<decimal>(reader["Open"]);
                Assert.Equal(19.4519m, (decimal)reader["Open"]);
                Assert.False(reader.Read());  // CommandBehavior.SingleRow ensures only one row returned.
                reader.Close();
            }
        }

        [Fact]
        public void ExecuteNonQuery_ConnectionObjectProvided_MonolithicTest()
        {
            // not testing: all exceptions generated from BuildCommand

            // exception executing command
            Exception actualException;
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<SqlException>(() => DbHelper.ExecuteNonQuery(sqlConnection, "Insert Into "));
                Assert.Equal("Incorrect syntax near 'Into'.", actualException.Message);
                Assert.Equal(ConnectionState.Open, sqlConnection.State);
                sqlConnection.Close();
            }

            // parameter is missing
            string sqlQueryParameterized = "Update [StockQuotes] Set [Symbol] = @symbol Where [Date] = @date and [Symbol] = @symbol";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                actualException = Assert.Throws<SqlException>(() => DbHelper.ExecuteNonQuery(sqlConnection, sqlQueryParameterized, new[] { dateParamter }));
                Assert.Equal("Must declare the scalar variable \"@symbol\".", actualException.Message);
                Assert.Equal(ConnectionState.Open, sqlConnection.State);
                sqlConnection.Close();
            }

            // transaction not provided - connection is pending a local transaction
            const string SqlQueryNonParameterized = "Update [StockQuotes] Set [Symbol] = 'msft' Where [Date] = '1/5/2009' and [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                using (sqlConnection.BeginTransaction(IsolationLevel.ReadUncommitted))
                {
                    actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ExecuteNonQuery(sqlConnection, SqlQueryNonParameterized));
                    Assert.Equal("ExecuteNonQuery requires the command to have a transaction when the connection assigned to the command is in a pending local transaction.  The Transaction property of the command has not been initialized.", actualException.Message);
                }

                sqlConnection.Close();
            }

            // parameter datatypes are wrong
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                actualException = Assert.Throws<SqlException>(() => DbHelper.ExecuteNonQuery(sqlConnection, sqlQueryParameterized, new[] { dateParamter, symbolParameter }));
                Assert.Equal("Conversion failed when converting character string to smalldatetime data type.", actualException.Message);
                sqlConnection.Close();
            }

            // execute non-query works. returns number of rows affected
            sqlQueryParameterized = "Update [StockQuotes] Set [Symbol] = @newSymbol Where [Date] = @date and [Symbol] = @oldSymbol";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                using (SqlTransaction transaction = sqlConnection.BeginTransaction())
                {
                    // newsymbol doesn't exist
                    const string VerifyInsertSql = "Select [Open] From [StockQuotes] Where [Symbol] = @symbol and [Date] = @date";
                    var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                    var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "newsymbol" };
                    using (IDataReader reader = DbHelper.ExecuteReader(sqlConnection, VerifyInsertSql, new[] { dateParamter, symbolParameter }, CommandType.Text, transaction, CommandBehavior.Default, true))
                    {
                        Assert.False(reader.Read());
                        reader.Close();
                    }

                    // rename msft to newsymbol for 1/5/2009
                    dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                    var newSymbolParameter = new SqlParameter("@newSymbol", SqlDbType.NVarChar, 10) { Value = "newsymbol" };
                    var oldSymbolParameter = new SqlParameter("@oldSymbol", SqlDbType.NVarChar, 10) { Value = "msft" };
                    int rowsAffected = DbHelper.ExecuteNonQuery(sqlConnection, sqlQueryParameterized, new[] { dateParamter, newSymbolParameter, oldSymbolParameter }, CommandType.Text, transaction, true);
                    Assert.Equal(1, rowsAffected);

                    // verify
                    dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                    symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "newsymbol" };
                    using (IDataReader reader = DbHelper.ExecuteReader(sqlConnection, VerifyInsertSql, new[] { dateParamter, symbolParameter }, CommandType.Text, transaction, CommandBehavior.Default, true))
                    {
                        Assert.Equal(1, reader.FieldCount);
                        Assert.True(reader.Read());
                        Assert.IsType<decimal>(reader["Open"]);
                        Assert.Equal(19.4519m, (decimal)reader["Open"]);
                        Assert.False(reader.Read());
                        reader.Close();
                    }

                    // discard changes
                    transaction.Rollback();
                }

                sqlConnection.Close();
            }
        }

        [Fact]
        public void ExecuteNonQuery_ConnectionStringProvided_MonolithicTest()
        {
            // not testing: all exceptions generated from BuildCommand
            // not testing: all exceptions from OpenConnection
            // not testing: all exceptions generated from ExecuteReader with connection

            // exception executing command
            Exception actualException = Assert.Throws<SqlException>(() => DbHelper.ExecuteNonQuery<SqlConnection>(this.ConnectionString, "Insert Into "));
            Assert.Equal("Incorrect syntax near 'Into'.", actualException.Message);

            // execute non-query works. returns number of rows affected
            using (new TransactionScope())
            {
                // newsymbol doesn't exist
                const string VerifyInsertSql = "Select [Open] From [StockQuotes] Where [Symbol] = @symbol and [Date] = @date";
                var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "newsymbol" };
                using (IDataReader reader = DbHelper.ExecuteReader<SqlConnection>(this.ConnectionString, VerifyInsertSql, new[] { dateParamter, symbolParameter }, CommandType.Text, CommandBehavior.CloseConnection, true))
                {
                    Assert.False(reader.Read());
                    reader.Close();
                }

                // rename msft to newsymbol for 1/5/2009
                const string SqlInsertParameterized = "Update [StockQuotes] Set [Symbol] = @newSymbol Where [Date] = @date and [Symbol] = @oldSymbol";
                dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                var newSymbolParameter = new SqlParameter("@newSymbol", SqlDbType.NVarChar, 10) { Value = "newsymbol" };
                var oldSymbolParameter = new SqlParameter("@oldSymbol", SqlDbType.NVarChar, 10) { Value = "msft" };
                int rowsAffected = DbHelper.ExecuteNonQuery<SqlConnection>(this.ConnectionString, SqlInsertParameterized, new[] { dateParamter, newSymbolParameter, oldSymbolParameter }, CommandType.Text, true, 30);
                Assert.Equal(1, rowsAffected);

                // verify
                dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "newsymbol" };
                using (IDataReader reader = DbHelper.ExecuteReader<SqlConnection>(this.ConnectionString, VerifyInsertSql, new[] { dateParamter, symbolParameter }, CommandType.Text, CommandBehavior.CloseConnection, true))
                {
                    Assert.Equal(1, reader.FieldCount);
                    Assert.True(reader.Read());
                    Assert.IsType<decimal>(reader["Open"]);
                    Assert.Equal(19.4519m, (decimal)reader["Open"]);
                    Assert.False(reader.Read());
                    reader.Close();
                }
            }
        }

        [Fact]
        public void CommandHasRows_ConnectionObjectProvided_MonolithicTest()
        {
            // not testing: all exceptions generated from BuildCommand via ExecuteReader
            // not testing: all exceptions from ExecuteReader
            // exception executing command
            Exception actualException;
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<SqlException>(() => DbHelper.CommandHasRows(sqlConnection, "Select * "));
                Assert.Equal("Must specify table to select from.", actualException.Message);
                Assert.Equal(ConnectionState.Open, sqlConnection.State);
                sqlConnection.Close();
            }

            // parameter datatypes are wrong
            const string SqlQueryParameterized = "Select [Open] From [StockQuotes] Where [Date] = @date AND [Symbol] = @symbol";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                actualException = Assert.Throws<SqlException>(() => DbHelper.CommandHasRows(sqlConnection, SqlQueryParameterized, new[] { dateParamter, symbolParameter }));
                Assert.Equal("Conversion failed when converting character string to smalldatetime data type.", actualException.Message);
                sqlConnection.Close();
            }

            // command produces no rows
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2012, 2, 12) };
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
                Assert.False(DbHelper.CommandHasRows(sqlConnection, SqlQueryParameterized, new[] { dateParamter, symbolParameter }, CommandType.Text, null, true));
                sqlConnection.Close();
            }

            // command produces 1 row
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
                Assert.True(DbHelper.CommandHasRows(sqlConnection, SqlQueryParameterized, new[] { dateParamter, symbolParameter }, CommandType.Text, null, true));
                sqlConnection.Close();
            }

            // command produces many rows
            const string SqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] > '1/5/2009' AND [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                Assert.True(DbHelper.CommandHasRows(sqlConnection, SqlQueryNonParameterized));
                sqlConnection.Close();
            }
        }

        [Fact]
        public void CommandHasRows_ConnectionStringProvided_MonolithicTest()
        {
            // not testing: all exceptions generated from BuildCommand via ExecuteReader
            // not testing: all exceptions from OpenConnection via ExecuteReader
            // not testing: all exceptions generated from ExecuteReader with connection

            // exception executing command
            Exception actualException = Assert.Throws<SqlException>(() => DbHelper.CommandHasRows<SqlConnection>(this.ConnectionString, "SELECT * "));
            Assert.Equal("Must specify table to select from.", actualException.Message);

            // parameter datatypes are wrong
            string sqlQueryParameterized = "Select [Open] From [StockQuotes] Where [Date] = @date AND [Symbol] = @symbol";
            var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
            var symbolParameter = new SqlParameter("@symbol", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
            actualException = Assert.Throws<SqlException>(() => DbHelper.CommandHasRows<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { dateParamter, symbolParameter }));
            Assert.Equal("Conversion failed when converting character string to smalldatetime data type.", actualException.Message);

            // command produces no rows
            sqlQueryParameterized = "Select [Open] From [StockQuotes] Where [Date] >= @date AND [Symbol] = @symbol Order By [Date]";
            dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2012, 3, 12) };
            symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            Assert.False(DbHelper.CommandHasRows<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { dateParamter, symbolParameter }, CommandType.Text, true));

            // command produces 1 row
            dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
            symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            Assert.True(DbHelper.CommandHasRows<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { dateParamter, symbolParameter }, CommandType.Text, true));

            // command produces many rows
            const string SqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] > '1/5/2009' AND [Symbol] = 'msft'";
            Assert.True(DbHelper.CommandHasRows<SqlConnection>(this.ConnectionString, SqlQueryNonParameterized, null, CommandType.Text, true));
        }

        [Fact]
        public void ReadSingleColumn_ConnectionObjectProvided_MonolithicTest()
        {
            // not testing: all exceptions generated from BuildCommand via ExecuteReader
            // not testing: all exceptions from ExecuteReader

            // exception executing command
            Exception actualException;
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<SqlException>(() => DbHelper.ReadSingleColumn(sqlConnection, "Select * "));
                Assert.Equal("Must specify table to select from.", actualException.Message);
                Assert.Equal(ConnectionState.Open, sqlConnection.State);
                sqlConnection.Close();
            }

            // parameter datatypes are wrong
            string sqlQueryParameterized = "Select [Open] From [StockQuotes] Where [Date] = @date And [Symbol] = @symbol";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                actualException = Assert.Throws<SqlException>(() => DbHelper.ReadSingleColumn(sqlConnection, sqlQueryParameterized, new[] { dateParamter, symbolParameter }));
                Assert.Equal("Conversion failed when converting character string to smalldatetime data type.", actualException.Message);
                sqlConnection.Close();
            }

            // two columns returned instead of one, with no rows
            string sqlQueryNonParameterized = "Select [Open] , [High] From [StockQuotes] Where [Date] = '12/2/2011' And [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleColumn(sqlConnection, sqlQueryNonParameterized));
                Assert.Equal("Query results in more than one column.", actualException.Message);
                sqlConnection.Close();
            }

            // two columns returned, with rows
            sqlQueryNonParameterized = "Select [Open] , [High] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleColumn(sqlConnection, sqlQueryNonParameterized));
                Assert.Equal("Query results in more than one column.", actualException.Message);
                sqlConnection.Close();
            }

            // one column with no rows returned
            sqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] = '12/2/2011' And [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleColumn(sqlConnection, sqlQueryNonParameterized));
                Assert.Equal("Query results in no rows.", actualException.Message);
                sqlConnection.Close();
            }

            // one column, one row
            sqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                Collection<object> values = DbHelper.ReadSingleColumn(sqlConnection, sqlQueryNonParameterized);
                Assert.Equal(1, values.Count);
                Assert.IsType<decimal>(values[0]);
                Assert.Equal(19.4519m, (decimal)values[0]);
                sqlConnection.Close();
            }

            // one column, two rows
            sqlQueryParameterized = "Select [Open] From [StockQuotes] Where ( [Date] = '1/5/2009' Or [Date] = '1/6/2009' ) And [Symbol] = @symbol Order By [Date]";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
                Collection<object> values = DbHelper.ReadSingleColumn(sqlConnection, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, null, true, 30);
                Assert.Equal(2, values.Count);
                Assert.IsType<decimal>(values[0]);
                Assert.Equal(19.4519m, (decimal)values[0]);
                Assert.IsType<decimal>(values[1]);
                Assert.Equal(19.9804m, (decimal)values[1]);
                sqlConnection.Close();
            }

            // one column three rows
            sqlQueryParameterized = "Select [Open] From [StockQuotes] Where ( [Date] >= '1/5/2009' And [Date] <= '1/7/2009' ) And [Symbol] = @symbol Order By [Date]";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
                Collection<object> values = DbHelper.ReadSingleColumn(sqlConnection, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, null, true, 30);
                Assert.Equal(3, values.Count);
                Assert.IsType<decimal>(values[0]);
                Assert.Equal(19.4519m, (decimal)values[0]);
                Assert.IsType<decimal>(values[1]);
                Assert.Equal(19.9804m, (decimal)values[1]);
                Assert.IsType<decimal>(values[2]);
                Assert.Equal(19.4449m, (decimal)values[2]);
                sqlConnection.Close();
            }

            // column contains null
            sqlQueryParameterized = "Select [OpenInterest] From [StockQuotes] Where ( [Date] >= '1/5/2009' And [Date] <= '1/7/2009' ) And [Symbol] = @symbol Order By [Date]";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
                Collection<object> values = DbHelper.ReadSingleColumn(sqlConnection, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, null, true, 30);
                Assert.Equal(3, values.Count);
                Assert.Null(values[0]);
                Assert.Null(values[1]);
                Assert.Null(values[2]);
                sqlConnection.Close();
            }
        }

        [Fact]
        public void ReadSingleColumn_ConnectionStringProvided_MonolithicTest()
        {
            // not testing: all exceptions generated from BuildCommand via ExecuteReader
            // not testing: all exceptions from OpenConnection via ExecuteReader
            // not testing: all exceptions generated from ExecuteReader with connection

            // exception executing command
            Exception actualException;
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<SqlException>(() => DbHelper.ReadSingleColumn<SqlConnection>(this.ConnectionString, "Select * "));
                Assert.Equal("Must specify table to select from.", actualException.Message);
                Assert.Equal(ConnectionState.Open, sqlConnection.State);
                sqlConnection.Close();
            }

            // parameter datatypes are wrong
            string sqlQueryParameterized = "Select [Open] From [StockQuotes] Where [Date] = @date And [Symbol] = @symbol";
            var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
            var symbolParameter = new SqlParameter("@symbol", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
            actualException = Assert.Throws<SqlException>(() => DbHelper.ReadSingleColumn<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { dateParamter, symbolParameter }));
            Assert.Equal("Conversion failed when converting character string to smalldatetime data type.", actualException.Message);

            // two columns returned instead of one, with no rows
            string sqlQueryNonParameterized = "Select [Open] , [High] From [StockQuotes] Where [Date] = '12/2/2011' And [Symbol] = 'msft'";
            actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleColumn<SqlConnection>(this.ConnectionString, sqlQueryNonParameterized));
            Assert.Equal("Query results in more than one column.", actualException.Message);

            // two columns returned, with rows
            sqlQueryNonParameterized = "Select [Open] , [High] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = 'msft'";
            actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleColumn<SqlConnection>(this.ConnectionString, sqlQueryNonParameterized));
            Assert.Equal("Query results in more than one column.", actualException.Message);

            // one column with no rows returned
            sqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] = '12/2/2011' And [Symbol] = 'msft'";
            actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleColumn<SqlConnection>(this.ConnectionString, sqlQueryNonParameterized));
            Assert.Equal("Query results in no rows.", actualException.Message);

            // one column, one row
            sqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = 'msft'";
            Collection<object> values = DbHelper.ReadSingleColumn<SqlConnection>(this.ConnectionString, sqlQueryNonParameterized);
            Assert.Equal(1, values.Count);
            Assert.IsType<decimal>(values[0]);
            Assert.Equal(19.4519m, (decimal)values[0]);

            // one column, two rows
            sqlQueryParameterized = "Select [Open] From [StockQuotes] Where ( [Date] = '1/5/2009' Or [Date] = '1/6/2009' ) And [Symbol] = @symbol Order By [Date]";
            symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            values = DbHelper.ReadSingleColumn<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, true);
            Assert.Equal(2, values.Count);
            Assert.IsType<decimal>(values[0]);
            Assert.Equal(19.4519m, (decimal)values[0]);
            Assert.IsType<decimal>(values[1]);
            Assert.Equal(19.9804m, (decimal)values[1]);

            // one column three rows
            sqlQueryParameterized = "Select [Open] From [StockQuotes] Where ( [Date] >= '1/5/2009' And [Date] <= '1/7/2009' ) And [Symbol] = @symbol Order By [Date]";
            symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            values = DbHelper.ReadSingleColumn<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, true, 30);
            Assert.Equal(3, values.Count);
            Assert.IsType<decimal>(values[0]);
            Assert.Equal(19.4519m, (decimal)values[0]);
            Assert.IsType<decimal>(values[1]);
            Assert.Equal(19.9804m, (decimal)values[1]);
            Assert.IsType<decimal>(values[2]);
            Assert.Equal(19.4449m, (decimal)values[2]);

            // column contains null values
            sqlQueryParameterized = "Select [OpenInterest] From [StockQuotes] Where ( [Date] >= '1/5/2009' And [Date] <= '1/7/2009' ) And [Symbol] = @symbol Order By [Date]";
            symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            values = DbHelper.ReadSingleColumn<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, true, 30);
            Assert.Equal(3, values.Count);
            Assert.Null(values[0]);
            Assert.Null(values[1]);
            Assert.Null(values[2]);
        }

        [Fact]
        public void ReadSingleValue_ConnectionObjectProvided_MonolithicTest()
        {
            // not testing: all exceptions generated from BuildCommand via ExecuteReader
            // not testing: all exceptions from ExecuteReader

            // exception executing command
            Exception actualException;
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<SqlException>(() => DbHelper.ReadSingleValue(sqlConnection, "Select * "));
                Assert.Equal("Must specify table to select from.", actualException.Message);
                Assert.Equal(ConnectionState.Open, sqlConnection.State);
                sqlConnection.Close();
            }

            // parameter datatypes are wrong
            string sqlQueryParameterized = "Select [Open] From [StockQuotes] Where [Date] = @date And [Symbol] = @symbol";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                actualException = Assert.Throws<SqlException>(() => DbHelper.ReadSingleValue(sqlConnection, sqlQueryParameterized, new[] { dateParamter, symbolParameter }));
                Assert.Equal("Conversion failed when converting character string to smalldatetime data type.", actualException.Message);
                sqlConnection.Close();
            }

            // one column with no rows returned
            string sqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] = '12/2/2011' And [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleValue(sqlConnection, sqlQueryNonParameterized));
                Assert.Equal("Query results in no rows.", actualException.Message);
                sqlConnection.Close();
            }

            // two columns returned instead of one, with no rows
            sqlQueryNonParameterized = "Select [Open] , [High] From [StockQuotes] Where [Date] = '12/2/2011' And [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleValue(sqlConnection, sqlQueryNonParameterized));
                Assert.Equal("Query results in no rows.", actualException.Message);
                sqlConnection.Close();
            }

            // two columns returned, 1 row
            sqlQueryNonParameterized = "Select [Open] , [High] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleValue(sqlConnection, sqlQueryNonParameterized));
                Assert.Equal("Query results in more than one column.", actualException.Message);
                sqlConnection.Close();
            }

            // two columns returned, 2 rows
            sqlQueryNonParameterized = "Select [Open] , [High] From [StockQuotes] Where ( [Date] = '1/5/2009' Or [Date]='1/6/2009' ) And [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleValue(sqlConnection, sqlQueryNonParameterized));
                Assert.Equal("Query results in more than one column.", actualException.Message);
                sqlConnection.Close();
            }

            // one column, two rows
            sqlQueryParameterized = "Select [Open] From [StockQuotes] Where ( [Date] = '1/5/2009' Or [Date] = '1/6/2009' ) And [Symbol] = @symbol Order By [Date]";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleValue(sqlConnection, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, null, true));
                Assert.Equal("Query results in more than one row.", actualException.Message);
                sqlConnection.Close();
            }

            // one column, one row
            sqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                object value = DbHelper.ReadSingleValue(sqlConnection, sqlQueryNonParameterized);
                Assert.IsType<decimal>(value);
                Assert.Equal(19.4519m, (decimal)value);
                sqlConnection.Close();
            }

            // null value
            sqlQueryNonParameterized = "Select [OpenInterest] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                object value = DbHelper.ReadSingleValue(sqlConnection, sqlQueryNonParameterized);
                Assert.Null(value);
            }
        }

        [Fact]
        public void ReadSingleValue_ConnectionStringProvided_MonolithicTest()
        {
            // not testing: all exceptions generated from BuildCommand via ExecuteReader
            // not testing: all exceptions from OpenConnection via ExecuteReader
            // not testing: all exceptions generated from ExecuteReader with connection

            // exception executing command
            Exception actualException = Assert.Throws<SqlException>(() => DbHelper.ReadSingleValue<SqlConnection>(this.ConnectionString, "Select * "));
            Assert.Equal("Must specify table to select from.", actualException.Message);

            // parameter datatypes are wrong
            string sqlQueryParameterized = "Select [Open] From [StockQuotes] Where [Date] = @date And [Symbol] = @symbol";
            var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
            var symbolParameter = new SqlParameter("@symbol", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
            actualException = Assert.Throws<SqlException>(() => DbHelper.ReadSingleValue<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { dateParamter, symbolParameter }));
            Assert.Equal("Conversion failed when converting character string to smalldatetime data type.", actualException.Message);

            // one column with no rows returned
            string sqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] = '12/2/2011' And [Symbol] = 'msft'";
            actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleValue<SqlConnection>(this.ConnectionString, sqlQueryNonParameterized));
            Assert.Equal("Query results in no rows.", actualException.Message);

            // two columns returned instead of one, with no rows
            sqlQueryNonParameterized = "Select [Open] , [High] From [StockQuotes] Where [Date] = '12/2/2011' And [Symbol] = 'msft'";
            actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleValue<SqlConnection>(this.ConnectionString, sqlQueryNonParameterized));
            Assert.Equal("Query results in no rows.", actualException.Message);

            // two columns returned, 1 row
            sqlQueryNonParameterized = "Select [Open] , [High] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = 'msft'";
            actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleValue<SqlConnection>(this.ConnectionString, sqlQueryNonParameterized));
            Assert.Equal("Query results in more than one column.", actualException.Message);

            // two columns returned, 2 rows
            sqlQueryNonParameterized = "Select [Open] , [High] From [StockQuotes] Where ( [Date] = '1/5/2009' Or [Date]='1/6/2009' ) And [Symbol] = 'msft'";
            actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleValue<SqlConnection>(this.ConnectionString, sqlQueryNonParameterized));
            Assert.Equal("Query results in more than one column.", actualException.Message);

            // one column, two rows
            sqlQueryParameterized = "Select [Open] From [StockQuotes] Where ( [Date] = '1/5/2009' Or [Date] = '1/6/2009' ) And [Symbol] = @symbol Order By [Date]";
            symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleValue<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, true));
            Assert.Equal("Query results in more than one row.", actualException.Message);

            // one column, one row
            sqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = 'msft'";
            object value = DbHelper.ReadSingleValue<SqlConnection>(this.ConnectionString, sqlQueryNonParameterized);
            Assert.IsType<decimal>(value);
            Assert.Equal(19.4519m, (decimal)value);

            // null value
            sqlQueryNonParameterized = "Select [OpenInterest] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = 'msft'";
            value = DbHelper.ReadSingleValue<SqlConnection>(this.ConnectionString, sqlQueryNonParameterized);
            Assert.Null(value);
        }

        [Fact]
        public void RollbackTransaction_MonolithicTest()
        {
            using (var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                using (SqlTransaction transaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted))
                {
                    // null transaction
                    Assert.Throws<ArgumentNullException>(() => DbHelper.RollbackTransaction(null));

                    // cannot rollback a transaction that is already rolled back
                    transaction.Rollback();
                    Assert.Throws<InvalidOperationException>(() => DbHelper.RollbackTransaction(transaction));
                }
            }

            using (var connection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                using (SqlTransaction transaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted))
                {
                    // cannot rollback a transaction that is already committed
                    transaction.Commit();
                    Assert.Throws<InvalidOperationException>(() => DbHelper.RollbackTransaction(transaction));
                }
            }

            // cannot repro OperationFailedException.  MSDN is vague about how the Rollback() method can fail.  Throws a generic Exception, which we wrap
            // with an OperationFailedException
        }

        [Fact]
        public void ReadSingleRow_ConnectionObjectProvided_MonolithicTest()
        {
            // not testing: all exceptions generated from BuildCommand via ExecuteReader
            // not testing: all exceptions from ExecuteReader

            // exception executing command
            Exception actualException;
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<SqlException>(() => DbHelper.ReadSingleRow(sqlConnection, "Select * "));
                Assert.Equal("Must specify table to select from.", actualException.Message);
                Assert.Equal(ConnectionState.Open, sqlConnection.State);
                sqlConnection.Close();
            }

            // parameter datatypes are wrong
            string sqlQueryParameterized = "Select [Open] From [StockQuotes] Where [Date] = @date And [Symbol] = @symbol";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
                actualException = Assert.Throws<SqlException>(() => DbHelper.ReadSingleRow(sqlConnection, sqlQueryParameterized, new[] { dateParamter, symbolParameter }));
                Assert.Equal("Conversion failed when converting character string to smalldatetime data type.", actualException.Message);
                sqlConnection.Close();
            }

            // one column returned with no rows
            string sqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] = '12/2/2011' And [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleRow(sqlConnection, sqlQueryNonParameterized));
                Assert.Equal("Query results in no rows.", actualException.Message);
                sqlConnection.Close();
            }

            // two columns returned, with no rows
            sqlQueryNonParameterized = "Select [Open] , [High] From [StockQuotes] Where [Date] = '12/2/2011' And [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleRow(sqlConnection, sqlQueryNonParameterized));
                Assert.Equal("Query results in no rows.", actualException.Message);
                sqlConnection.Close();
            }

            // two columns with same name returned
            sqlQueryNonParameterized = "Select [Open] , [Close] , [Open] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleRow(sqlConnection, sqlQueryNonParameterized));
                Assert.Equal("Query results in two columns with the same name.", actualException.Message);
                sqlConnection.Close();
            }

            // one column, two rows
            sqlQueryParameterized = "Select [Open] From [StockQuotes] Where ( [Date] = '1/5/2009' Or [Date] = '1/6/2009' ) And [Symbol] = @symbol Order By [Date]";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleRow(sqlConnection, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, null, true));
                Assert.Equal("Query results in more than one row.", actualException.Message);
                sqlConnection.Close();
            }

            // two columns, two rows
            sqlQueryParameterized = "Select [Open] , [Close] From [StockQuotes] Where ( [Date] = '1/5/2009' Or [Date] = '1/6/2009' ) And [Symbol] = @symbol Order By [Date]";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleRow(sqlConnection, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, null, true));
                Assert.Equal("Query results in more than one row.", actualException.Message);
                sqlConnection.Close();
            }

            // two columns, three rows
            sqlQueryParameterized = "Select [Open] , [Close] From [StockQuotes] Where ( [Date] >= '1/5/2009' And [Date] <= '1/7/2009' )  And [Symbol] = @symbol Order By [Date]";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
                actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleRow(sqlConnection, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, null, true));
                Assert.Equal("Query results in more than one row.", actualException.Message);
                sqlConnection.Close();
            }

            // one row one column
            sqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = 'msft'";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                Dictionary<string, object> values = DbHelper.ReadSingleRow(sqlConnection, sqlQueryNonParameterized);
                Assert.Equal(1, values.Count);
                Assert.True(values.ContainsKey("open"));
                Assert.IsType<decimal>(values["open"]);
                Assert.Equal(19.4519m, (decimal)values["open"]);
                sqlConnection.Close();
            }

            // one row two columns
            sqlQueryParameterized = "Select [Open] , [High] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = @symbol";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
                Dictionary<string, object> values = DbHelper.ReadSingleRow(sqlConnection, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, null, true, 30);
                Assert.Equal(2, values.Count);
                Assert.True(values.ContainsKey("open"));
                Assert.IsType<decimal>(values["open"]);
                Assert.Equal(19.4519m, (decimal)values["open"]);
                Assert.True(values.ContainsKey("high"));
                Assert.IsType<decimal>(values["high"]);
                Assert.Equal(19.9044m, (decimal)values["high"]);
                sqlConnection.Close();
            }

            // one row three columns, one has null value
            sqlQueryParameterized = "Select [Open] , [High], [OpenInterest] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = @symbol";
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                var symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
                Dictionary<string, object> values = DbHelper.ReadSingleRow(sqlConnection, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, null, true, 30);
                Assert.Equal(3, values.Count);
                Assert.True(values.ContainsKey("open"));
                Assert.IsType<decimal>(values["open"]);
                Assert.Equal(19.4519m, (decimal)values["open"]);
                Assert.True(values.ContainsKey("high"));
                Assert.IsType<decimal>(values["high"]);
                Assert.Equal(19.9044m, (decimal)values["high"]);
                Assert.True(values.ContainsKey("openinterest"));
                Assert.Null(values["openinterest"]);
                sqlConnection.Close();
            }
        }

        [Fact]
        public void ReadSingleRow_ConnectionStringProvided_MonolithicTest()
        {
            // not testing: all exceptions generated from BuildCommand via ExecuteReader
            // not testing: all exceptions from OpenConnection via ExecuteReader
            // not testing: all exceptions generated from ExecuteReader with connection

            // exception executing command
            Exception actualException = Assert.Throws<SqlException>(() => DbHelper.ReadSingleRow<SqlConnection>(this.ConnectionString, "Select * "));
            Assert.Equal("Must specify table to select from.", actualException.Message);

            // parameter datatypes are wrong
            string sqlQueryParameterized = "Select [Open] From [StockQuotes] Where [Date] = @date And [Symbol] = @symbol";
            var dateParamter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
            var symbolParameter = new SqlParameter("@symbol", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
            actualException = Assert.Throws<SqlException>(() => DbHelper.ReadSingleRow<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { dateParamter, symbolParameter }));
            Assert.Equal("Conversion failed when converting character string to smalldatetime data type.", actualException.Message);

            // one column returned with no rows
            string sqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] = '12/2/2011' And [Symbol] = 'msft'";
            actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleRow<SqlConnection>(this.ConnectionString, sqlQueryNonParameterized));
            Assert.Equal("Query results in no rows.", actualException.Message);

            // two columns returned, with no rows
            sqlQueryNonParameterized = "Select [Open] , [High] From [StockQuotes] Where [Date] = '12/2/2011' And [Symbol] = 'msft'";
            actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleRow<SqlConnection>(this.ConnectionString, sqlQueryNonParameterized));
            Assert.Equal("Query results in no rows.", actualException.Message);

            // two columns with same name returned
            sqlQueryNonParameterized = "Select [Open] , [Close] , [Open] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = 'msft'";
            actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleRow<SqlConnection>(this.ConnectionString, sqlQueryNonParameterized));
            Assert.Equal("Query results in two columns with the same name.", actualException.Message);

            // one column, two rows
            sqlQueryParameterized = "Select [Open] From [StockQuotes] Where ( [Date] = '1/5/2009' Or [Date] = '1/6/2009' ) And [Symbol] = @symbol Order By [Date]";
            symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleRow<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, true));
            Assert.Equal("Query results in more than one row.", actualException.Message);

            // two columns, two rows
            sqlQueryParameterized = "Select [Open] , [Close] From [StockQuotes] Where ( [Date] = '1/5/2009' Or [Date] = '1/6/2009' ) And [Symbol] = @symbol Order By [Date]";
            symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleRow<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, true));
            Assert.Equal("Query results in more than one row.", actualException.Message);

            // two columns, three rows
            sqlQueryParameterized = "Select [Open] , [Close] From [StockQuotes] Where ( [Date] >= '1/5/2009' And [Date] <= '1/7/2009' )  And [Symbol] = @symbol Order By [Date]";
            symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.ReadSingleRow<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, true));
            Assert.Equal("Query results in more than one row.", actualException.Message);

            // one row one column
            sqlQueryNonParameterized = "Select [Open] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = 'msft'";
            Dictionary<string, object> values = DbHelper.ReadSingleRow<SqlConnection>(this.ConnectionString, sqlQueryNonParameterized);
            Assert.Equal(1, values.Count);
            Assert.True(values.ContainsKey("open"));
            Assert.IsType<decimal>(values["open"]);
            Assert.Equal(19.4519m, (decimal)values["open"]);

            // one row two columns
            sqlQueryParameterized = "Select [Open] , [High] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = @symbol";
            symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            values = DbHelper.ReadSingleRow<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, true, 30);
            Assert.Equal(2, values.Count);
            Assert.True(values.ContainsKey("open"));
            Assert.IsType<decimal>(values["open"]);
            Assert.Equal(19.4519m, (decimal)values["open"]);
            Assert.True(values.ContainsKey("high"));
            Assert.IsType<decimal>(values["high"]);
            Assert.Equal(19.9044m, (decimal)values["high"]);

            // one row three columns, one has null value
            sqlQueryParameterized = "Select [Open] , [High], [OpenInterest] From [StockQuotes] Where [Date] = '1/5/2009' And [Symbol] = @symbol";
            symbolParameter = new SqlParameter("@symbol", SqlDbType.NVarChar, 10) { Value = "msft" };
            values = DbHelper.ReadSingleRow<SqlConnection>(this.ConnectionString, sqlQueryParameterized, new[] { symbolParameter }, CommandType.Text, true, 30);
            Assert.Equal(3, values.Count);
            Assert.True(values.ContainsKey("open"));
            Assert.IsType<decimal>(values["open"]);
            Assert.Equal(19.4519m, (decimal)values["open"]);
            Assert.True(values.ContainsKey("high"));
            Assert.IsType<decimal>(values["high"]);
            Assert.Equal(19.9044m, (decimal)values["high"]);
            Assert.True(values.ContainsKey("openinterest"));
            Assert.Null(values["openinterest"]);
        }

        [Fact]
        public void WriteToCsv_ConnectionStringProvided_MonolithicTest()
        {
            // not testing: all exceptions generated from BuildCommand via ExecuteReader
            // not testing: all exceptions from OpenConnection via ExecuteReader
            // not testing: all exceptions generated from ExecuteReader with connection

            // null or whitespace outputFilePath
            Assert.Throws<ArgumentNullException>(() => DbHelper.WriteToCsv<SqlConnection>(this.ConnectionString, "Select * From [Test]", null));
            Assert.Throws<ArgumentException>(() => DbHelper.WriteToCsv<SqlConnection>(this.ConnectionString, "Select * From [Test]", string.Empty));
            Assert.Throws<ArgumentException>(() => DbHelper.WriteToCsv<SqlConnection>(this.ConnectionString, "Select * From [Test]", "   "));
            Assert.Throws<ArgumentException>(() => DbHelper.WriteToCsv<SqlConnection>(this.ConnectionString, "Select * From [Test]", "  \r\n   "));

            // IOException
            string tempFilePath = Path.GetTempFileName();
            using (new FileStream(tempFilePath, FileMode.Open, FileAccess.Read, FileShare.None))
            {
                Assert.Throws<IOException>(() => DbHelper.WriteToCsv<SqlConnection>(this.ConnectionString, "Select * From [Test]", tempFilePath));
            }

            // UnauthorizedAccessException & SecurityException - no good way to test this
            // Non-query
            Assert.Throws<InvalidOperationException>(() => DbHelper.WriteToCsv<SqlConnection>(this.ConnectionString, "Update [DbHelper] Set [Csv,Test] = 'bla' Where [Csv,Test] = 'bla'", tempFilePath));

            // no results - just headers are printed
            DbHelper.WriteToCsv<SqlConnection>(this.ConnectionString, "Select * From [DbHelper] Where [Csv,Test] = 'bla'", tempFilePath);
            Assert.Equal("Id,Date,Value,\"Csv,Test\"", File.ReadAllText(tempFilePath), StringComparer.CurrentCulture);

            // header + one row
            if (!DbHelper.CommandHasRows<SqlConnection>(this.ConnectionString, "Select * From [DbHelper] Where [Csv,Test] = 'writetocsvtest1'"))
            {
                DbHelper.ExecuteNonQuery<SqlConnection>(this.ConnectionString, "Insert Into [DbHelper] ([Date],[Value],[Csv,Test]) Values ('2010-08-14',49.21,'writetocsvtest1')");
            }

            DbHelper.WriteToCsv<SqlConnection>(this.ConnectionString, "Select [Date],[Value],[Csv,Test] FROM [DbHelper] Where [Csv,Test] = 'writetocsvtest1'", tempFilePath);
            Assert.Equal("Date,Value,\"Csv,Test\"" + Environment.NewLine + "2010-08-14 00:00:00.000,49.21000,writetocsvtest1", File.ReadAllText(tempFilePath));

            // header + one row with csv-treatment
            if (!DbHelper.CommandHasRows<SqlConnection>(this.ConnectionString, "Select * From [DbHelper] Where [Csv,Test] = 'writetocsv\"test\"2'"))
            {
                DbHelper.ExecuteNonQuery<SqlConnection>(this.ConnectionString, "Insert Into [DbHelper] ([Date],[Value],[Csv,Test]) Values ('2010-04-12',32.001,'writetocsv\"test\"2')");
            }

            DbHelper.WriteToCsv<SqlConnection>(this.ConnectionString, "Select [Date],[Value],[Csv,Test] From [DbHelper] Where [Csv,Test] = 'writetocsv\"test\"2'", tempFilePath);
            Assert.Equal("Date,Value,\"Csv,Test\"" + Environment.NewLine + "2010-04-12 00:00:00.000,32.00100,\"writetocsv\"\"test\"\"2\"", File.ReadAllText(tempFilePath));

            // header + two rows
            DbHelper.WriteToCsv<SqlConnection>(this.ConnectionString, "Select [Date],[Value],[Csv,Test] From [DbHelper] Where [Csv,Test] = 'writetocsvtest1' OR [Csv,Test] = 'writetocsv\"test\"2'", tempFilePath);
            Assert.Equal("Date,Value,\"Csv,Test\"" + Environment.NewLine + "2010-08-14 00:00:00.000,49.21000,writetocsvtest1" + Environment.NewLine + "2010-04-12 00:00:00.000,32.00100,\"writetocsv\"\"test\"\"2\"", File.ReadAllText(tempFilePath));

            // header + three rows
            if (!DbHelper.CommandHasRows<SqlConnection>(this.ConnectionString, "Select * From [DbHelper] Where [Csv,Test] = 'writetocsvtest3'"))
            {
                DbHelper.ExecuteNonQuery<SqlConnection>(this.ConnectionString, "Insert Into [DbHelper] ([Date],[Value],[Csv,Test]) Values ('1998-03-02 15:58:47.817',0.99929,'writetocsvtest3')");
            }

            DbHelper.WriteToCsv<SqlConnection>(this.ConnectionString, "Select [Csv,Test],[Date],[Value] FROM [DbHelper] Where [Csv,Test] = 'writetocsvtest1' Or [Csv,Test] = 'writetocsv\"test\"2' Or [Csv,Test] = 'writetocsvtest3'", tempFilePath);
            Assert.Equal("\"Csv,Test\",Date,Value" + Environment.NewLine + "writetocsvtest1,2010-08-14 00:00:00.000,49.21000" + Environment.NewLine + "\"writetocsv\"\"test\"\"2\",2010-04-12 00:00:00.000,32.00100" + Environment.NewLine + "writetocsvtest3,1998-03-02 15:58:47.817,0.99929", File.ReadAllText(tempFilePath));

            // need to test char and char[] field types for csv-treatment
        }

        [Fact]
        public void WriteToCsv_IDbConnectionProvided_MonolithicTest()
        {
            // not testing: all exceptions generated from BuildCommand via ExecuteReader
            // not testing: all exceptions from OpenConnection via ExecuteReader
            // not testing: all exceptions generated from ExecuteReader with connection

            // null or whitespace outputFilePath
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                Assert.Throws<ArgumentNullException>(() => DbHelper.WriteToCsv(sqlConnection, "Select * From [Test]", null));
                Assert.Throws<ArgumentException>(() => DbHelper.WriteToCsv(sqlConnection, "Select * From [Test]", string.Empty));
                Assert.Throws<ArgumentException>(() => DbHelper.WriteToCsv(sqlConnection, "Select * From [Test]", "   "));
                Assert.Throws<ArgumentException>(() => DbHelper.WriteToCsv(sqlConnection, "Select * From [Test]", "  \r\n   "));
            }

            // IOException
            string tempFilePath = Path.GetTempFileName();
            using (new FileStream(tempFilePath, FileMode.Open, FileAccess.Read, FileShare.None))
            {
                using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
                {
                    Assert.Throws<IOException>(() => DbHelper.WriteToCsv(sqlConnection, "Select * From [Test]", tempFilePath));
                }
            }

            // UnauthorizedAccessException & SecurityException - no good way to test this
            // Non-query
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                Assert.Throws<InvalidOperationException>(() => DbHelper.WriteToCsv(sqlConnection, "Update [DbHelper] Set [Csv,Test] = 'bla' Where [Csv,Test] = 'bla'", tempFilePath));
            }

            // no results - just headers are printed
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                DbHelper.WriteToCsv(sqlConnection, "Select * From [DbHelper] Where [Csv,Test] = 'bla'", tempFilePath);
                Assert.Equal("Id,Date,Value,\"Csv,Test\"", File.ReadAllText(tempFilePath), StringComparer.CurrentCulture);
            }

            // header + one row
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                if (!DbHelper.CommandHasRows(sqlConnection, "Select * From [DbHelper] Where [Csv,Test] = 'writetocsvtest1'"))
                {
                    DbHelper.ExecuteNonQuery(sqlConnection, "Insert Into [DbHelper] ([Date],[Value],[Csv,Test]) Values ('2010-08-14',49.21,'writetocsvtest1')");
                }
            }

            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                DbHelper.WriteToCsv(sqlConnection, "Select [Date],[Value],[Csv,Test] FROM [DbHelper] Where [Csv,Test] = 'writetocsvtest1'", tempFilePath);
                Assert.Equal("Date,Value,\"Csv,Test\"" + Environment.NewLine + "2010-08-14 00:00:00.000,49.21000,writetocsvtest1", File.ReadAllText(tempFilePath));
            }

            // header + one row with csv-treatment
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                if (!DbHelper.CommandHasRows(sqlConnection, "Select * From [DbHelper] Where [Csv,Test] = 'writetocsv\"test\"2'"))
                {
                    DbHelper.ExecuteNonQuery(sqlConnection, "Insert Into [DbHelper] ([Date],[Value],[Csv,Test]) Values ('2010-04-12',32.001,'writetocsv\"test\"2')");
                }
            }

            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                DbHelper.WriteToCsv(sqlConnection, "Select [Date],[Value],[Csv,Test] From [DbHelper] Where [Csv,Test] = 'writetocsv\"test\"2'", tempFilePath);
                Assert.Equal("Date,Value,\"Csv,Test\"" + Environment.NewLine + "2010-04-12 00:00:00.000,32.00100,\"writetocsv\"\"test\"\"2\"", File.ReadAllText(tempFilePath));
            }

            // header + two rows
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                DbHelper.WriteToCsv(sqlConnection, "Select [Date],[Value],[Csv,Test] From [DbHelper] Where [Csv,Test] = 'writetocsvtest1' OR [Csv,Test] = 'writetocsv\"test\"2'", tempFilePath);
                Assert.Equal("Date,Value,\"Csv,Test\"" + Environment.NewLine + "2010-08-14 00:00:00.000,49.21000,writetocsvtest1" + Environment.NewLine + "2010-04-12 00:00:00.000,32.00100,\"writetocsv\"\"test\"\"2\"", File.ReadAllText(tempFilePath));
            }

            // header + three rows
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                if (!DbHelper.CommandHasRows(sqlConnection, "Select * From [DbHelper] Where [Csv,Test] = 'writetocsvtest3'"))
                {
                    DbHelper.ExecuteNonQuery(sqlConnection, "Insert Into [DbHelper] ([Date],[Value],[Csv,Test]) Values ('1998-03-02 15:58:47.817',0.99929,'writetocsvtest3')");
                }
            }

            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                DbHelper.WriteToCsv(sqlConnection, "Select [Csv,Test],[Date],[Value] FROM [DbHelper] Where [Csv,Test] = 'writetocsvtest1' Or [Csv,Test] = 'writetocsv\"test\"2' Or [Csv,Test] = 'writetocsvtest3'", tempFilePath);
                Assert.Equal("\"Csv,Test\",Date,Value" + Environment.NewLine + "writetocsvtest1,2010-08-14 00:00:00.000,49.21000" + Environment.NewLine + "\"writetocsv\"\"test\"\"2\",2010-04-12 00:00:00.000,32.00100" + Environment.NewLine + "writetocsvtest3,1998-03-02 15:58:47.817,0.99929", File.ReadAllText(tempFilePath));
            }

            // need to test char and char[] field types for csv-treatment
        }

        [Fact]
        public void ExecuteNonQueryBatch_ConnectionObjectProvided_MonolithicTest()
        {
            // not testing: all exceptions generated from BuildCommand
            // not testing: all exceptions generated from ExecuteNonQuery
            // batch command text is null or empty
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                Assert.Throws<ArgumentNullException>(() => DbHelper.ExecuteNonQueryBatch(sqlConnection, null, null, 30));
                Assert.Throws<ArgumentException>(() => DbHelper.ExecuteNonQueryBatch(sqlConnection, string.Empty));
                Assert.Throws<ArgumentException>(() => DbHelper.ExecuteNonQueryBatch(sqlConnection, "    "));
                Assert.Throws<ArgumentException>(() => DbHelper.ExecuteNonQueryBatch(sqlConnection, "  \r\n   "));
                sqlConnection.Close();
            }

            string timeStamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:fff", CultureInfo.CurrentCulture);

            // bad sqlCommand (date missing) - single command
            Exception actualException;
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                const string SqlCommand = "Insert Into [DbHelper] ([Value]) Values (1.234)";
                actualException = Assert.Throws<SqlException>(() => DbHelper.ExecuteNonQueryBatch(sqlConnection, SqlCommand));
                Assert.Equal(string.Format("Cannot insert the value NULL into column 'Date', table '{0}'; column does not allow nulls. INSERT fails.\r\nThe statement has been terminated.", this.DatabaseName + ".dbo.DbHelper"), actualException.Message);
                Assert.False(DbHelper.CommandHasRows<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, "Select * From [DbHelper] Where [Date]='{0}'", timeStamp)));
                sqlConnection.Close();
            }

            // bad sqlCommand - first command
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                const string SqlCommand = "asdfasdf\r\nGO\r\nInsert Into [DbHelper] ([Date],[Value]) Values ('{0}',3)";
                actualException = Assert.Throws<SqlException>(() => DbHelper.ExecuteNonQueryBatch(sqlConnection, string.Format(CultureInfo.CurrentCulture, SqlCommand, timeStamp)));
                Assert.Equal("Could not find stored procedure 'asdfasdf'.", actualException.Message);
                Assert.False(DbHelper.CommandHasRows<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, "Select * From [DbHelper] Where [Date]='{0}'", timeStamp)));
                sqlConnection.Close();
            }

            // bad sqlCommand - second command (in transaction so rolled back)
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                using (var sqlTransaction = sqlConnection.BeginTransaction())
                {
                    const string SqlCommand = "Insert Into [DbHelper] ([Date],[Value]) Values ('{0}',3)\r\nGO\r\nasdfasd";
                    actualException = Assert.Throws<SqlException>(() => DbHelper.ExecuteNonQueryBatch(sqlConnection, string.Format(CultureInfo.CurrentCulture, SqlCommand, timeStamp), sqlTransaction));
                    Assert.Equal("Could not find stored procedure 'asdfasd'.", actualException.Message);
                    Assert.True(DbHelper.CommandHasRows(sqlConnection, string.Format(CultureInfo.CurrentCulture, "Select * From [DbHelper] Where [Date]='{0}'", timeStamp), null, CommandType.Text, sqlTransaction));
                    sqlTransaction.Rollback();
                }

                Assert.False(DbHelper.CommandHasRows<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, "Select * From [DbHelper] Where [Date]='{0}'", timeStamp)));
                sqlConnection.Close();
            }

            // bad sqlCommand - second command 
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                const string SqlCommand = "Insert Into [DbHelper] ([Date],[Value]) Values ('{0}',3)\r\nGO\r\nasdfasd";
                actualException = Assert.Throws<SqlException>(() => DbHelper.ExecuteNonQueryBatch(sqlConnection, string.Format(CultureInfo.CurrentCulture, SqlCommand, timeStamp)));
                Assert.Equal("Could not find stored procedure 'asdfasd'.", actualException.Message);
                Assert.True(DbHelper.CommandHasRows<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, "Select * From [DbHelper] Where [Date]='{0}'", timeStamp)));
                sqlConnection.Close();
            }

            // good sqlCommand - batch contains only one statement
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                const string SqlCommand = "Update [DbHelper] Set [Value] = 5 Where [Date]='{0}'";
                Assert.Equal(1, DbHelper.ExecuteNonQueryBatch(sqlConnection, string.Format(CultureInfo.CurrentCulture, SqlCommand, timeStamp)));
                Assert.Equal((decimal)5.00000, DbHelper.ReadSingleValue<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, "Select [Value] From [DbHelper] Where [Date]='{0}'", timeStamp)));
                sqlConnection.Close();
            }

            // good sqlCommand with multiple statements
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                const string SqlCommand = "Update [DbHelper] Set [Value] = 4 Where [Date]='{0}'\r\nGO\r\nUpdate [DbHelper] Set [Csv,Test] = 'whatever' Where [Date]='{0}'\r\nGO\r\n";
                Assert.Equal(2, DbHelper.ExecuteNonQueryBatch(sqlConnection, string.Format(CultureInfo.CurrentCulture, SqlCommand, timeStamp)));
                Assert.Equal((decimal)4.00000, DbHelper.ReadSingleValue<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, "Select [Value] From [DbHelper] Where [Date]='{0}'", timeStamp)));
                Assert.Equal("whatever", DbHelper.ReadSingleValue<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, "Select [Csv,Test] From [DbHelper] Where [Date]='{0}'", timeStamp)));
                sqlConnection.Close();
            }

            // empty batch, nothing happens.
            using (var sqlConnection = DbHelper.OpenConnection<SqlConnection>(this.ConnectionString))
            {
                const string SqlCommand = "\r\nGO\r\n\r\nGO";
                Assert.Throws<InvalidOperationException>(() => DbHelper.ExecuteNonQueryBatch(sqlConnection, SqlCommand));
                sqlConnection.Close();
            }
        }

        [Fact]
        public void ExecuteNonQueryBatch_ConnectionStringProvided_MonolithicTest()
        {
            // not testing: From OpenConnection via ExecuteNonQuery
            // not testing: From BuildCommand via ExecuteNonQuery
            // not testing: From ExecuteNonQuery

            // batch command text is null or empty
            Assert.Throws<ArgumentNullException>(() => DbHelper.ExecuteNonQueryBatch<SqlConnection>(this.ConnectionString, null, 30));
            Assert.Throws<ArgumentException>(() => DbHelper.ExecuteNonQueryBatch<SqlConnection>(this.ConnectionString, string.Empty, 30));
            Assert.Throws<ArgumentException>(() => DbHelper.ExecuteNonQueryBatch<SqlConnection>(this.ConnectionString, "    ", 30));
            Assert.Throws<ArgumentException>(() => DbHelper.ExecuteNonQueryBatch<SqlConnection>(this.ConnectionString, "  \r\n  ", 30));

            string timeStamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:fff", CultureInfo.CurrentCulture);

            // bad sqlCommand (date missing) - single command
            string sqlCommand = "Insert Into [DbHelper] ([Value]) Values (1.234)";
            Exception actualException = Assert.Throws<SqlException>(() => DbHelper.ExecuteNonQueryBatch<SqlConnection>(this.ConnectionString, sqlCommand, 30));
            Assert.Equal(string.Format("Cannot insert the value NULL into column 'Date', table '{0}'; column does not allow nulls. INSERT fails.\r\nThe statement has been terminated.", this.DatabaseName + ".dbo.DbHelper"), actualException.Message);
            Assert.False(DbHelper.CommandHasRows<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, "Select * From [DbHelper] Where [Date]='{0}'", timeStamp)));

            // bad sqlCommand - first command
            sqlCommand = "asdfasdf\r\nGO\r\nInsert Into [DbHelper] ([Date],[Value]) Values ('{0}',3)";
            actualException = Assert.Throws<SqlException>(() => DbHelper.ExecuteNonQueryBatch<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, sqlCommand, timeStamp), 30));
            Assert.Equal("Could not find stored procedure 'asdfasdf'.", actualException.Message);
            Assert.False(DbHelper.CommandHasRows<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, "Select * From [DbHelper] Where [Date]='{0}'", timeStamp)));

            // bad sqlCommand - second command
            sqlCommand = "Insert Into [DbHelper] ([Date],[Value]) Values ('{0}',3)\r\nGO\r\nasdfasd";
            actualException = Assert.Throws<SqlException>(() => DbHelper.ExecuteNonQueryBatch<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, sqlCommand, timeStamp), 30));
            Assert.Equal("Could not find stored procedure 'asdfasd'.", actualException.Message);
            Assert.True(DbHelper.CommandHasRows<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, "Select * From [DbHelper] Where [Date]='{0}'", timeStamp)));

            // good sqlCommand - batch contains only one statement
            sqlCommand = "UPDATE [DbHelper] Set [Value] =5 WHERE [Date]='{0}'";
            Assert.Equal(1, DbHelper.ExecuteNonQueryBatch<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, sqlCommand, timeStamp), 30));
            Assert.Equal((decimal)5.00000, DbHelper.ReadSingleValue<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, "Select [Value] From [DbHelper] Where [Date]='{0}'", timeStamp)));

            // good sqlCommand with multiple statements
            sqlCommand = "UPDATE [DbHelper] Set [Value] = 4 WHERE [Date]='{0}'\r\nGO\r\nUPDATE [DbHelper] Set [Csv,Test] = 'whatever' WHERE [Date]='{0}'\r\nGO\r\n";
            Assert.Equal(2, DbHelper.ExecuteNonQueryBatch<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, sqlCommand, timeStamp), 30));
            Assert.Equal((decimal)4.00000, DbHelper.ReadSingleValue<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, "Select [Value] From [DbHelper] Where [Date]='{0}'", timeStamp)));
            Assert.Equal("whatever", DbHelper.ReadSingleValue<SqlConnection>(this.ConnectionString, string.Format(CultureInfo.CurrentCulture, "Select [Csv,Test] From [DbHelper] Where [Date]='{0}'", timeStamp)));

            // empty batch, nothing happens.
            sqlCommand = "\r\nGO\r\n\r\nGO";
            Assert.Throws<InvalidOperationException>(() => DbHelper.ExecuteNonQueryBatch<SqlConnection>(this.ConnectionString, sqlCommand));
        }
        
        /// <summary>
        /// Dispose the instance.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispose the instance.
        /// </summary>
        /// <param name="disposing">Is disposing?</param>
        public virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                // delete the database
                this.DropDatabase(this.DatabaseName);
            }
        }

        // ReSharper restore CoVariantArrayConversion
        // ReSharper restore InconsistentNaming

        /// <summary>
        /// Creates a database of stock quote data.
        /// </summary>
        /// <returns>
        /// Returns the database name.
        /// </returns>
        private string CreatedSeededDatabase()
        {
            // create the database
            var random = new Random();
            string databaseName = "DbHelperTest_" + random.Next();
            using (var connection = new SqlConnection(this.BuildConnectionString(null)))
            {
                connection.Open();
                using (var command = new SqlCommand("Create Database " + databaseName, connection))
                {
                    command.ExecuteNonQuery();
                }

                connection.Close();
            }

            // prepare all the SQL statements.
            string quotesSchemaSql = AssemblyHelper.ReadEmbeddedResourceAsString("StockQuotesSchema.sql", true);
            string quotesStoredProcSql = AssemblyHelper.ReadEmbeddedResourceAsString("StockQuotesStoredProcs.sql", true);
            string quotesDataSql = AssemblyHelper.ReadEmbeddedResourceAsString("StockQuotesData.sql", true);
            string helperSchemaSql = AssemblyHelper.ReadEmbeddedResourceAsString("DbHelperSchema.sql", true);
            string helperDataSql = AssemblyHelper.ReadEmbeddedResourceAsString("DbHelperData.sql", true);
            var allSqlStatements = new List<string> { quotesSchemaSql, quotesStoredProcSql, quotesDataSql, helperSchemaSql, helperDataSql };

            // import schema and data
            using (var connection = new SqlConnection(this.BuildConnectionString(databaseName)))
            {
                connection.Open();
                foreach (string sqlStatement in allSqlStatements)
                {
                    using (var command = new SqlCommand(sqlStatement, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }

                connection.Close();
            }

            return databaseName;
        }

        /// <summary>
        /// Drops a database
        /// </summary>
        /// <param name="databaseName">The name of the database.</param>
        private void DropDatabase(string databaseName)
        {
            // delete the database
            var sqlCommandText = string.Format(@"Alter Database {0} Set Single_User With Rollback Immediate; Drop Database {0}", databaseName);
            using (var connection = new SqlConnection(this.BuildConnectionString(null)))
            {
                connection.Open();
                using (var command = new SqlCommand(sqlCommandText, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Builds a connection string.
        /// </summary>
        /// <param name="databaseName">The name of the database.</param>
        /// <returns>
        /// The connection string.
        /// </returns>
        private string BuildConnectionString(string databaseName)
        {
            return string.Format(@"Server={0};Database={1};User ID={2};Password={3};Connection Timeout=3600", Server, databaseName ?? "master", UserId, Password);
        }        
    }
}
